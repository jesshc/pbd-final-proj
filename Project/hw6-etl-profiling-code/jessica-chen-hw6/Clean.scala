import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Create SparkSession
val spark = SparkSession.builder()
  .appName("NYPD Service Calls Cleaning")
  .getOrCreate()

import spark.implicits._

// Load CSV
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "false")
  .option("mode", "PERMISSIVE")
  .csv("./hw6/nypd_cfs.csv")

// 1. Deduplicate CAD_EVNT_ID (keep row with fewest nulls)
val dfWithNullCount = df.withColumn(
  "null_count",
  df.columns.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
)

val wDedup = Window.partitionBy("CAD_EVNT_ID").orderBy(col("null_count"))

val dfDeduped = dfWithNullCount
  .withColumn("row_num", row_number().over(wDedup))
  .filter(col("row_num") === 1)
  .drop("null_count", "row_num")

// 2. Convert columns to correct data types
val dfTyped = dfDeduped
  .withColumn("CREATE_DATE", to_date(col("CREATE_DATE"), "MM/dd/yyyy"))
  .withColumn("INCIDENT_DATE", to_date(col("INCIDENT_DATE"), "MM/dd/yyyy"))
  .withColumn("NYPD_PCT_CD", col("NYPD_PCT_CD").cast("int"))
  .withColumn("ADD_TS", to_timestamp(col("ADD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("DISP_TS", to_timestamp(col("DISP_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("ARRIVD_TS", to_timestamp(col("ARRIVD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("CLOSNG_TS", to_timestamp(col("CLOSNG_TS"), "MM/dd/yyyy hh:mm:ss a"))

// 3. Time difference features
val dfWithTimeDiffs = dfTyped
  .withColumn("RESPONSE_TIME_SEC",
    when(col("ADD_TS").isNotNull && col("ARRIVD_TS").isNotNull,
      unix_timestamp(col("ARRIVD_TS")) - unix_timestamp(col("ADD_TS"))
    )
  )
  .withColumn("DISPATCH_DELAY_SEC",
    when(col("ADD_TS").isNotNull && col("DISP_TS").isNotNull,
      unix_timestamp(col("DISP_TS")) - unix_timestamp(col("ADD_TS"))
    )
  )
  .withColumn("TRAVEL_TIME_SEC",
    when(col("DISP_TS").isNotNull && col("ARRIVD_TS").isNotNull,
      unix_timestamp(col("ARRIVD_TS")) - unix_timestamp(col("DISP_TS"))
    )
  )
  .withColumn("RESPONSE_TIME", date_format(from_unixtime(col("RESPONSE_TIME_SEC")), "HH:mm:ss"))
  .withColumn("DISPATCH_DELAY", date_format(from_unixtime(col("DISPATCH_DELAY_SEC")), "HH:mm:ss"))
  .withColumn("TRAVEL_TIME", date_format(from_unixtime(col("TRAVEL_TIME_SEC")), "HH:mm:ss"))

// 4. Enum encoding for TYP_DESC
val typDescDF = dfWithTimeDiffs
  .select("TYP_DESC")
  .filter(col("TYP_DESC").isNotNull)
  .distinct()
  .orderBy("TYP_DESC")
  .as[String]
  .rdd
  .zipWithIndex()
  .map { case (desc, idx) => (desc, idx) }
  .toDF("TYP_DESC", "TYP_DESC_ENUM")

val dfWithEnum = dfWithTimeDiffs
  .join(typDescDF, Seq("TYP_DESC"), "left")
  .withColumn("TYP_DESC_ENUM", coalesce(col("TYP_DESC_ENUM"), lit(-1)))

// 5. Remove invalid BORO_NM
val dfValidBoro = dfWithEnum.filter(
  col("BORO_NM").isNotNull && trim(col("BORO_NM")) =!= "(null)"
)

// 6. Remove invalid dispatch ordering
val dfCleanStep6 = dfValidBoro.filter(
  col("DISP_TS").isNull || col("ADD_TS").isNull || col("DISP_TS") >= col("ADD_TS")
)

// 7. INCIDENT_TS check
val dfWithIncidentTS = dfCleanStep6.withColumn(
  "INCIDENT_TS",
  to_timestamp(concat_ws(" ", col("INCIDENT_DATE"), col("INCIDENT_TIME")), "MM/dd/yyyy HH:mm:ss")
)

val dfCleanStep7 = dfWithIncidentTS.filter(
  col("INCIDENT_TS").isNull ||
    col("ADD_TS").isNull ||
    (unix_timestamp(col("INCIDENT_TS")) - unix_timestamp(col("ADD_TS"))) <= 60
)

// 8. Select final columns
val colsToKeep = Seq(
  "CAD_EVNT_ID", "CREATE_DATE", "INCIDENT_DATE", "INCIDENT_TIME",
  "NYPD_PCT_CD", "BORO_NM", "RADIO_CODE", "TYP_DESC", "TYP_DESC_ENUM",
  "CIP_JOBS", "ADD_TS", "DISP_TS", "ARRIVD_TS", "CLOSNG_TS",
  "RESPONSE_TIME", "DISPATCH_DELAY", "TRAVEL_TIME"
)

val dfCleanFinal = dfCleanStep7.select(colsToKeep.map(col): _*)

// 9. Show sample
dfCleanFinal.show(20, false)
dfCleanFinal.printSchema()

// 10. Write output to HDFS
val hdfsOutputPath = "hdfs:///user/jhc10024_nyu_edu/hw6/nypd_cfs_cleaned_1"

dfCleanFinal
  .repartition(50)
  .write
  .mode("overwrite")
  .parquet(hdfsOutputPath)