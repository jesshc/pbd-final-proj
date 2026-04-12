import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

val spark = SparkSession.builder()
  .appName("NYPD Service Calls Cleaning")
  .getOrCreate()
import spark.implicits._

// Load CSV (all as strings initially)
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

val w = Window.partitionBy("CAD_EVNT_ID").orderBy(col("null_count"))

val dfDeduped = dfWithNullCount
  .withColumn("row_num", row_number().over(w))
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

// 3. Create time difference features
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

// 4. Add enum encoding for TYP_DESC
val wTypDesc = Window.orderBy("TYP_DESC")
val dfWithEnum = dfWithTimeDiffs.withColumn("TYP_DESC_ENUM", dense_rank().over(wTypDesc))

// 5. Remove invalid BORO_NM
// Removes rows where BORO_NM = null or "(null)"
val dfValidBoro = dfWithEnum.filter(
  col("BORO_NM").isNotNull && trim(col("BORO_NM")) =!= "(null)"
)

// 6. Remove invalid dispatch ordering
// Removes rows where DISP_TS occurred before ADD_TS
val dfCleanStep6 = dfValidBoro.filter(
  col("DISP_TS").isNull ||
    col("ADD_TS").isNull ||
    col("DISP_TS") >= col("ADD_TS")
)

// 7. Remove rows where the incident timestamp occurs more than 1 minute after the call was added
/*
 * This helps identify entries where the reported incident time is suspiciously later than
 * the time the call was logged. Any rows where INCIDENT_TS > ADD_TS + 1 minute are removed.
 */
val dfCleanStep7 = dfCleanStep6
  .withColumn("INCIDENT_TS", to_timestamp(concat_ws(" ", col("INCIDENT_DATE"), col("INCIDENT_TIME")), "MM/dd/yyyy HH:mm:ss"))
  .filter(
    col("INCIDENT_TS").isNull ||
      col("ADD_TS").isNull ||
      (unix_timestamp(col("INCIDENT_TS")) - unix_timestamp(col("ADD_TS"))) <= 60
  )

// 8. Select final columns (clean schema)
val colsToKeep = Seq(
  "CAD_EVNT_ID",
  "CREATE_DATE",
  "INCIDENT_DATE",
  "INCIDENT_TIME",
  "NYPD_PCT_CD",
  "BORO_NM",
  "RADIO_CODE",
  "TYP_DESC",
  "TYP_DESC_ENUM",
  "CIP_JOBS",
  "ADD_TS",
  "DISP_TS",
  "ARRIVD_TS",
  "CLOSNG_TS",
  "RESPONSE_TIME",
  "DISPATCH_DELAY",
  "TRAVEL_TIME"
)

val dfCleanFinal = dfCleanStep7.select(colsToKeep.map(col): _*)

// Show sample
dfCleanFinal.show(20, false)
dfCleanFinal.printSchema()

// Write output
// dfCleanFinal.write
//   .mode("overwrite")
//   .option("header", "true")
//   .csv("./hw6/nypd_cfs_cleaned")

hdfs dfs -setfacl -R -m user:adm209_nyu_edu:r-x /user/jhc10024_nyu_edu/hw6/nypd_cfs_cleaned