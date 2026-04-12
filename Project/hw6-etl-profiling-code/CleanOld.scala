import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

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

val w = Window.partitionBy("CAD_EVNT_ID").orderBy(col("null_count"))

val dfDeduped = dfWithNullCount
  .withColumn("row_num", row_number().over(w))
  .filter(col("row_num") === 1)
  .drop("null_count", "row_num")


// 2. Format timestamps
val dfWithFormattedTS = dfDeduped
  .withColumn("ADD_TS_FORMATTED", to_timestamp(col("ADD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("ARRIVD_TS_FORMATTED", to_timestamp(col("ARRIVD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("DISP_TS_FORMATTED", to_timestamp(col("DISP_TS"), "MM/dd/yyyy hh:mm:ss a"))


// 3. Create time difference columns
val dfWithTimeDiffs = dfWithFormattedTS
  .withColumn("RESPONSE_TIME_SEC",
    when(col("ADD_TS_FORMATTED").isNotNull && col("ARRIVD_TS_FORMATTED").isNotNull,
      unix_timestamp(col("ARRIVD_TS_FORMATTED")) - unix_timestamp(col("ADD_TS_FORMATTED"))
    )
  )
  .withColumn("DISPATCH_DELAY_SEC",
    when(col("ADD_TS_FORMATTED").isNotNull && col("DISP_TS_FORMATTED").isNotNull,
      unix_timestamp(col("DISP_TS_FORMATTED")) - unix_timestamp(col("ADD_TS_FORMATTED"))
    )
  )
  .withColumn("TRAVEL_TIME_SEC",
    when(col("DISP_TS_FORMATTED").isNotNull && col("ARRIVD_TS_FORMATTED").isNotNull,
      unix_timestamp(col("ARRIVD_TS_FORMATTED")) - unix_timestamp(col("DISP_TS_FORMATTED"))
    )
  )
  // Convert to HH:mm:ss
  .withColumn("RESPONSE_TIME", date_format(from_unixtime(col("RESPONSE_TIME_SEC")), "HH:mm:ss"))
  .withColumn("DISPATCH_DELAY", date_format(from_unixtime(col("DISPATCH_DELAY_SEC")), "HH:mm:ss"))
  .withColumn("TRAVEL_TIME", date_format(from_unixtime(col("TRAVEL_TIME_SEC")), "HH:mm:ss"))


// 4. Add enum for TYP_DESC
val wTypDesc = Window.orderBy("TYP_DESC")

val dfWithTypEnum = dfWithTimeDiffs.withColumn(
  "TYP_DESC_ENUM",
  dense_rank().over(wTypDesc)
)


// 5. Remove invalid BORO_NM
val dfValidBoro = dfWithTypEnum.filter(
  col("BORO_NM").isNotNull && trim(col("BORO_NM")) =!= "(null)"
)


// 6. Remove invalid dispatch ordering
val dfValidDispatch = dfValidBoro.filter(
  col("DISP_TS_FORMATTED").isNull ||
    col("ADD_TS_FORMATTED").isNull ||
    col("DISP_TS_FORMATTED") >= col("ADD_TS_FORMATTED")
)


// Final cleaned DataFrame
val dfClean = dfValidDispatch


// Verification
val distinctBoroCount = dfClean.select("BORO_NM").distinct().count()
println(s"Number of distinct BORO_NM values: $distinctBoroCount")

println("Distinct BORO_NM values:")
dfClean.select("BORO_NM").distinct().show(false)


// 7. Select final columns (REMOVE raw timestamp strings, KEEP new features)
val colsToKeep = Seq(
  "CAD_EVNT_ID", "CREATE_DATE", "INCIDENT_DATE", "INCIDENT_TIME",
  "NYPD_PCT_CD", "BORO_NM", "RADIO_CODE", "TYP_DESC", "TYP_DESC_ENUM",
  "CIP_JOBS",
  // Keep formatted timestamps ONLY
  "ADD_TS_FORMATTED", "DISP_TS_FORMATTED", "ARRIVD_TS_FORMATTED",
  // Keep derived metrics
  "RESPONSE_TIME", "DISPATCH_DELAY", "TRAVEL_TIME"
)

val dfCleanFinal = dfClean.select(colsToKeep.map(col): _*)


// Show sample
dfCleanFinal.show(20, false)


// Write cleaned DataFrame
// dfCleanFinal.write
//   .mode("overwrite")
//   .option("header", "true")
//   .csv("./hw6/nypd_cfs_cleaned")