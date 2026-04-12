import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// Create SparkSession
val spark = SparkSession.builder()
  .appName("NYPD Service Calls Exploration")
  .getOrCreate()
import spark.implicits._


// 1. Load CSV
val df = spark.read
  .option("header", "true")        // first row used as column names
  .option("inferSchema", "false")  // all columns read as strings (preserves raw data)
  .option("mode", "PERMISSIVE")    // malformed rows are kept with nulls instead of failing
  .csv("./hw6/nypd_cfs.csv")


// 2. Count total records
val totalRecords = df.count()
println(s"Total records: $totalRecords")

// 3. RDD-based count
val totalRows = df.rdd.map(_ => 1).reduce(_ + _)
println(s"Total rows (RDD count): $totalRows")


// 4. Distinct values in each column
val columns = Seq(
  "CAD_EVNT_ID", "CREATE_DATE", "INCIDENT_DATE", "INCIDENT_TIME",
  "NYPD_PCT_CD", "BORO_NM", "RADIO_CODE", "TYP_DESC",
  "CIP_JOBS", "ADD_TS", "DISP_TS", "ARRIVD_TS", "CLOSNG_TS"
)

columns.foreach { colName =>
  println(s"Distinct values for $colName:")
  val distinctDF = df.select(colName).distinct()
  distinctDF.show(false)

  val distinctCount = distinctDF.count()
  println(s"Number of distinct values in $colName: $distinctCount\n")
}


// 5. Consistency checks between RADIO_CODE and TYP_DESC to detect many-to-many or inconsistent mappings

// Count unique (RADIO_CODE, TYP_DESC) pairs
val totalPairs = df.select("RADIO_CODE", "TYP_DESC").distinct().count()
println(s"Total distinct RADIO_CODE-TYP_DESC pairs: $totalPairs")

// Check if a single TYP_DESC maps to multiple RADIO_CODEs
val descToCodes = df.groupBy("TYP_DESC")
  .agg(
    collect_set("RADIO_CODE").alias("RADIO_CODES"),
    countDistinct("RADIO_CODE").alias("num_codes")
  )
  .orderBy(desc("num_codes"))

println("TYP_DESC values mapping to multiple RADIO_CODEs:")
descToCodes.filter(col("num_codes") > 1).show(false)

// Check for missing descriptions
val missingDesc = df.filter(col("TYP_DESC").isNull || trim(col("TYP_DESC")) === "")
println("RADIO_CODEs with missing TYP_DESC:")
missingDesc.select("RADIO_CODE", "TYP_DESC").distinct().show(false)

// Reverse check: RADIO_CODE → multiple descriptions
val codeToDesc = df.groupBy("RADIO_CODE")
  .agg(
    collect_set("TYP_DESC").alias("TYP_DESCS"),
    countDistinct("TYP_DESC").alias("num_descs")
  )
  .orderBy(desc("num_descs"))

println("RADIO_CODE values mapping to multiple TYP_DESC:")
codeToDesc.filter(col("num_descs") > 1).show(false)


// 6. Verify each incident type maps consistently to a CIP category
val typDescCip = df.groupBy("TYP_DESC")
  .agg(
    collect_set("CIP_JOBS").alias("CIP_JOBS_set"),
    countDistinct("CIP_JOBS").alias("num_CIP_JOBS")
  )
  .orderBy(desc("num_CIP_JOBS"))

println("TYP_DESC values mapping to multiple CIP_JOBS:")
typDescCip.filter(col("num_CIP_JOBS") > 1).show(false)

println("All TYP_DESC → CIP_JOBS mappings:")
typDescCip.select("TYP_DESC", "CIP_JOBS_set").show(false)


// 7. Check for duplicate CAD_EVNT_ID rows
// CAD_EVNT_ID should uniquely identify each call
val dupCounts = df.groupBy("CAD_EVNT_ID")
  .count()
  .filter(col("count") > 1)

println("Duplicate CAD_EVNT_ID rows:")
dupCounts.show(false)


// 8. Date range validation
// Convert string dates to date type for min/max analysis
val dfWithDates = df
  .withColumn("CREATE_DATE_FORMATTED", to_date(col("CREATE_DATE"), "MM/dd/yyyy"))
  .withColumn("INCIDENT_DATE_FORMATTED", to_date(col("INCIDENT_DATE"), "MM/dd/yyyy"))

val minMaxDates = dfWithDates.agg(
  min("CREATE_DATE_FORMATTED").alias("min_CREATE_DATE"),
  max("CREATE_DATE_FORMATTED").alias("max_CREATE_DATE"),
  min("INCIDENT_DATE_FORMATTED").alias("min_INCIDENT_DATE"),
  max("INCIDENT_DATE_FORMATTED").alias("max_INCIDENT_DATE")
)

println("Min/Max CREATE_DATE and INCIDENT_DATE:")
minMaxDates.show(false)


// 9. Max length check for TYP_DESC
val maxTypDescLen = df.agg(
  max(length(col("TYP_DESC"))).alias("max_TYP_DESC_len")
)

println("Max length of TYP_DESC:")
maxTypDescLen.show(false)


/*
 * 10. Filter rows where the incident timestamp occurs more than 1 minute
 * after the call was added. This identifies entries where the reported incident
 * time is suspiciously later than the time the call was logged.
 */
// Convert INCIDENT_DATE + INCIDENT_TIME to timestamp and ADD_TS to timestamp format
val dfWithTimes = df
  .withColumn(
    "INCIDENT_TS_FORMATTED",
    to_timestamp(
      concat_ws(" ", col("INCIDENT_DATE"), col("INCIDENT_TIME")),
      "MM/dd/yyyy HH:mm:ss"
    )
  )
  .withColumn(
    "ADD_TS_FORMATTED",
    to_timestamp(col("ADD_TS"), "MM/dd/yyyy hh:mm:ss a")
  )

// Filter rows where INCIDENT_TS occurs more than 1 minute after ADD_TS
val invalidIncidentRows = dfWithTimes.filter(
  col("INCIDENT_TS_FORMATTED").isNotNull &&
    col("ADD_TS_FORMATTED").isNotNull &&
    (unix_timestamp(col("INCIDENT_TS_FORMATTED")) - unix_timestamp(col("ADD_TS_FORMATTED")) > 60)
)

// Show first 10 invalid rows
println("First 10 rows where INCIDENT_TS is more than 1 minute after ADD_TS:")
invalidIncidentRows.show(10, false)

// Count total invalid rows
val invalidIncidentAfterAdd = invalidIncidentRows.count()
println(s"Invalid rows (incident more than 1 min after add): $invalidIncidentAfterAdd")


// 11. Check for out-of-range time entries
// Check formatting validity for time-related columns
val anomalies = df.select(
  col("CAD_EVNT_ID"),

  // INCIDENT_TIME: must be valid 24-hour format
  when(col("INCIDENT_TIME").isNull, "missing")
    .when(!trim(col("INCIDENT_TIME")).rlike(
      """^([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$"""
    ), "invalid")
    .otherwise("ok")
    .alias("INCIDENT_TIME_status"),

  // Timestamp columns: must parse correctly (12-hour format)
  when(col("ADD_TS").isNull, "missing")
    .when(to_timestamp(col("ADD_TS"), "MM/dd/yyyy hh:mm:ss a").isNull, "invalid")
    .otherwise("ok")
    .alias("ADD_TS_status"),

  when(col("DISP_TS").isNull, "missing")
    .when(to_timestamp(col("DISP_TS"), "MM/dd/yyyy hh:mm:ss a").isNull, "invalid")
    .otherwise("ok")
    .alias("DISP_TS_status"),

  when(col("ARRIVD_TS").isNull, "missing")
    .when(to_timestamp(col("ARRIVD_TS"), "MM/dd/yyyy hh:mm:ss a").isNull, "invalid")
    .otherwise("ok")
    .alias("ARRIVD_TS_status"),

  when(col("CLOSNG_TS").isNull, "missing")
    .when(to_timestamp(col("CLOSNG_TS"), "MM/dd/yyyy hh:mm:ss a").isNull, "invalid")
    .otherwise("ok")
    .alias("CLOSNG_TS_status")
)

// Columns with status
val statusCols = Seq(
  "INCIDENT_TIME_status", "ADD_TS_status", "DISP_TS_status",
  "ARRIVD_TS_status", "CLOSNG_TS_status"
)

// Count invalid entries per column
statusCols.foreach { c =>
  val invalidCount = anomalies.filter(col(c) === "invalid").count()
  println(s"$c invalid count: $invalidCount")
}


// 12. Check if Dispatch Timestamp is after Added Timestamp
// Ensure dispatch does not occur before call is added
val dfWithTS = dfWithTimes
  .withColumn("DISP_TS_FORMATTED", to_timestamp(col("DISP_TS"), "MM/dd/yyyy hh:mm:ss a"))

val invalidDispBeforeAdd = dfWithTS.filter(
  col("DISP_TS_FORMATTED").isNotNull &&
    col("ADD_TS_FORMATTED").isNotNull &&
    col("DISP_TS_FORMATTED") < col("ADD_TS_FORMATTED")
).count()

println(s"Invalid rows (DISP before ADD): $invalidDispBeforeAdd")