import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// Create SparkSession
val spark = SparkSession.builder()
  .appName("NYPD Service Calls Analysis")
  .getOrCreate()

// Read CSV into DataFrame
val df = spark.read
  .option("header", "true")
  .option("inferSchema", "true")
  .csv("./hw6/nypd_cfs.csv")

// Total records
val totalRecords = df.count()
println(s"Total records: $totalRecords")  // Should be 7038863 rows

// Total rows using RDD
val totalRows = df.rdd.map(_ => 1).reduce(_ + _)
println(s"Total rows (RDD count): $totalRows")  // Should be 7038863 rows

// Distinct values per column
val columns = Seq(
  "CAD_EVNT_ID", "CREATE_DATE", "INCIDENT_DATE", "INCIDENT_TIME",
  "NYPD_PCT_CD", "BORO_NM", "PATRL_BORO_NM", "RADIO_CODE",
  "TYP_DESC", "CIP_JOBS", "ADD_TS", "DISP_TS", "ARRIVD_TS", "CLOSNG_TS"
)

columns.foreach { colName =>
  println(s"Distinct values for $colName:")
  val distinctDF = df.select(colName).distinct()
  distinctDF.show(false)

  val distinctCount = distinctDF.count()
  println(s"Number of distinct values in $colName: $distinctCount\n")
}

/*********/
// Find RADIO_CODE vs TYP_DESC discrepancies

// Count distinct RADIO_CODE-TYP_DESC pairs
val codeDescPairs = df.select("RADIO_CODE", "TYP_DESC").distinct()
val totalPairs = codeDescPairs.count()
println(s"Total distinct RADIO_CODE-TYP_DESC pairs: $totalPairs")

// Group by TYP_DESC to see how many RADIO_CODEs map to same description
val descToCodes = df.groupBy("TYP_DESC")
  .agg(collect_set("RADIO_CODE").alias("RADIO_CODES"), countDistinct("RADIO_CODE").alias("num_codes"))
  .orderBy(desc("num_codes"))

// Show TYP_DESC that map to multiple RADIO_CODEs - 3 TYP_DESC are each mapped to 2 diff radio codes
println("TYP_DESC values mapping to multiple RADIO_CODEs:")
descToCodes.filter(col("num_codes") > 1).show(false)

// Find RADIO_CODEs with missing or null TYP_DESC - none
val missingDesc = df.filter(col("TYP_DESC").isNull || trim(col("TYP_DESC")) === "")
println("RADIO_CODEs with missing TYP_DESC:")
missingDesc.select("RADIO_CODE", "TYP_DESC").distinct().show(false)

/*********/

// Check consistency between TYP_DESC and CIP_JOBS
// Group by TYP_DESC and collect distinct CIP_JOBS values
val typDescCip = df.groupBy("TYP_DESC")
  .agg(
    collect_set("CIP_JOBS").alias("CIP_JOBS_set"),
    countDistinct("CIP_JOBS").alias("num_CIP_JOBS")
  )
  .orderBy(desc("num_CIP_JOBS"))

// Show TYP_DESC values that map to more than one CIP_JOBS
println("TYP_DESC values mapping to multiple CIP_JOBS (inconsistencies):")
typDescCip.filter(col("num_CIP_JOBS") > 1).show(false)

// Optional: Show a summary of all TYP_DESC -> CIP_JOBS mappings
println("All TYP_DESC -> CIP_JOBS mappings:")
typDescCip.select("TYP_DESC", "CIP_JOBS_set").show(false)


///////

// Check for duplicate CAD_EVNT_ID
val dupCounts = df.groupBy("CAD_EVNT_ID").count().filter(col("count") > 1)
println("Duplicate CAD_EVNT_ID rows:")
dupCounts.show(false)


val dfWithNullCount = df.withColumn(
  "null_count",
  df.columns.map(c => when(col(c).isNull, 1).otherwise(0)).reduce(_ + _)
)

val w = Window.partitionBy("CAD_EVNT_ID").orderBy(col("null_count"))

val dfDeduped = dfWithNullCount
  .withColumn("row_num", row_number().over(w))
  .filter(col("row_num") === 1)
  .drop("null_count", "row_num")







// 5️⃣ Min and max dates
val dfWithDates = df
  .withColumn("CREATE_DATE_TS", to_date(col("CREATE_DATE"), "MM/dd/yyyy"))
  .withColumn("INCIDENT_DATE_TS", to_date(col("INCIDENT_DATE"), "MM/dd/yyyy"))

val minMaxDates = dfWithDates.agg(
  min("CREATE_DATE_TS").alias("min_CREATE_DATE"),
  max("CREATE_DATE_TS").alias("max_CREATE_DATE"),
  min("INCIDENT_DATE_TS").alias("min_INCIDENT_DATE"),
  max("INCIDENT_DATE_TS").alias("max_INCIDENT_DATE")
)
println("Min/Max CREATE_DATE and INCIDENT_DATE:")
minMaxDates.show(false)

// 6️⃣ Max string length of TYP_DESC
val maxTypDescLen = df.agg(max(length(col("TYP_DESC"))).alias("max_TYP_DESC_len"))
println("Max length of TYP_DESC:")
maxTypDescLen.show(false)

// 7️⃣ Timestamp parsing for anomaly checks
val dfWithTS = df
  .withColumn("ADD_TS_TS", to_timestamp(col("ADD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("DISP_TS_TS", to_timestamp(col("DISP_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("ARRIVD_TS_TS", to_timestamp(col("ARRIVD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("CLOSNG_TS_TS", to_timestamp(col("CLOSNG_TS"), "MM/dd/yyyy hh:mm:ss a"))

// 8️⃣ Additional anomaly checks
val anomalies = dfWithTS.select(
  col("CAD_EVNT_ID"),

  // Missing required fields
  when(col("CAD_EVNT_ID").isNull, "missing").otherwise("ok").alias("CAD_EVNT_ID_status"),
  when(col("ARRIVD_TS_TS").isNull, "missing").otherwise("ok").alias("ARRIVD_TS_status"),
  when(col("CLOSNG_TS_TS").isNull, "missing").otherwise("ok").alias("CLOSNG_TS_status"),
  when(col("PATRL_BORO_NM").isNull, "missing").otherwise("ok").alias("PATRL_BORO_NM_status"),
  when(col("RADIO_CODE").isNull, "missing").otherwise("ok").alias("RADIO_CODE_status"),
  when(col("TYP_DESC").isNull, "missing").otherwise("ok").alias("TYP_DESC_status"),

  // BORO_NM validation
  when(!col("BORO_NM").isin("MANHATTAN", "BROOKLYN", "QUEENS", "BRONX", "STATEN ISLAND"), "invalid").otherwise("ok").alias("BORO_NM_status"),


  // Incident before call or after closing
  when(col("INCIDENT_DATE_TS").isNotNull && col("CREATE_DATE_TS").isNotNull && col("INCIDENT_DATE_TS") < col("CREATE_DATE_TS"),
    "before_CREATE_DATE").otherwise("ok").alias("INCIDENT_DATE_check1"),
  when(col("INCIDENT_DATE_TS").isNotNull && col("CLOSNG_TS_TS").isNotNull && col("INCIDENT_DATE_TS") > col("CLOSNG_TS_TS"),
    "after_CLOSNG_TS").otherwise("ok").alias("INCIDENT_DATE_check2"),

  // Out-of-range incident times
  when(!col("INCIDENT_TIME").rlike("""^([01]?[0-9]|2[0-3]):[0-5][0-9]:[0-5][0-9]$"""), "invalid").otherwise("ok").alias("INCIDENT_TIME_status"),

  // Temporal consistency: DISP_TS - ADD_TS
  when(col("DISP_TS_TS").isNotNull && col("ADD_TS_TS").isNotNull && col("DISP_TS_TS") < col("ADD_TS_TS"),
    "DISP_before_ADD").otherwise("ok").alias("DISP_ADD_check")
)

// 9️⃣ Show first 20 anomalies
println("Sample anomaly checks:")
anomalies.show(20,false)