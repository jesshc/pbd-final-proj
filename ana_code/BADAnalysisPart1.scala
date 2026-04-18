// Analysis Part 1: Response Time Disparities

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


// Create Spark session
val spark = SparkSession.builder()
  .appName("NYPD Service Calls Analysis")
  .getOrCreate()

import spark.implicits._

// Read cleaned parquet data from HDFS
val hdfsInputPath = "hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned_2"
val df = spark.read.parquet(hdfsInputPath)


// Filter valid response times
val dfFiltered = df
  .filter(col("RESPONSE_TIME_SEC").isNotNull)
  .filter(col("RESPONSE_TIME_SEC") > 0)

// 1. Borough-level analysis of Response Times
println("=== 1. Response Time by Borough ===")
val boroughStats = dfFiltered
  .groupBy("BORO_NM")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
    stddev("RESPONSE_TIME_SEC").alias("stddev_response_sec"),
    count("*").alias("num_records")
  )
  .orderBy(desc("avg_response_sec"))
boroughStats.show(false)


// 2. Detailed Borough-level Response Time Stats
// Aggregates NYPD incidents by borough, radio-code category, and CIP job type, and
// computes response time statistics (avg, median, min, max) with incident counts.

// Mapping of the first two digits of RADIO_CODE to their
// corresponding TYP_DESC for aggregation later
val radioToType = Map(
  "34" -> "ASSAULT (IN PROGRESS)",
  "13" -> "ASSIST POLICE OFFICER",
  "31" -> "BURGLARY (IN PROGRESS)",
  "10" -> "INVESTIGATE/POSSIBLE CRIME",
  "32" -> "LARCENY (IN PROGRESS)",
  "30" -> "ROBBERY (IN PROGRESS)",
  "66" -> "UNUSUAL INCIDENT",
  "39" -> "OTHER CRIMES (IN PROGRESS)",
  "51" -> "ROVING BAND"
)

val broadcastMap = spark.sparkContext.broadcast(radioToType)

val dfBoroughResponse =
  dfFiltered.withColumn("RADIO_PREFIX", substring(col("RADIO_CODE"), 1, 2))
    .withColumn(
      "TYP_DESC",
      when(col("RADIO_PREFIX").isin(broadcastMap.value.keys.toSeq: _*),
        expr(s"""CASE ${broadcastMap.value.map { case (k,v) => s"WHEN RADIO_PREFIX='$k' THEN '$v'" }.mkString(" ")} END""")
          ).otherwise("UNKNOWN")
          )
        .groupBy("BORO_NM","RADIO_PREFIX","TYP_DESC","CIP_JOBS")
            .agg(
              avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
              expr("percentile_approx(RESPONSE_TIME_SEC,0.5)").alias("median_response_sec"),
              min("RESPONSE_TIME_SEC").alias("min_response_sec"),
              max("RESPONSE_TIME_SEC").alias("max_response_sec"),
              count("*").alias("incident_count")
            )
            .orderBy(desc("incident_count"))
            .withColumn("avg_response_time", date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss"))
            .withColumn("median_response_time", date_format(from_unixtime(col("median_response_sec")), "HH:mm:ss"))
            .withColumn("min_response_time", date_format(from_unixtime(col("min_response_sec")), "HH:mm:ss"))
            .withColumn("max_response_time", date_format(from_unixtime(col("max_response_sec")), "HH:mm:ss"))
            .select(
              "BORO_NM",
              "RADIO_PREFIX",
              "TYP_DESC",
              "CIP_JOBS",
              "avg_response_time",
              "median_response_time",
              "min_response_time",
              "max_response_time",
              "incident_count"
            )

//dfBoroughResponse.orderBy(col("BORO_NM"), col("CIP_JOBS"), col("avg_response_time")).show(55, false)

// Within each borough, rank each (incident type, incident severity) pair by avg response time
println("=== 2a. Detailed Borough-level Response Time Stats ===")
dfBoroughResponse.orderBy(col("BORO_NM"), col("avg_response_time")).show(55, false)

// * For each (incident type, incident severity) pair, rank boroughs by avg_response_time
println("=== 2b. Detailed Borough-level Response Time Stats ===")
dfBoroughResponse.orderBy(col("TYP_DESC"), col("CIP_JOBS"), col("avg_response_time")).show(55, false)


// 3. Precinct-level analysis of Reponse Times
println("=== Response Time by Precinct ===")
val precinctStats = dfFiltered
  .groupBy("BORO_NM", "NYPD_PCT_CD")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
    stddev("RESPONSE_TIME_SEC").alias("stddev_response_sec"),
    count("*").alias("num_records")
  )
  .orderBy(desc("avg_response_sec"))

// Top 10 slowest precincts
println("=== 3a. Top 10 Slowest Precincts ===")
precinctStats.orderBy(desc("avg_response_sec")).show(10, false)

// Top 10 fastest precincts
println("=== 3b. Top 10 Fastest Precincts ===");
precinctStats.orderBy(asc("avg_response_sec")).show(10, false);


// 4. Detailed Precinct-level Response Time Stats
val dfPrecinctResponse =
  dfFiltered
    .withColumn("RADIO_PREFIX", substring(col("RADIO_CODE"), 1, 2))
    .withColumn(
      "TYP_DESC",
      when(col("RADIO_PREFIX").isin(broadcastMap.value.keys.toSeq: _*),
        expr(s"""CASE ${broadcastMap.value.map { case (k,v) =>
          s"WHEN RADIO_PREFIX='$k' THEN '$v'"
        }.mkString(" ")} ELSE 'UNKNOWN' END""")
      ).otherwise("UNKNOWN")
    )
    .groupBy("BORO_NM", "NYPD_PCT_CD", "RADIO_PREFIX", "TYP_DESC", "CIP_JOBS")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
      expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
      min("RESPONSE_TIME_SEC").alias("min_response_sec"),
      max("RESPONSE_TIME_SEC").alias("max_response_sec"),
      count("*").alias("incident_count")
    )

println("Created dfPrecinctResponse +++++++")
// Window definition: ranks records within each (TYP_DESC, CIP_JOBS) group by average response time
// Used to identify both fastest and slowest precinct-level performance within each category
val w = Window.partitionBy("TYP_DESC","CIP_JOBS").orderBy(col("avg_response_sec"))
println("Created w +++++++")
// Add ranking columns:
// rank_fast = fastest response times (ascending order)
// rank_slow = slowest response times (descending order)
val ranked =
    dfPrecinctResponse
    .withColumn("rank_fast", row_number().over(w))
    .withColumn("rank_slow", row_number().over(w.orderBy(col("avg_response_sec").desc)))

// Extract top 5 fastest response records per (TYP_DESC, CIP_JOBS)
val top5 =
  ranked
    .filter(col("rank_fast") <= 5)
    .withColumn("group_type", lit("TOP_5_FASTEST"))
    .withColumn("group_order", lit(0))

// Extract bottom 5 slowest response records per (TYP_DESC, CIP_JOBS)
val bottom5 =
  ranked
    .filter(col("rank_slow") <= 5)
    .withColumn("group_type", lit("BOTTOM_5_SLOWEST"))
    .withColumn("group_order", lit(1))

// Combine top and bottom results into a single dataset for comparison
val combined =
  top5.union(bottom5)
    .withColumn("avg_response_time", date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss"))
    .withColumn("median_response_time", date_format(from_unixtime(col("median_response_sec")), "HH:mm:ss"))
    .withColumn("min_response_time", date_format(from_unixtime(col("min_response_sec")), "HH:mm:ss"))
    .withColumn("max_response_time", date_format(from_unixtime(col("max_response_sec")), "HH:mm:ss"))

// Final output formatting:
// - Orders by incident type, job type, and group ordering (TOP 5 before BOTTOM 5)
// - Removes intermediate ranking and raw second-based columns
val finalResult =
  combined
    .orderBy("TYP_DESC", "CIP_JOBS", "group_order", "avg_response_sec")
    .drop(
      "rank_fast",
      "rank_slow",
      "group_order",
      "avg_response_sec",
      "median_response_sec",
      "min_response_sec",
      "max_response_sec"
    )

// Display final ranked comparison table (top vs bottom response performance)
println("=== 4. Detailed Precinct-level Response Time Stats ===")
finalResult.show(200, false)
