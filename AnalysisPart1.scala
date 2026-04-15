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
    avg("RESPONSE_TIME_SEC").alias("AVG_RESPONSE_SEC"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("MEDIAN_RESPONSE_SEC"),
    stddev("RESPONSE_TIME_SEC").alias("STDDEV_RESPONSE_SEC"),
    count("*").alias("NUM_RECORDS")
  )
  .orderBy(desc("AVG_RESPONSE_SEC"))
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
  dfFiltered.withColumn("RADIO_CODE_PREFIX", substring(col("RADIO_CODE"), 1, 2))
    .withColumn(
      "TYP_DESC",
      when(col("RADIO_CODE_PREFIX").isin(broadcastMap.value.keys.toSeq: _*),
        expr(s"""CASE ${broadcastMap.value.map { case (k,v) =>
          s"WHEN RADIO_CODE_PREFIX='$k' THEN '$v'"
        }.mkString(" ")} ELSE 'UNKNOWN' END""")
      ).otherwise("UNKNOWN")
    )
    .groupBy("BORO_NM","RADIO_CODE_PREFIX","TYP_DESC","CIP_JOBS")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("AVG_RESPONSE_SEC"),
      expr("percentile_approx(RESPONSE_TIME_SEC,0.5)").alias("MEDIAN_RESPONSE_SEC"),
      min("RESPONSE_TIME_SEC").alias("MIN_RESPONSE_SEC"),
      max("RESPONSE_TIME_SEC").alias("MAX_RESPONSE_SEC"),
      count("*").alias("INCIDENT_COUNT")
    )
    .orderBy(desc("INCIDENT_COUNT"))
    .withColumn("AVG_RESPONSE_TIME", date_format(from_unixtime(col("AVG_RESPONSE_SEC")), "HH:mm:ss"))
    .withColumn("MEDIAN_RESPONSE_TIME", date_format(from_unixtime(col("MEDIAN_RESPONSE_SEC")), "HH:mm:ss"))
    .withColumn("MIN_RESPONSE_TIME", date_format(from_unixtime(col("MIN_RESPONSE_SEC")), "HH:mm:ss"))
    .withColumn("MAX_RESPONSE_TIME", date_format(from_unixtime(col("MAX_RESPONSE_SEC")), "HH:mm:ss"))
    .select(
      "BORO_NM",
      "RADIO_CODE_PREFIX",
      "TYP_DESC",
      "CIP_JOBS",
      "AVG_RESPONSE_TIME",
      "MEDIAN_RESPONSE_TIME",
      "MIN_RESPONSE_TIME",
      "MAX_RESPONSE_TIME",
      "INCIDENT_COUNT"
    )

println("=== 2a. Detailed Borough-level Response Time Stats ===")
dfBoroughResponse.orderBy(col("BORO_NM"), col("AVG_RESPONSE_TIME")).show(55, false)

println("=== 2b. Detailed Borough-level Response Time Stats ===")
dfBoroughResponse.orderBy(col("TYP_DESC"), col("CIP_JOBS"), col("AVG_RESPONSE_TIME")).show(55, false)


// 3. Precinct-level analysis of Response Times
println("=== Response Time by Precinct ===")
val precinctStats = dfFiltered
  .groupBy("BORO_NM", "NYPD_PCT_CD")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("AVG_RESPONSE_SEC"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("MEDIAN_RESPONSE_SEC"),
    stddev("RESPONSE_TIME_SEC").alias("STDDEV_RESPONSE_SEC"),
    count("*").alias("NUM_RECORDS")
  )
  .orderBy(desc("AVG_RESPONSE_SEC"))

// Top 10 slowest precincts
println("=== 3a. Top 10 Slowest Precincts ===")
precinctStats.orderBy(desc("AVG_RESPONSE_SEC")).show(10, false)

// Top 10 fastest precincts
println("=== 3b. Top 10 Fastest Precincts ===");
precinctStats.orderBy(asc("AVG_RESPONSE_SEC")).show(10, false)


// 4. Detailed Precinct-level Response Time Stats
val dfPrecinctResponse =
  dfFiltered
    .withColumn("RADIO_CODE_PREFIX", substring(col("RADIO_CODE"), 1, 2))
    .withColumn(
      "TYP_DESC",
      when(col("RADIO_CODE_PREFIX").isin(broadcastMap.value.keys.toSeq: _*),
        expr(s"""CASE ${broadcastMap.value.map { case (k,v) =>
          s"WHEN RADIO_CODE_PREFIX='$k' THEN '$v'"
        }.mkString(" ")} ELSE 'UNKNOWN' END""")
      ).otherwise("UNKNOWN")
    )
    .groupBy("BORO_NM", "NYPD_PCT_CD", "RADIO_CODE_PREFIX", "TYP_DESC", "CIP_JOBS")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("AVG_RESPONSE_SEC"),
      expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("MEDIAN_RESPONSE_SEC"),
      min("RESPONSE_TIME_SEC").alias("MIN_RESPONSE_SEC"),
      max("RESPONSE_TIME_SEC").alias("MAX_RESPONSE_SEC"),
      count("*").alias("INCIDENT_COUNT")
    )

val w = Window.partitionBy("TYP_DESC","CIP_JOBS").orderBy(col("AVG_RESPONSE_SEC"))

val ranked =
  dfPrecinctResponse
    .withColumn("RANK_FAST", row_number().over(w))
    .withColumn("RANK_SLOW", row_number().over(w.orderBy(col("AVG_RESPONSE_SEC").desc)))

val top5 =
  ranked
    .filter(col("RANK_FAST") <= 5)
    .withColumn("GROUP_TYPE", lit("TOP_5_FASTEST"))
    .withColumn("GROUP_ORDER", lit(0))

val bottom5 =
  ranked
    .filter(col("RANK_SLOW") <= 5)
    .withColumn("GROUP_TYPE", lit("BOTTOM_5_SLOWEST"))
    .withColumn("GROUP_ORDER", lit(1))

val combined =
  top5.union(bottom5)
    .withColumn("AVG_RESPONSE_TIME", date_format(from_unixtime(col("AVG_RESPONSE_SEC")), "HH:mm:ss"))
    .withColumn("MEDIAN_RESPONSE_TIME", date_format(from_unixtime(col("MEDIAN_RESPONSE_SEC")), "HH:mm:ss"))
    .withColumn("MIN_RESPONSE_TIME", date_format(from_unixtime(col("MIN_RESPONSE_SEC")), "HH:mm:ss"))
    .withColumn("MAX_RESPONSE_TIME", date_format(from_unixtime(col("MAX_RESPONSE_SEC")), "HH:mm:ss"))

val finalResult =
  combined
    .orderBy("TYP_DESC", "CIP_JOBS", "GROUP_ORDER", "AVG_RESPONSE_SEC")
    .drop(
      "RANK_FAST",
      "RANK_SLOW",
      "GROUP_ORDER",
      "AVG_RESPONSE_SEC",
      "MEDIAN_RESPONSE_SEC",
      "MIN_RESPONSE_SEC",
      "MAX_RESPONSE_SEC"
    )

println("=== 4. Detailed Precinct-level Response Time Stats ===")
finalResult.show(200, false)