// Analysis Part 3: Temporal patterns (Hour-of-Day)
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

val dfTime = df
  .filter(col("RESPONSE_TIME_SEC").isNotNull)
  .filter(col("RESPONSE_TIME_SEC") > 0)

// Compute response time statistics across each hour of the day
// Use ADD_TS to create a new column with extracted hour
val dfHour=dfTime.withColumn("hour_of_day",hour(col("ADD_TS")))

val hourlyStatsNonCritical =
  dfHour
    .filter(col("CIP_JOBS") === "NON CRITICAL")
    .groupBy("hour_of_day")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
      count("*").alias("num_records")
    )
    .withColumn("avg_response_time", date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss"))
    .drop("avg_response_sec")
    .orderBy("hour_of_day")

hourlyStatsNonCritical.show(24, false)

hourlyStatsNonCritical.orderBy(desc("avg_response_time")).show(5, false)

val hourlyStatsHighSeverity =
  dfHour
    .filter(col("CIP_JOBS") =!= "NON CRITICAL")
    .groupBy("hour_of_day")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
      count("*").alias("num_records")
    )
    .withColumn("avg_response_time", date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss"))
    .drop("avg_response_sec")
    .orderBy("hour_of_day")

hourlyStatsHighSeverity.show(24, false)

hourlyStatsHighSeverity.orderBy(desc("avg_response_time")).show(5, false)


// 3a2 Hourly stats with avg_dispatch_delay_time and avg_travel_time
val hourlyStatsNonCritical_v2 =
  dfHour
    .filter(col("CIP_JOBS") === "NON CRITICAL")
    .groupBy("hour_of_day")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
      avg("DISPATCH_DELAY_SEC").alias("avg_dispatch_delay_sec"),
      avg("TRAVEL_TIME_SEC").alias("avg_travel_sec"),
      count("*").alias("num_records")
    )
    .withColumn("avg_response_time", date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss"))
    .withColumn("avg_dispatch_delay_time", date_format(from_unixtime(col("avg_dispatch_delay_sec")), "HH:mm:ss"))
    .withColumn("avg_travel_time", date_format(from_unixtime(col("avg_travel_sec")), "HH:mm:ss"))
    .drop("avg_response_sec", "avg_dispatch_delay_sec", "avg_travel_sec")
    .orderBy("hour_of_day")

hourlyStatsNonCritical_v2.show(24, false)

val hourlyStatsHighSeverity_v2 =
  dfHour
    .filter(col("CIP_JOBS") =!= "NON CRITICAL")
    .groupBy("hour_of_day")
    .agg(
      avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
      avg("DISPATCH_DELAY_SEC").alias("avg_dispatch_delay_sec"),
      avg("TRAVEL_TIME_SEC").alias("avg_travel_sec"),
      count("*").alias("num_records")
    )
    .withColumn("avg_response_time", date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss"))
    .withColumn("avg_dispatch_delay_time", date_format(from_unixtime(col("avg_dispatch_delay_sec")), "HH:mm:ss"))
    .withColumn("avg_travel_time", date_format(from_unixtime(col("avg_travel_sec")), "HH:mm:ss"))
    .drop("avg_response_sec", "avg_dispatch_delay_sec", "avg_travel_sec")
    .orderBy("hour_of_day")

hourlyStatsHighSeverity_v2.show(24, false)
// 3GENERIC
//val dfHour = dfTime.withColumn("hour_of_day", hour(col("ADD_TS")))
//
//println("=== Response Time by Hour of Day ===")
//
//val hourlyStats = dfHour
//  .groupBy("hour_of_day")
//  .agg(
//    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
//    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
//    stddev("RESPONSE_TIME_SEC").alias("stddev_response_sec"),
//    count("*").alias("num_records")
//  )
//  .orderBy("hour_of_day")
//
//hourlyStats.show(24, false)
//
//println("=== Top 5 Slowest Hours ===")
//hourlyStats.orderBy(desc("avg_response_sec")).show(5, false)
//      hourlyStats.orderBy("num_records").show(24, false)