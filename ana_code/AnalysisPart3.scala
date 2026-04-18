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

// -- Compute response, dispatch, and travel time statistics across each hour of the day --

// Use ADD_TS to create a new column with extracted hour
val dfHour=dfTime.withColumn("hour_of_day",hour(col("ADD_TS")))

val dfHourlyStatsNonCritical =
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

dfHourlyStatsNonCritical.show(24, false)

val dfHourlyStatsHighSeverity =
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

dfHourlyStatsHighSeverity.show(24, false)

// Save all generated dataframes to HDFS for visualization usage later
val hdfsOutputPath = "hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part3"

dfHourlyStatsNonCritical.write.mode("overwrite").parquet(hdfsOutputPath + "/dfHourlyStatsNonCritical")
dfHourlyStatsHighSeverity.write.mode("overwrite").parquet(hdfsOutputPath + "/dfHourlyStatsHighSeverity")