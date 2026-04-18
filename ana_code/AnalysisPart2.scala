// Analysis Part 2: Incident-type Impact
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

// Filter for valid records to use
val dfCIP = df
  .filter(col("CIP_JOBS").isNotNull)
  .filter(col("RESPONSE_TIME_SEC").isNotNull)
  .filter(col("RESPONSE_TIME_SEC") > 0)


// Compute response time stats by Incident Severity Level
println("=== Incident Type Impact (CIP_JOBS) ===")

val dfCIPStats = dfCIP
  .groupBy("CIP_JOBS")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),

    avg("DISPATCH_DELAY_SEC").alias("avg_dispatch_sec"),
    expr("percentile_approx(DISPATCH_DELAY_SEC, 0.5)").alias("median_dispatch_sec"),

    avg("TRAVEL_TIME_SEC").alias("avg_travel_sec"),
    expr("percentile_approx(TRAVEL_TIME_SEC, 0.5)").alias("median_travel_sec"),

    count("*").alias("num_records")
  )
  .withColumn(
    "avg_response_time",
    date_format(from_unixtime(col("avg_response_sec")), "HH:mm:ss")
  )
  .withColumn(
    "median_response_time",
    date_format(from_unixtime(col("median_response_sec")), "HH:mm:ss")
  )
  .withColumn(
    "avg_dispatch_time",
    date_format(from_unixtime(col("avg_dispatch_sec")), "HH:mm:ss")
  )
  .withColumn(
    "median_dispatch_time",
    date_format(from_unixtime(col("median_dispatch_sec")), "HH:mm:ss")
  )
  .withColumn(
    "avg_travel_time",
    date_format(from_unixtime(col("avg_travel_sec")), "HH:mm:ss")
  )
  .withColumn(
    "median_travel_time",
    date_format(from_unixtime(col("median_travel_sec")), "HH:mm:ss")
  )
  .drop(
    "avg_response_sec",
    "median_response_sec",
    "avg_dispatch_sec",
    "median_dispatch_sec",
    "avg_travel_sec",
    "median_travel_sec"
  )
  .orderBy(asc("avg_response_time"))

dfCIPStats.show(false)

// Save generated dataframe to HDFS for visualization usage later
val hdfsOutputPath = "hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part2"

dfCIPStats.write.mode("overwrite").parquet(hdfsOutputPath + "/dfCIPStats")
