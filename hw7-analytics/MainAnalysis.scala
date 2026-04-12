import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create Spark session
val spark = SparkSession.builder()
  .appName("NYPD Service Calls Analysis")
  .getOrCreate()

import spark.implicits._

// Read cleaned parquet data from HDFS
val hdfsInputPath = "hdfs:///user/jhc10024_nyu_edu/hw6/nypd_cfs_cleaned_2"
val df = spark.read.parquet(hdfsInputPath)


// Part 1a: Analyze Response Time Disparities

// Filter valid response times
val dfFiltered = df
  .filter(col("RESPONSE_TIME_SEC").isNotNull)
  .filter(col("RESPONSE_TIME_SEC") > 0)

// Borough-level analysis
println("=== Response Time by Borough ===")
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

// Precinct-level analysis
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
//precinctStats.show(50, false)

// Top 10 slowest precincts
println("=== Top 10 Slowest Precincts ===")
precinctStats.orderBy(desc("avg_response_sec")).show(10, false)

// Top 10 fastest precincts
println("=== Top 10 Fastest Precincts ===");
precinctStats.orderBy(asc("avg_response_sec")).show(10, false);


// Part 1b: Analyze Dispatch Delay Disparities
val dfDispatchFiltered = df.filter(col("DISPATCH_DELAY_SEC").isNotNull).filter(col("DISPATCH_DELAY_SEC") > 0)

println("=== Dispatch Delay by Borough ===")
val boroughDispatchStats = dfDispatchFiltered
  .groupBy("BORO_NM")
  .agg(
    avg("DISPATCH_DELAY_SEC").alias("avg_dispatch_sec"),
    expr("percentile_approx(DISPATCH_DELAY_SEC, 0.5)").alias("median_dispatch_sec"),
    stddev("DISPATCH_DELAY_SEC").alias("stddev_dispatch_sec"),
    count("*").alias("num_records")
  )
  .orderBy(desc("avg_dispatch_sec"))
boroughDispatchStats.show(false)

println("=== Dispatch Delay by Precinct ===")
val precinctDispatchStats = dfDispatchFiltered
  .groupBy("BORO_NM", "NYPD_PCT_CD")
  .agg(
    avg("DISPATCH_DELAY_SEC").alias("avg_dispatch_sec"),
    expr("percentile_approx(DISPATCH_DELAY_SEC, 0.5)").alias("median_dispatch_sec"),
    stddev("DISPATCH_DELAY_SEC").alias("stddev_dispatch_sec"),
    count("*").alias("num_records")
  )
  .orderBy(desc("avg_dispatch_sec"))
//precinctDispatchStats.show(50, false)

println("=== Top 10 Slowest Precincts by Dispatch Delay ===")
precinctDispatchStats.orderBy(desc("avg_dispatch_sec")).show(10, false)

println("=== Top 10 Fastest Precincts by Dispatch Delay ===")
precinctDispatchStats.orderBy(asc("avg_dispatch_sec")).show(10, false)


// Part 1c: Analyze Travel Time Disparities
val dfTravelFiltered = df.filter(col("TRAVEL_TIME_SEC").isNotNull).filter(col("TRAVEL_TIME_SEC") > 0)

println("=== Travel Time by Borough ===")
val boroughTravelStats = dfTravelFiltered
  .groupBy("BORO_NM")
  .agg(
    avg("TRAVEL_TIME_SEC").alias("avg_travel_sec"),
    expr("percentile_approx(TRAVEL_TIME_SEC, 0.5)").alias("median_travel_sec"),
    stddev("TRAVEL_TIME_SEC").alias("stddev_travel_sec"),
    count("*").alias("num_records")
  )
  .orderBy(desc("avg_travel_sec"))
boroughTravelStats.show(false)

println("=== Travel Time by Precinct ===")
val precinctTravelStats = dfTravelFiltered
  .groupBy("BORO_NM", "NYPD_PCT_CD")
  .agg(
    avg("TRAVEL_TIME_SEC").alias("avg_travel_sec"),
    expr("percentile_approx(TRAVEL_TIME_SEC, 0.5)").alias("median_travel_sec"),
    stddev("TRAVEL_TIME_SEC").alias("stddev_travel_sec"),
    count("*").alias("num_records")
  )
  .orderBy(desc("avg_travel_sec"))
//precinctTravelStats.show(50, false)

println("=== Top 10 Slowest Precincts by Travel Time ===")
precinctTravelStats.orderBy(desc("avg_travel_sec")).show(10, false)

println("=== Top 10 Fastest Precincts by Travel Time ===")
precinctTravelStats.orderBy(asc("avg_travel_sec")).show(10, false)


// Part 2: Incident-Type Impact (CIP_JOBS)

// Filter for valid records to use
val dfCIP = df
  .filter(col("CIP_JOBS").isNotNull)
  .filter(col("RESPONSE_TIME_SEC").isNotNull)
  .filter(col("RESPONSE_TIME_SEC") > 0)

println("=== Incident Type Impact (CIP_JOBS) ===")

val cipStats = dfCIP
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
  .orderBy(asc("avg_response_sec"))   // fastest (most prioritized) first

cipStats.show(false)


// Part 3: Temporal Patterns

val dfTime = df
  .filter(col("RESPONSE_TIME_SEC").isNotNull)
  .filter(col("RESPONSE_TIME_SEC") > 0)


// 3a: Hour of Day
// Use ADD_TS to create a new column with extracted hour
val dfHour = dfTime.withColumn("hour_of_day", hour(col("ADD_TS")))

println("=== Response Time by Hour of Day ===")

val hourlyStats = dfHour
  .groupBy("hour_of_day")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
    stddev("RESPONSE_TIME_SEC").alias("stddev_response_sec"),
    count("*").alias("num_records")
  )
  .orderBy("hour_of_day")

hourlyStats.show(24, false)

println("=== Top 5 Slowest Hours ===")
hourlyStats.orderBy(desc("avg_response_sec")).show(5, false)


// 3b: Month
val dfMonth = dfTime.withColumn("month", month(col("ADD_TS")))

println("=== Response Time by Month ===")

val monthlyStats = dfMonth
  .groupBy("month")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
    stddev("RESPONSE_TIME_SEC").alias("stddev_response_sec"),
    count("*").alias("num_records")
  )
  .orderBy("month")

monthlyStats.show(12, false)

println("=== Top 5 Slowest Months ===")
monthlyStats.orderBy(desc("avg_response_sec")).show(5, false)


// 3c: Day of Week
val dfDay = dfTime.withColumn("day_of_week", date_format(col("ADD_TS"), "E"))

println("=== Response Time by Day of Week ===")

val dayStats = dfDay
  .groupBy("day_of_week")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    expr("percentile_approx(RESPONSE_TIME_SEC, 0.5)").alias("median_response_sec"),
    stddev("RESPONSE_TIME_SEC").alias("stddev_response_sec"),
    count("*").alias("num_records")
  )

dayStats.show(false)


// Cross Analysis 1: Hour × Borough (Response Time)
println("=== Hour x Borough (Response Time) ===");
val hourBoroStats = dfHour
  .groupBy("hour_of_day","BORO_NM")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    count("*").alias("num_records")
  )
  .orderBy(col("hour_of_day"), desc("avg_response_sec"));

hourBoroStats.show(50, false);


// Cross Analysis 2: Hour × CIP_JOBS
println("=== Hour x CIP_JOBS ===");
val hourCIPStats = dfHour
  .filter(col("CIP_JOBS").isNotNull)
  .groupBy("hour_of_day","CIP_JOBS")
  .agg(
    avg("RESPONSE_TIME_SEC").alias("avg_response_sec"),
    count("*").alias("num_records")
  )
  .orderBy(col("hour_of_day"), desc("avg_response_sec"));

hourCIPStats.show(50, false);