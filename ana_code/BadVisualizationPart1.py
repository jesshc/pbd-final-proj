from pyspark.sql import SparkSession
from pyspark.sql.functions import desc, asc

spark = SparkSession.builder.getOrCreate()

# Load data from HDFS (Part1 outputs)
hdfsInputPath = "hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1"

print("Loading parquet datasets from HDFS...")

dfboroughStats = spark.read.parquet(hdfsInputPath + "/dfboroughStats")
dfBoroughResponse = spark.read.parquet(hdfsInputPath + "/dfBoroughResponse")
dfPrecinctStats = spark.read.parquet(hdfsInputPath + "/dfPrecinctStats")
dfPrecinctResponseRanked = spark.read.parquet(hdfsInputPath + "/dfPrecinctResponseRanked")

print("Successfully loaded all datasets\n")

# Borough-level summary
print("dfboroughStats (Top 5 sample rows)")
dfboroughStats.show(5, False)

# Borough detailed breakdown
print("dfBoroughResponse Schema")
dfBoroughResponse.printSchema()

print("dfBoroughResponse (Sample rows)")
dfBoroughResponse.show(10, False)

# Precinct-level stats
print("TOP 5 SLOWEST PRECINCTS (by AVG_RESPONSE_SEC)")
dfPrecinctStats.orderBy(desc("AVG_RESPONSE_SEC")).show(5, False)

print("TOP 5 FASTEST PRECINCTS (by AVG_RESPONSE_SEC)")
dfPrecinctStats.orderBy(asc("AVG_RESPONSE_SEC")).show(5, False)

# Ranked precinct response dataset
print("dfPrecinctResponseRanked Schema")
dfPrecinctResponseRanked.printSchema()

print("dfPrecinctResponseRanked (Sample rows)")
dfPrecinctResponseRanked.show(10, False)

print("Data loading + preview complete")