/* This file performs the following initial data analysis:
  1. Calculates the mean and median for the selected numerical data columns.
  2. Calculates the mode for the selected categorical columns.
  3. Calculates the std dev for RESPONSE_TIME column.

  It then performs the following code cleaning operations:
  1. Date Formatting
  2. Text formatting
*/
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

// Create Spark session
val spark = SparkSession.builder()
  .appName("NYPD Service Calls Analysis")
  .getOrCreate()

import spark.implicits._

// Read cleaned parquet data from HDFS
val hdfsInputPath = "hdfs:///user/jhc10024_nyu_edu/hw6/nypd_cfs_cleaned"
val dfCleanFinal = spark.read.parquet(hdfsInputPath)


// Part 1 - Initial Code Analysis:
// Columns for analysis
val numCols = Seq("RESPONSE_TIME", "DISPATCH_DELAY", "TRAVEL_TIME")
val catCols = Seq("BORO_NM", "RADIO_CODE", "TYP_DESC_ENUM", "CIP_JOBS")

// Convert HH:mm:ss strings to seconds
def timeStrToSec(colName: String) = {
  val splitCol = split(col(colName), ":")
  splitCol.getItem(0).cast("int") * 3600 + splitCol.getItem(1).cast("int") * 60 + splitCol.getItem(2).cast("int")
}

// Add new columns converting time strings (HH:mm:ss)
// into seconds for each column in numCols
val dfWithSec = numCols.foldLeft(dfCleanFinal) { (df, c) =>
  df.withColumn(c + "_SEC", timeStrToSec(c))
}

// 1. Calculate mean
val meanDF = dfWithSec.select(numCols.map(c => mean(col(c + "_SEC")).alias(c + "_mean_sec")): _*)
meanDF.show(false)

// 2. Calculate median
numCols.foreach { c =>
  val medianVal = dfWithSec.stat.approxQuantile(c + "_SEC", Array(0.5), 0.001)
  println(s"Median of $c: ${medianVal(0)} seconds")
}

// 3. Calculate standard deviation for RESPONSE_TIME
val stdDevResponse = dfWithSec.select(stddev(col("RESPONSE_TIME_SEC")).alias("RESPONSE_TIME_stddev_sec"))
stdDevResponse.show(false)

// 4. Calculate mode for categorical columns
catCols.foreach { c =>
  val modeDF = dfCleanFinal.groupBy(col(c))
    .count()
    .orderBy(desc("count"))
    .limit(1)
  println(s"Mode of $c:")
  modeDF.show(false)
}


// Part 2 - Code cleaning: Date formatting and Text formatting
// 1. Date Formatting
// dfCleanDates preserves _SEC columns calculated in Part 1
val dfCleanDates = dfWithSec
  .withColumn("CREATE_DATE", to_date(col("CREATE_DATE"), "MM/dd/yyyy"))
  .withColumn("INCIDENT_DATE", to_date(col("INCIDENT_DATE"), "MM/dd/yyyy"))
  .withColumn("ADD_TS", to_timestamp(col("ADD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("DISP_TS", to_timestamp(col("DISP_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("ARRIVD_TS", to_timestamp(col("ARRIVD_TS"), "MM/dd/yyyy hh:mm:ss a"))
  .withColumn("CLOSNG_TS", to_timestamp(col("CLOSNG_TS"), "MM/dd/yyyy hh:mm:ss a"))
  // Convert INCIDENT_TIME from 24-hour string to 12-hour string with AM/PM
  // to be consistent with other timestamp columns' that follow 12hr time format
  .withColumn("INCIDENT_TIME",
    date_format(to_timestamp(col("INCIDENT_TIME"), "HH:mm:ss"), "hh:mm:ss a")
  )

// 2. Text Formatting - normalize categorical columns for consistency
// Trim categorical values and convert to uppercase
val dfCleanText = dfCleanDates
  .withColumn("BORO_NM", upper(trim(col("BORO_NM"))))
  .withColumn("RADIO_CODE", upper(trim(col("RADIO_CODE"))))
  .withColumn("TYP_DESC", upper(trim(col("TYP_DESC"))))
  .withColumn("CIP_JOBS", upper(trim(col("CIP_JOBS"))))

// dfCleanText now contains formatted dates, normalized text columns, and
// _SEC columns from Part 1

val hdfsOutputPath = "hdfs:///user/jhc10024_nyu_edu/hw6/nypd_cfs_cleaned_1"

dfCleanText
  .repartition(50)
  .write
  .mode("overwrite")
  .parquet(hdfsOutputPath)