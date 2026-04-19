## NYPD Response Time Analysis Project — README

### Project overview

This project processes the NYPD Calls for Service dataset using NYU Dataproc with Spark and HDFS. The work was done in the following order:

1. Data ingestion
2. Data profiling
3. ETL cleaning
4. Analysis
5. Visualization

### Directory structure

- `/data_ingest`
  - DataIngestInstructions.md — instructions for uploading the raw CSV into HDFS on NYU Dataproc
- `/profiling_code`
  - CountRecs.scala — raw data profiling and validation on the ingested CSV
- `/etl_code`
  - Clean.scala — Stage 1 cleaning: deduplication, type conversion, bad row removal, time feature creation, and parquet output to HDFS
  - CleanStage2.scala — Stage 2 cleaning: filters only CIP incidents, removes unrealistic response times, and writes a second cleaned parquet dataset to HDFS
  - CleanStage3.scala — Stage 3 cleaning: date/timestamp formatting, text normalization, and time-to-seconds conversion for numerical analysis
- `/ana_code`
  - AnalysisPart0.scala — initial data analysis: mean/median/mode calculations and descriptive statistics
  - AnalysisPart1.scala — borough, precinct, and incident-type response time exploration
  - AnalysisPart2.scala — severity-level impact analysis
  - AnalysisPart3.scala — temporal (hourly) response time patterns
  - VisualizationPart1.py — generates Part 1 charts from saved parquet outputs
  - VisualizationPart2.py — generates Part 2 charts from saved parquet outputs
  - VisualizationPart3.py — generates Part 3 charts from saved parquet outputs
- `/screenshots`
  - contains image examples of analysis and visualization outputs

---

## Step 1: Data ingestion

Follow `data_ingest/DataIngestInstructions.md` to complete data ingestion.

---

## Step 2: Profiling

Run the profiling code first to understand raw data quality.

### File

- `/profiling_code/CountRecs.scala`

### What it does

- Loads the raw CSV
- Counts total records
- Checks distinct values for key columns
- Examines RADIO_CODE / TYP_DESC mappings
- Finds duplicate `CAD_EVNT_ID` rows
- Validates date ranges and time formatting
- Detects suspicious incident timestamp anomalies

### Run on NYU Dataproc

Connect to your Dataproc instance via the Google Cloud web SSH interface, then run:

```bash
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/profiling_code/CountRecs.scala
```

---

## Step 3: ETL cleaning

After profiling, run ETL cleaning in 3 stages.

### Stage 1

- `/etl_code/Clean.scala`

### What it does

- Read the raw CSV from HDFS at `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs.csv`
- Deduplicates on `CAD_EVNT_ID`
- Converts dates/timestamps
- Creates time difference fields:
  - `RESPONSE_TIME`
  - `DISPATCH_DELAY`
  - `TRAVEL_TIME`
- Filters invalid borough values and bad timestamp order
- Writes cleaned parquet to HDFS:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned`

### Run

```bash
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/etl_code/Clean.scala
```

### Stage 2

- `/etl_code/CleanStage2.scala`

### What it does

- Reads Stage 1's cleaned parquet
- Keeps only CIP-related job categories: `SERIOUS`, `CRITICAL`, `NON CRITICAL`
- Removes records with unrealistic response times (`<= 90 seconds`)
- Writes a refined parquet dataset to HDFS:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned_1`

### Run

```bash
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/etl_code/CleanStage2.scala
```

### Stage 3

- `/etl_code/CleanStage3.scala`

### What it does

- Reads Stage 2 cleaned parquet
- Formats date columns to proper date/timestamp types
- Normalizes categorical text columns (uppercase, trim)
- Converts time fields (`RESPONSE_TIME`, `DISPATCH_DELAY`, `TRAVEL_TIME`) to seconds for numerical analysis
- Writes final cleaned parquet dataset to HDFS:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned_2`

### Run

```bash
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/etl_code/CleanStage3.scala
```

---

## Step 4: Analysis

Analysis scripts read cleaned HDFS datasets and produce summary data frames.

### `/ana_code/AnalysisPart0.scala`

- Reads HDFS parquet input from:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned_1`
- Performs initial descriptive statistics:
  - Mean, median, and standard deviation for response time fields
  - Mode for categorical columns
- Outputs summary statistics to console
- Does not save results to HDFS (outputs to console only)

### `/ana_code/AnalysisPart1.scala`

- Reads HDFS parquet input from:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned_2`
- Performs:
  - Borough-level response time stats
  - Incident-type (`RADIO_CODE` / CIP) response statistics
  - Precinct-level ranking of fastest/slower precincts
- Outputs results to console and saves results to HDFS in the following locations:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1/dfBoroughStats`
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1/dfBoroughResponse`
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1/dfPrecinctStats`
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1/dfPrecinctResponseRanked`

> Note: the code currently expects cleaned input at `nypd_cfs_cleaned_2`. If your ETL output is only available at `nypd_cfs_cleaned_1`, update the path in the script or rename the HDFS folder accordingly.

### `/ana_code/AnalysisPart2.scala`

- Reads the same cleaned HDFS dataset
- Computes response time, dispatch delay, and travel time by severity level
- Outputs results to console and saves results to HDFS in the following locations:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part2/dfCIPStats`

### `/ana_code/AnalysisPart3.scala`

- Reads the same cleaned HDFS dataset
- Computes hourly response statistics for:
  - `NON CRITICAL`
  - high severity (`SERIOUS` + `CRITICAL`)
- Outputs results to console and saves results to HDFS in the following locations:
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part3/dfHourlyStatsNonCritical`
  - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part3/dfHourlyStatsHighSeverity`

### Run on Dataproc

```bash
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/ana_code/AnalysisPart0.scala
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/ana_code/AnalysisPart1.scala
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/ana_code/AnalysisPart2.scala
spark-shell --deploy-mode client -i /home/<your-user>/jhc10024-project/ana_code/AnalysisPart3.scala
```

---

## Step 4.5: Download Analysis Results from HDFS to Local Dataproc Server

Before running the Python visualization scripts, you need to download the saved parquet files 
from HDFS to your local filesystem in your home directory.

### Download commands

From your Dataproc instance, run:

```bash
cd /home/<your-user>/jhc10024-project/ana_code

# Download Part 1 analysis results
hdfs dfs -get /user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1 <your-directory>/Part1

# Download Part 2 analysis results
hdfs dfs -get /user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part2 <your-directory>/Part2

# Download Part 3 analysis results
hdfs dfs -get /user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part3 <your-directory>/Part3
```

---

## Step 5: Visualization

Visualization scripts generate final images from saved parquet outputs. 
Before running the Python visualization scripts, make sure you are in `<your-directory>`, the parent folder that contains the `Part1`, `Part2`, and `Part3` directories.

### Files

- `ana_code/VisualizationPart1.py`
- `ana_code/VisualizationPart2.py`
- `ana_code/VisualizationPart3.py`

### What they do

- VisualizationPart1.py
  - Reads local parquet data in `./Part1/`
  - Saves plots such as borough response time and precinct scatter charts
- VisualizationPart2.py
  - Reads local parquet data in `./Part2/`
  - Saves severity-level response plots
- VisualizationPart3.py
  - Reads local parquet data in `./Part3/`
  - Saves hourly response breakdown plots

### Run

From the local Python environment:

```bash
cd /home/<your-user>/jhc10024-project/ana_code
python3 VisualizationPart1.py
python3 VisualizationPart2.py
python3 VisualizationPart3.py
```

### Notes

- The Python scripts expect parquet input in local folders (as downloaded in Step 4.5):
  - `./Part1/`
  - `./Part2/`
  - `./Part3/`
- After running the Python visualizations, use the **Download File** feature in the web SSH interface to download the generated .png files to your local machine to view them

---

## Where to find results

- HDFS outputs:
  - cleaned parquet: `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned`
  - stage 2 cleaned: `hdfs:///user/jhc10024_nyu_edu/FinalProject/nypd_cfs_cleaned_1`
  - analysis outputs:
    - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part1`
    - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part2`
    - `hdfs:///user/jhc10024_nyu_edu/FinalProject/AnalysisOutputs/Part3`
- Visualization graphs (.png files) are saved in the same directory as where you run the VisualizationPart#.py scripts 
  (pre-generated graphs are saved in the `/screenshots` directory):
