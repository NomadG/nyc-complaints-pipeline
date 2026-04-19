from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types
import os

BUCKET_NAME = None  # set via --bucket arg in __main__


# creating spark context
spark = SparkSession.builder \
    .appName('nyc-complaints-transform') \
    .getOrCreate()


def transform_complaints():
    #schema enforcement for the complaints data
    df_schema = types.StructType([
        types.StructField('unique_key', types.StringType(), False), #kept this false because this is the primary key
        types.StructField('closed_date', types.TimestampType(), True),
        types.StructField('created_date', types.TimestampType(), True),
        types.StructField('due_date', types.TimestampType(), True),
        types.StructField('resolution_action_updated_date', types.TimestampType(), True),
        types.StructField('address_type', types.StringType(), True),
        types.StructField('agency', types.StringType(), True),
        types.StructField('agency_name', types.StringType(), True),
        types.StructField('bbl', types.StringType(), True),
        types.StructField('borough', types.StringType(), True),
        types.StructField('city', types.StringType(), True),
        types.StructField('community_board', types.StringType(), True),
        types.StructField('complaint_type', types.StringType(), True),
        types.StructField('council_district', types.StringType(), True),
        types.StructField('descriptor', types.StringType(), True),
        types.StructField('descriptor_2', types.StringType(), True),
        types.StructField('incident_zip', types.StringType(), True),
        types.StructField('latitude', types.StringType(), True),
        types.StructField('location_type', types.StringType(), True),
        types.StructField('longitude', types.StringType(), True),
        types.StructField('police_precinct', types.StringType(), True),
        types.StructField('status', types.StringType(), True),
        types.StructField(':created_at', types.TimestampType(), True),
        types.StructField(':updated_at', types.TimestampType(), True)])


    # this is for getting the watermark if not already there

    # 1. Define your path
    silver_path = f"gs://{BUCKET_NAME}/silver/complaints/"

    # 2. Check if files exist and get the 'Watermark'
    try:
        # Try to read the max updated_at from your Silver Parquet files
        # Spark is smart enough to check if the path exists here
        last_modified = spark.read \
        .format("binaryFile") \
        .load(silver_path) \
        .select(F.max("modificationTime")) \
        .collect()[0][0]

        last_modified = last_modified or "2025-01-01T00:00:00"

        print(f"Silver last modified: {last_modified}")


    except Exception:
        # If the folder doesn't exist or is empty, Spark will throw an error.
        # We catch that error and set a "First Run" date.
        last_modified = "1970-01-01T00:00:00"
        print("First Run: Pulling all available records.")


    bronze_path = f"gs://{BUCKET_NAME}/bronze/complaints/"
    df_new = spark.read \
        .format("json") \
        .schema(df_schema) \
        .option("modifiedAfter", last_modified) \
        .load(bronze_path)

    # removing duplicate rows in case there are any
    df_new = df_new.dropDuplicates()

    # adding a column for calculating the resolution time for the closed cases
    df_new = df_new.withColumn(
        "resolution_time_days",
        F.when(
            F.col("status") == "Closed", 
            F.datediff(F.to_timestamp("closed_date"), F.to_timestamp("created_date"))
        ).otherwise(F.lit(None)) # This sets it to null for any other status
    )


    # adding a column for calculating the SLA time
    df_new = df_new.withColumn("SLA_days", F.datediff(F.to_timestamp("due_date"), F.to_timestamp("created_date")))


    # Option A: Custom Date only
    # df_final = df_new.withColumn("ingest_date", F.to_date(F.lit("2026-04-10")))

    # Option B: Custom Full Timestamp
    # custom_ts = "2026-04-10 14:30:00"
    df_new = df_new.withColumn("ingest_timestamp", F.current_timestamp()) \
                    .withColumn("ingest_date", F.to_date("ingest_timestamp"))


    # Partitioning by year, month, day
    df_new.write \
        .mode("append") \
        .partitionBy("ingest_date") \
        .parquet(silver_path)



def transform_median_income():
    # now doing the spark transformations for the zip to income file
    median_income_path = f"gs://{BUCKET_NAME}/bronze/median_income/median_income.parquet"
    df_median_income = spark.read \
        .format("parquet") \
        .load(median_income_path)

    mapping = {"NAME": "name", "S1901_C01_012E": "median_income", "zip code tabulation area": "zip_code"}
    for old, new in mapping.items():
        df_median_income = df_median_income.withColumnRenamed(old, new)

    # cleaning the median_income column for type_conversion to integer

    df_income_clean = df_median_income.withColumn(
        "median_income",
        F.when(F.col("median_income").contains("-666666"), 0) # Handle Census "No Data" code
        .otherwise(F.col("median_income").cast("int"))
    )

    # enforcing the schema
    median_income_schema = types.StructType([
        types.StructField('name', types.StringType(), True), 
        types.StructField('median_income', types.IntegerType(), True), 
        types.StructField('zip_code', types.StringType(), True)])

    # Convert your messy JSON to a clean Parquet for future use
    df_income_clean.write \
        .mode("overwrite") \
        .parquet(f"gs://{BUCKET_NAME}/silver/median_income/")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('--task', choices=['complaints', 'median_income'], required=True)
    parser.add_argument('--bucket', required=True)
    args = parser.parse_args()

    BUCKET_NAME = args.bucket

    if args.task == 'complaints':
        transform_complaints()
    else:
        transform_median_income()

    spark.stop()