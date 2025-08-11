from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit

PROJECT_ID = "coinproject-463620"
BQ_DATASET = "bikerentapp"
OUTPUT_TABLE = "ml_dataset.daily_features"

spark = SparkSession.builder \
    .appName("JoinWeatherHolidaysTrips") \
    .getOrCreate()

# Read from BigQuery
weather_df = spark.read.format("bigquery") \
    .option("project", PROJECT_ID) \
    .option("table", f"{BQ_DATASET}.weather_daily_dc") \
    .load()

holidays_df = spark.read.format("bigquery") \
    .option("project", PROJECT_ID) \
    .option("table", f"{BQ_DATASET}.holidays_and_weekends") \
    .load()

trips_df = spark.read.format("bigquery") \
    .option("project", PROJECT_ID) \
    .option("table", f"{BQ_DATASET}.daily_trip_counts") \
    .load()

holidays_df = holidays_df.withColumnRenamed("trip_date", "date")
trips_df = trips_df.withColumnRenamed("trip_date", "date")

holidays_df = holidays_df.withColumn("is_weekend", col("is_weekend").cast("integer"))
holidays_df = holidays_df.withColumn("is_holiday", col("is_holiday").cast("integer"))

joined_df = weather_df \
    .join(holidays_df, on="date", how="left") \
    .join(trips_df, on="date", how="left")

final_df = joined_df.select(
    "date",
    "temperature_max", "temperature_min", "precipitation_sum", "windspeed_max",
    "is_weekend", "is_holiday", "holiday_name",
    "daily_trip_count"
)

final_df.write \
    .format("bigquery") \
    .option("project", PROJECT_ID) \
    .option("table", OUTPUT_TABLE) \
    .option("temporaryGcsBucket", "spark_tmp_airflow_pr") \
    .mode("overwrite") \
    .save()

spark.stop()
