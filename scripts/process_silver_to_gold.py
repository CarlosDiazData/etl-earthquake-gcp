"""
Silver to Gold ETL Job for the Earthquake Data Pipeline.

Responsibilities:
- Reads cleaned and enriched data from the Silver Delta table.
- Creates dimensional tables (DimDate, DimLocation, DimMagnitude, DimEventType)
  to build a star schema.
- Creates a fact table (FactEarthquakeEvents) containing measures and foreign
  keys to the dimension tables.
- Writes the final dimension and fact tables to a BigQuery dataset, making them
  available for analytics and business intelligence.
"""

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date

# --- 1. Script Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables for GCP configuration
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCS_PROJECT_ID_NAME") 

# Data layer paths
SILVER_PATH = f"gs://{GCS_BUCKET}/silver/earthquakes_cleaned/"
GOLD_BIGQUERY_DATASET = "gold_earthquakes"

def main(spark: SparkSession):
    """Main function to execute the Silver to Gold ETL job."""
    logger.info("--- Starting Silver to Gold Job (Dimensional Model) ---")

    # --- 2. Read Silver Layer Data ---
    logger.info(f"Reading Delta table from Silver layer: {SILVER_PATH}")
    try:
        df_silver = spark.read.format("delta").load(SILVER_PATH)
        if df_silver.rdd.isEmpty():
            logger.warning("Silver layer is empty. No data to process. Job finished.")
            return
        logger.info(f"Successfully read {df_silver.count()} records from the Silver layer.")
    except Exception as e:
        logger.error(f"Failed to read from Silver layer. Error: {e}", exc_info=True)
        raise

    # --- 3. Create Dimension Tables ---

    # DimDate: A comprehensive date dimension
    logger.info("Generando DimDate...")
    min_max_date = df_silver.select(
        F.min("event_timestamp_utc").alias("min_date"),
        F.max("event_timestamp_utc").alias("max_date")
    ).first()
    
    start_date = min_max_date["min_date"].date()
    end_date = min_max_date["max_date"].date() + timedelta(days=30) # Buffer de 30 días
    
    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append({
            'DateKey': int(current_date.strftime('%Y%m%d')),
            'FullDate': current_date,
            'Year': current_date.year,
            'Quarter': (current_date.month - 1) // 3 + 1,
            'Month': current_date.month,
            'MonthName': current_date.strftime('%B'),
            'DayOfMonth': current_date.day,
            'DayOfWeek': current_date.isoweekday() % 7 + 1,
            'DayName': current_date.strftime('%A'),
            'IsWeekend': 1 if current_date.weekday() >= 5 else 0,
        })
        current_date += timedelta(days=1)
        
    df_dim_date = spark.createDataFrame(date_list)

    # DimLocation: A dimension for geographical attributes
    logger.info("Creando DimLocation...")
    df_dim_location = df_silver.select(
        "latitude", "longitude", "place", "extracted_country", 
        "extracted_region_detail", "hemisphere_ns", "hemisphere_ew"
    ).distinct() \
    .withColumn("LocationKey", F.monotonically_increasing_id())

    # DimMagnitude: A static dimension for magnitude categories
    logger.info("Creando DimMagnitude...")
    magnitude_data = [
        {"MagnitudeCategory": "Micro", "MinMagnitude": -2.0, "MaxMagnitude": 2.9, "Description": "No sentido o raramente sentido."},
        {"MagnitudeCategory": "Minor", "MinMagnitude": 3.0, "MaxMagnitude": 3.9, "Description": "A menudo sentido, raramente causa daños."},
        {"MagnitudeCategory": "Light", "MinMagnitude": 4.0, "MaxMagnitude": 4.9, "Description": "Sentido por muchos, posibles daños leves."},
        {"MagnitudeCategory": "Moderate", "MinMagnitude": 5.0, "MaxMagnitude": 5.9, "Description": "Daños en estructuras débiles."},
        {"MagnitudeCategory": "Strong", "MinMagnitude": 6.0, "MaxMagnitude": 6.9, "Description": "Daños moderados en estructuras bien construidas."},
        {"MagnitudeCategory": "Major", "MinMagnitude": 7.0, "MaxMagnitude": 7.9, "Description": "Daños graves en la mayoría de los edificios."},
        {"MagnitudeCategory": "Great", "MinMagnitude": 8.0, "MaxMagnitude": 10.0, "Description": "Destrucción generalizada."},
        {"MagnitudeCategory": "Unknown", "MinMagnitude": None, "MaxMagnitude": None, "Description": "Categoría no determinada."}
    ]
    df_dim_magnitude = spark.createDataFrame(magnitude_data) \
        .withColumn("MagnitudeKey", F.monotonically_increasing_id())

    # DimEventType: A dimension for event and magnitude types
    logger.info("Creando DimEventType...")
    df_dim_event_type = df_silver.select("event_type", "magType").distinct() \
        .withColumn("EventTypeKey", F.monotonically_increasing_id())

    # --- 4. Prepare Fact Table ---
    logger.info("Preparing FactEarthquakeEvents by joining with dimensions...")
    
    # Add DateKey for joining
    df_fact_source = df_silver.withColumn("DateKey", F.date_format(F.col("event_timestamp_utc"), "yyyyMMdd").cast("int"))

    # Join with dimensions to get surrogate keys
    df_fact_joined = df_fact_source \
        .join(df_dim_date.select("DateKey"), "DateKey", "inner") \
        .join(df_dim_location, ["latitude", "longitude", "place"], "inner") \
        .join(df_dim_magnitude, df_fact_source.magnitude_category == df_dim_magnitude.MagnitudeCategory, "inner") \
        .join(df_dim_event_type, ["event_type", "magType"], "inner")

    # Final selection of columns for the fact table
    df_fact_final = df_fact_joined.select(
        F.col("event_id").alias("EventID"),
        F.col("DateKey"),
        F.col("LocationKey"),
        F.col("MagnitudeKey"),
        F.col("EventTypeKey"),
        F.col("magnitude").alias("Magnitude"),
        F.col("depth_km").alias("DepthKm"),
        F.col("tsunami_warning").alias("TsunamiWarning"),
        F.col("significance").alias("Significance"),
        F.col("felt_reports").alias("FeltReports"),
        F.col("nst_stations").alias("NumberOfStations"),
        F.col("rms_travel_time").alias("RmsTravelTime"),
        F.col("gap_azimuthal").alias("AzimuthalGap"),
        F.col("url").alias("SourceURL"),
        F.col("silver_processing_timestamp_utc").alias("SilverProcessingTimestampUTC"),
        F.current_timestamp().alias("DWLoadTimestampUTC")
    ).dropDuplicates(["EventID"])

    # --- 5. Write Gold Layer to BigQuery ---
    # Configure the temporary GCS bucket required by the BigQuery connector
    spark.conf.set('temporaryGcsBucket', GCS_BUCKET)

    def write_to_bigquery(df, table_name):
        logger.info(f"Writing {df.count()} records to BigQuery table: {GOLD_BIGQUERY_DATASET}.{table_name}")
        df.write.format('bigquery') \
          .option('table', f"{GCP_PROJECT_ID}.{GOLD_BIGQUERY_DATASET}.{table_name}") \
          .mode('overwrite') \
          .save()
        logger.info(f"Table '{table_name}' successfully written to BigQuery.")

    try:
        write_to_bigquery(df_dim_date, "dim_date")
        write_to_bigquery(df_dim_location, "dim_location")
        write_to_bigquery(df_dim_magnitude, "dim_magnitude")
        write_to_bigquery(df_dim_event_type, "dim_event_type")
        write_to_bigquery(df_fact_final, "fact_earthquake_events")
        
        logger.info("--- Silver to Gold Job COMPLETED ---")
    except Exception as e:
        logger.error(f"Failed to write to BigQuery. Error: {e}", exc_info=True)
        raise

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("SilverToGoldProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    main(spark)
    
    spark.stop()

