"""
Bronze to Silver ETL Job for the Earthquake Data Pipeline.

Responsibilities:
- Reads raw GeoJSON data from the Bronze layer in GCS.
- Flattens the nested JSON structure into a tabular format.
- Performs data cleaning, type casting, and range validation.
- Deduplicates records to ensure each event is unique, keeping the most recent update.
- Enriches the data with new features (e.g., categories, date components).
- Writes the cleaned and enriched DataFrame as a Delta table to the Silver layer in GCS.
"""

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, DoubleType, BooleanType

# --- 1. Script Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables for GCS bucket name
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")

# Data layer paths
BRONZE_PATH = f"gs://{GCS_BUCKET}/bronze/"
SILVER_PATH = f"gs://{GCS_BUCKET}/silver/earthquakes_cleaned/"

def main(spark: SparkSession):
    """Main function to execute the Bronze to Silver ETL job."""
    logger.info(f"Reading raw JSON data from: {BRONZE_PATH}")

    # --- 2. Lectura y Aplanado de la Capa Bronze ---
    logger.info(f"Reading raw JSON data from: {BRONZE_PATH}")
    try:
        df_bronze_raw = spark.read.json(BRONZE_PATH)

        # Si no hay datos, termina de forma controlada.
        if df_bronze_raw.rdd.isEmpty():
            logger.warning("Bronze layer is empty. No data to process. Job finished.")
            return

        # Extraer y aplanar la estructura anidada de GeoJSON
        df_features = df_bronze_raw.select(F.explode("features").alias("feature"))

        df_bronze = df_features.select(
            F.col("feature.id").alias("id"),
            F.col("feature.properties.mag").alias("mag"),
            F.col("feature.properties.place").alias("place"),
            F.col("feature.properties.time").alias("time"),
            F.col("feature.properties.updated").alias("updated"),
            F.col("feature.properties.url").alias("url"),
            F.col("feature.properties.felt").alias("felt"),
            F.col("feature.properties.cdi").alias("cdi"),
            F.col("feature.properties.mmi").alias("mmi"),
            F.col("feature.properties.alert").alias("alert"),
            F.col("feature.properties.status").alias("status"),
            F.col("feature.properties.tsunami").alias("tsunami"),
            F.col("feature.properties.sig").alias("sig"),
            F.col("feature.properties.net").alias("net"),
            F.col("feature.properties.code").alias("code"),
            F.col("feature.properties.nst").alias("nst"),
            F.col("feature.properties.dmin").alias("dmin"),
            F.col("feature.properties.rms").alias("rms"),
            F.col("feature.properties.gap").alias("gap"),
            F.col("feature.properties.magType").alias("magType"),
            F.col("feature.properties.type").alias("type"),
            F.col("feature.properties.title").alias("title"),
            F.col("feature.geometry.coordinates").getItem(0).alias("longitude"),
            F.col("feature.geometry.coordinates").getItem(1).alias("latitude"),
            F.col("feature.geometry.coordinates").getItem(2).alias("depth")
        )
        logger.info(f"Successfully read and flattened {df_bronze.count()} records.")
    except Exception as e:
        logger.error(f"Failed to read or flatten the Bronze layer. Error: {e}", exc_info=True)
        raise

    # --- 3. Transform to Silver Layer ---
    logger.info("Aplicando limpieza, tipado, validación y deduplicación.")
    
    # Type casting and initial renaming
    df_cleaned = df_bronze.withColumn("event_timestamp_utc", (F.col("time") / 1000).cast(TimestampType())) \
        .withColumn("updated_timestamp_utc", (F.col("updated") / 1000).cast(TimestampType())) \
        .withColumn("magnitude", F.col("mag").cast(DoubleType())) \
        .withColumn("depth_km", F.col("depth").cast(DoubleType())) \
        .withColumn("tsunami_warning", (F.col("tsunami") == 1).cast(BooleanType())) \
        .withColumn("significance", F.col("sig").cast(IntegerType())) \
        .withColumn("felt_reports", F.col("felt").cast(IntegerType())) \
        .withColumn("nst_stations", F.col("nst").cast(IntegerType())) \
        .withColumn("rms_travel_time", F.col("rms").cast(DoubleType())) \
        .withColumn("gap_azimuthal", F.col("gap").cast(DoubleType()))

    # Select and rename final columns for the Silver layer schema
    df_selected = df_cleaned.select(
        F.col("id").alias("event_id"), "event_timestamp_utc", "updated_timestamp_utc", "magnitude", "depth_km",
        "latitude", "longitude", "place", F.col("type").alias("event_type"), "magType", "tsunami_warning",
        "significance", "felt_reports", "nst_stations", "rms_travel_time", "gap_azimuthal", "alert", "status", "url", "title"
    )

    # Data validation to ensure quality
    df_validated = df_selected.filter(
        (F.col("magnitude").isNotNull()) & (F.col("magnitude").between(-2.0, 10.0)) &
        (F.col("latitude").isNotNull()) & (F.col("latitude").between(-90.0, 90.0)) &
        (F.col("longitude").isNotNull()) & (F.col("longitude").between(-180.0, 180.0)) &
        (F.col("depth_km").isNotNull()) & (F.col("depth_km") >= 0) & (F.col("depth_km") < 1000) &
        (F.col("event_timestamp_utc").isNotNull()) & (F.col("event_id").isNotNull())
    )

    # Deduplication: keep the most recently updated record for each event ID
    window_spec = Window.partitionBy("event_id").orderBy(F.col("updated_timestamp_utc").desc())
    df_deduplicated = df_validated.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")

    # --- 4. Feature Engineering ---
    logger.info("Enriqueciendo datos con nuevas características.")
    
    df_enriched = df_deduplicated \
        .withColumn("magnitude_category",
            F.when(F.col("magnitude") < 3.0, "Micro")
             .when(F.col("magnitude") < 4.0, "Minor")
             .when(F.col("magnitude") < 5.0, "Light")
             .when(F.col("magnitude") < 6.0, "Moderate")
             .when(F.col("magnitude") < 7.0, "Strong")
             .when(F.col("magnitude") < 8.0, "Major")
             .otherwise("Great")) \
        .withColumn("depth_category",
            F.when(F.col("depth_km") <= 70, "Shallow")
             .when(F.col("depth_km") <= 300, "Intermediate")
             .otherwise("Deep")) \
        .withColumn("hemisphere_ns", F.when(F.col("latitude") >= 0, "Northern").otherwise("Southern")) \
        .withColumn("hemisphere_ew", F.when(F.col("longitude") >= 0, "Eastern").otherwise("Western")) \
        .withColumn("year", F.year(F.col("event_timestamp_utc"))) \
        .withColumn("month", F.month(F.col("event_timestamp_utc"))) \
        .withColumn("day", F.dayofmonth(F.col("event_timestamp_utc"))) \
        .withColumn("hour", F.hour(F.col("event_timestamp_utc"))) \
        .withColumn("day_of_week", F.dayofweek(F.col("event_timestamp_utc"))) \
        .withColumn("extracted_region_detail", F.trim(F.regexp_extract(F.col("place"), r",\s*(.*)$", 1))) \
        .withColumn("extracted_country",
            F.when(F.col("extracted_region_detail") != "", F.col("extracted_region_detail"))
             .otherwise(F.trim(F.col("place")))) \
        .withColumn("silver_processing_timestamp_utc", F.current_timestamp())

    # --- 5. Write to Silver Layer ---
    logger.info(f"Escribiendo {df_enriched.count()} registros en la capa Silver: {SILVER_PATH}")
    df_enriched.write.format("delta").mode("overwrite").partitionBy("year", "month").save(SILVER_PATH)
    logger.info("--- Tarea: Bronze a Silver COMPLETADA ---")

if __name__ == "__main__":
    # Initialize SparkSession with Delta Lake support
    spark = SparkSession.builder \
        .appName("BronzeToSilverProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    main(spark)
    
    spark.stop()