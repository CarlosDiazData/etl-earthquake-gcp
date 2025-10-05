"""
ML Model Training Job for Tsunami Prediction.

Responsibilities:
- Loads cleaned data from the Silver layer.
- Prepares the data for machine learning, including feature selection and handling class imbalance.
- Defines and builds an ML pipeline using PySpark MLlib (VectorAssembler, StandardScaler, RandomForestClassifier).
- Splits the data into training and testing sets.
- Trains the classification model.
- Evaluates the model's performance using metrics like AUC ROC and F1-Score.
- Saves the trained model artifact to GCS for future inference.
- Generates predictions on the full dataset and writes them to a BigQuery table for analysis.
"""

import logging
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator, MulticlassClassificationEvaluator
from pyspark.ml.functions import vector_to_array



# --- 1. Script Configuration ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Environment variables for GCP configuration
GCS_BUCKET = os.getenv("GCS_BUCKET_NAME")
GCP_PROJECT_ID = os.getenv("GCS_PROJECT_ID_NAME")

# I/O Paths
SILVER_PATH = f"gs://{GCS_BUCKET}/silver/earthquakes_cleaned/"
MODEL_SAVE_PATH = f"gs://{GCS_BUCKET}/ml_models/tsunami_prediction_rf_model"
GOLD_BIGQUERY_DATASET = "gold_earthquakes"
PREDICTIONS_TABLE = "tsunami_predictions"


def main(spark: SparkSession):
    """Main function to train the model and save predictions."""
    logger.info("--- Starting Tsunami Prediction Model Training Job ---")

    # --- 2. Load and Prepare Data ---
    logger.info(f"Reading data from Silver layer: {SILVER_PATH}")
    df_silver = spark.read.format("delta").load(SILVER_PATH)
    
    # Filter for 'earthquake' events, select relevant features, and drop nulls
    feature_cols = ["magnitude", "depth_km", "latitude", "longitude", "significance"]
    label_col = "tsunami_warning"
    df_ml_source = df_silver.filter(F.col("event_type") == 'earthquake') \
                            .select(feature_cols + [label_col, "event_id"]) \
                            .na.drop()

    # --- 3. Handle Class Imbalance using Downsampling ---
    logger.info("Manejando desbalance de clases...")
    df_majority = df_ml_source.filter(F.col(label_col) == False)
    df_minority = df_ml_source.filter(F.col(label_col) == True)

    # Calculate ratio and downsample the majority class
    ratio = df_minority.count() / df_majority.count()
    df_majority_downsampled = df_majority.sample(withReplacement=False, fraction=ratio, seed=42)
   
    df_balanced = df_majority_downsampled.unionAll(df_minority)
    logger.info(f"Balanced dataset created with {df_balanced.count()} records.")

    # Convert boolean label to numeric (0/1) for the classifier
    df_balanced = df_balanced.withColumn(label_col, F.when(F.col(label_col) == True, 1).otherwise(0))


    # --- 4. Define and Train ML Pipeline ---
    assembler = VectorAssembler(inputCols=feature_cols, outputCol="unscaled_features")
    scaler = StandardScaler(inputCol="unscaled_features", outputCol="features")
    rf_classifier = RandomForestClassifier(featuresCol="features", labelCol=label_col, seed=42)
    pipeline = Pipeline(stages=[assembler, scaler, rf_classifier])

    # Split data and train the model
    (train_data, test_data) = df_balanced.randomSplit([0.8, 0.2], seed=42)
    logger.info("Training RandomForest model...")
    model = pipeline.fit(train_data)
    logger.info("Training completed.")

    # --- 5. Evaluate the Model ---
    predictions = model.transform(test_data)

    # AUC ROC evaluator
    evaluator = BinaryClassificationEvaluator(labelCol=label_col, rawPredictionCol="rawPrediction")
    auc_roc = evaluator.setMetricName("areaUnderROC").evaluate(predictions)

    # F1-Score evaluator
    f1_eval = MulticlassClassificationEvaluator(labelCol=label_col, metricName="f1")
    f1 = f1_eval.evaluate(predictions)

    logger.info(f"Evaluation Metrics: Area Under ROC = {auc_roc:.4f}, F1-Score = {f1:.4f}")

    # --- 6. Save the Model Artifact ---
    logger.info(f"Saving trained model to: {MODEL_SAVE_PATH}")
    model.write().overwrite().save(MODEL_SAVE_PATH)

    # --- 7. Generate and Save Predictions to BigQuery ---
    logger.info("Generating predictions on the full dataset...")
    full_predictions = model.transform(df_ml_source)

    # Select final columns and write to BigQuery
    df_predictions_final = full_predictions.select(
        F.col("event_id"),
        F.col(label_col).alias("actual_tsunami_warning"),
        F.col("prediction").cast("boolean").alias("predicted_tsunami_warning"),
        vector_to_array(F.col("probability")).getItem(1).alias("tsunami_probability"),
        F.current_timestamp().alias("prediction_timestamp_utc")
    )
    
    logger.info(f"Writing {df_predictions_final.count()} predictions to BigQuery...")
    spark.conf.set('temporaryGcsBucket', GCS_BUCKET)
    df_predictions_final.write.format('bigquery') \
        .option('table', f"{GCP_PROJECT_ID}.{GOLD_BIGQUERY_DATASET}.{PREDICTIONS_TABLE}") \
        .mode('overwrite') \
        .save()
        
    logger.info("--- ML Training Job COMPLETED ---")

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("TsunamiPredictionTraining") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    
    main(spark)
    
    spark.stop()