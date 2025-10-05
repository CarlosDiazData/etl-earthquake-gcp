from __future__ import annotations
import datetime
from airflow.models.dag import DAG
from airflow.models.variable import Variable
from airflow.providers.http.operators.http import HttpOperator
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator

# --- 1. DAG Configuration ---
# GCP Environment configuration retrieved from Airflow Variables
GCP_PROJECT_ID = Variable.get("gcs_project_id")
GCS_BUCKET = Variable.get("gcs_bucket")
GCP_REGION = Variable.get("gcs_region")  
#CLOUD_FUNCTION_NAME = Variable.get("gcs_cloud_function")


# Paths to PySpark scripts stored in GCS
BRONZE_TO_SILVER_SCRIPT = f"gs://{GCS_BUCKET}/scripts/process_bronze_to_silver.py"
SILVER_TO_GOLD_SCRIPT = f"gs://{GCS_BUCKET}/scripts/process_silver_to_gold.py"
TRAIN_MODEL_SCRIPT = f"gs://{GCS_BUCKET}/scripts/train_tsunami_model.py"


# --- 2. Dataproc Batch Operator Configurations ---

# Configuration for the Bronze-to-Silver processing job
BRONZE_TO_SILVER_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": BRONZE_TO_SILVER_SCRIPT,
    },
    "runtime_config": {
        "version": "2.3",  
        "properties": {
            "spark.executor.cores": "4",
            "spark.executor.memory": "4g",
            "spark.driver.cores": "4",
            "spark.driver.memory": "4g",
            "spark.dataproc.driverEnv.GCS_BUCKET_NAME": GCS_BUCKET,
        }
    },
}

# Configuration for the Silver-to-Gold processing job
SILVER_TO_GOLD_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": SILVER_TO_GOLD_SCRIPT,
    },
    "runtime_config": {
        "version": "2.3",  
        "properties": {
            "spark.executor.cores": "4",
            "spark.executor.memory": "4g",
            "spark.driver.cores": "4",
            "spark.driver.memory": "4g",
            "spark.dataproc.driverEnv.GCS_BUCKET_NAME": GCS_BUCKET,
            "spark.dataproc.driverEnv.GCS_PROJECT_ID_NAME": GCP_PROJECT_ID,
            
        }
    },
}

# Configuration for the Silver-to-Gold processing job
TRAIN_MODEL_BATCH_CONFIG = {
    "pyspark_batch": {
        "main_python_file_uri": TRAIN_MODEL_SCRIPT,
    },
    "runtime_config": {
        "version": "2.3",  # Especifica la versión del runtime
        "properties": {
            "spark.executor.cores": "4",
            "spark.executor.memory": "4g",
            "spark.driver.cores": "4",
            "spark.driver.memory": "4g",
            "spark.dataproc.driverEnv.GCS_BUCKET_NAME": GCS_BUCKET,
            "spark.dataproc.driverEnv.GCS_PROJECT_ID_NAME": GCP_PROJECT_ID,
        }
    },
}

# --- 3. Airflow DAG Definition ---

with DAG(
    dag_id="earthquake_etl_dag", 
    start_date=datetime.datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["earthquake", "dataproc", "cloud-function", "gcp"],
    description="Pipeline completo que ingiere, transforma datos de sismos y entrena un modelo de ML."
) as dag:
    
    # Task 1: Ingest - Trigger the Cloud Function to fetch raw data.
    # Assumes an Airflow HTTP connection with ID 'cloud_run_conn' is configured,
    # pointing to the base URL of your Cloud Function trigger.
    ingest_to_bronze_layer = HttpOperator(
        task_id="ingest_to_bronze_layer",
        http_conn_id="cloud_run_conn",
        endpoint="/",
        method="POST",
        data={},
    )
    
    # Task 2: Process data from Bronze to Silver layer.
    process_bronze_to_silver = DataprocCreateBatchOperator(
        task_id="process_bronze_to_silver",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch=BRONZE_TO_SILVER_BATCH_CONFIG,
        retries=5,
    )

    # Task 3: Process data from Silver to Gold layer.
    process_silver_to_gold = DataprocCreateBatchOperator(
        task_id="process_silver_to_gold",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch=SILVER_TO_GOLD_BATCH_CONFIG,
        retries=5,

    )

    # Task 4: Train the ML model using data from the Silver layer.
    train_tsunami_prediction_model = DataprocCreateBatchOperator(
        task_id="train_tsunami_prediction_model",
        project_id=GCP_PROJECT_ID,
        region=GCP_REGION,
        batch=TRAIN_MODEL_BATCH_CONFIG,
        retries=5,

    )

    # --- 4. Task Dependencies ---
    # Defines the execution order of the pipeline tasks.
    ingest_to_bronze_layer >> process_bronze_to_silver >> process_silver_to_gold >> train_tsunami_prediction_model