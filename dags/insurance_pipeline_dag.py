"""
insurance_pipeline_dag.py

Airflow DAG orchestrating the full Insurance Data Pipeline.

Runs three tasks in sequence:
1. ingest_raw_data: Download CSV to MinIO raw bucket
2. transform_claims: PySpark transformation to Parquet
3. load_to_warehouse: Load Parquet to DuckDB warehouse

AWS equivalent: AWS Step Functions state machine triggering Glue jobs in sequence with error handling and retry logic.

Schedule: Daily at 6:00 AM UTC
"""

from datetime import datetime, timedelta  # noqa: F401
from airflow import DAG
from airflow.operators.python import PythonOperator  # noqa: F401
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments applied to all tasks
# These mirror the retry/timeout config in Step Functions
default_args = {
    "owner": "gokul_prasath",
    "depends_on_past": False,
    "email": ["gokulprasath560@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
}

# DAG definition
with DAG(
    dag_id="insurance_data_pipeline",
    default_args=default_args,
    description="End-to-end insurance data pipeline: ingest -> transform -> load",
    schedule_interval="0 6 * * *",  # Daily at 6AM UTC (cron format)
    start_date=days_ago(1),
    catchup=False,  # Don't backfill missed runs
    tags=["insurance", "etl", "pyspark", "minio", "duckdb"],
    doc_md="""
## Insurance Data Pipeline

End-to-end cloud data pipeline processing US insurance data.

### Pipeline Flow

Raw Data -> MinIO (S3) -> PySpark Transform -> Parquet -> DuckDB (Redshift)

### Tasks
1. **ingest_raw_data**: Downloads public insurance CSV to MinIO raw bucket
2. **transform_claims**: PySpark job cleans and enriches data to Parquet
3. **load_to_warehouse**: Loads Parquet into DuckDB star schema

### AWS Equivalent
- Airflow DAG -> AWS Step Functions state machine
- PythonOperator -> AWS Lambda function trigger
- BashOperator -> AWS Glue job trigger
- MinIO -> Amazon S3
- DuckDB -> Amazon Redshift / Snowflake
""",
) as dag:

    # Task 1: Ingest
    # Downloads raw insurance CSV and lands in MinIO
    # AWS equivalent: Lambda trigger Glue ingestion job
    ingest_task = BashOperator(
        task_id="ingest_raw_data",
        bash_command="python3 /opt/airflow/jobs/ingestion/ingest_raw_data.py",
        doc_md="""
### Ingest Raw Data
Downloads public US insurance dataset and uploads to MinIO raw bucket with UTC timestamp filename.

**Output:** s3://insurance-raw/insurance_YYYYMMDD_HHMMSS.csv
""",
    )

    # Task 2: Transform
    # Runs PySpark transformation inside Spark container
    # AWS equivalent: AWS Glue PySpark job
    transform_task = BashOperator(
        task_id="transform_claims",
        bash_command="docker exec insurance_spark python3 /opt/spark/app/jobs/transformation/transform_claims.py",
        # """
        # docker run --rm \
        #   --network infra_pipeline_network \
        #   -e MINIO_ENDPOINT=http://minio:9000 \
        #   -e MINIO_ROOT_USER=minioadmin \
        #   -e MINIO_ROOT_PASSWORD=minioadmin123 \
        #   -e MINIO_BUCKET_RAW=insurance-raw \
        #   -e MINIO_BUCKET_PROCESSED=insurance-processed \
        #   -v /opt/airflow/jobs:/opt/spark/app/jobs \
        #   -v /opt/airflow/data:/opt/spark/app/data \
        #   insurance_spark:latest \
        #   python3 /opt/spark/app/jobs/transformation/transform_claims.py
        # """,
        doc_md="""
### Transform Claims
PySpark job reading raw CSV from MinIO, applying business transformations and writing enriched Parquet to processed bucket.

**Input:** s3://insurance-raw/insurance_*.csv
**Output:** s3://insurance-processed/transformed_*/
""",
    )

    # Task 3: Load
    # Loads transformed Parquet into DuckDB warehouse
    # AWS equivalent: Glue job loading into Redshift
    load_task = BashOperator(
        task_id="load_to_warehouse",
        bash_command="python3 /opt/airflow/jobs/loading/load_to_warehouse.py",
        doc_md="""
### Load to Warehouse
Reads latest Parquet from MinIO processed bucket and loads into DuckDB star schema (dim_customers, dim_regions, fact_claims).

**Input:** s3://insurance-processed/transformed_*/
**Output:** DuckDB tables: dim_customers, dim_regions, fact_claims
""",
    )

    # Pipeline dependency chain
    # Defines execution order: ingest -> transform -> load
    # >> operator sets downstream dependency in Airflow
    # AWS equivalent: Step Functions "Next" state config
    ingest_task >> transform_task >> load_task
