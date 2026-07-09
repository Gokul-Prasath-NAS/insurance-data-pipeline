"""
load_to_warehouse.py

Loading layer of the Insurance Data Pipeline.
Reads transformed Parquet files from MinIO processed bucket and loads them into DuckDB analytical warehouse tables.
DuckDB is our local Amazon Redshift / Snowflake equivalent.
The same loading patterns (COPY, INSERT, MERGE) apply directly to Redshift and Snowflake in production.

AWS equivalent: AWS Glue job loading from S3 processed prefix into Amazon Redshift using COPY command or Snowflake connector.

Warehouse schema:
- dim_customers: policyholder demographic dimensions
- dim_regions: regional reference data
- fact_claims: insurance claims fact table
"""

import os
import sys  # noqa: F401
import logging
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import boto3
import pandas as pd
import tempfile
from botocore.client import Config
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("load_to_warehouse")


def get_minio_client():
    """Creates boto3 S3 client pointed at MinIO."""
    return boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def get_warehouse_connection(db_path: str) -> duckdb.DuckDBPyConnection:
    """
    Creates a DuckDB connection - local Redshift equivalent.
    DuckDB is an embedded analytical database - no server needed.
    It runs inside your Python process, reads Parquet natively, and supports full SQL including window functions and CTEs.

    In production this connection string would point to:
    Amazon Redshift: redshift+psycopg2://user:pass@host:5439/db
    Snowflake: snowflake://user:pass@account/db/schema
    """
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = duckdb.connect(db_path)
    logger.info(f"Connected to warehouse: {db_path}")
    return conn


def create_warehouse_schema(conn: duckdb.DuckDBPyConnection):
    """
    Creates the dimensional warehouse schema.
    We use a star schema - industry standard for insurance analytics:
    - Dimension tables (dim_*) contain descriptive attributes
    - Fact table (fact_claims) contains measurable events/metrics

    This mirrors the Redshift schema - same DDL concepts, different syntax.
    """
    logger.info("Creating warehouse schema...")

    # Dimension: Customers
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_customers (
        customer_id INTEGER PRIMARY KEY,
        age INTEGER NOT NULL,
        age_group VARCHAR(20) NOT NULL,
        sex VARCHAR(10) NOT NULL,
        bmi DOUBLE NOT NULL,
        bmi_category VARCHAR(20) NOT NULL,
        smoker VARCHAR(5) NOT NULL,
        children INTEGER NOT NULL,
        ingestion_ts TIMESTAMP NOT NULL
    );
    """
    )

    # Dimension: Regions
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS dim_regions (
        region_id INTEGER PRIMARY KEY,
        region_name VARCHAR(50) NOT NULL UNIQUE
    );
    """
    )

    # Fact: Claims
    conn.execute(
        """
    CREATE TABLE IF NOT EXISTS fact_claims (
        claim_id INTEGER PRIMARY KEY,
        customer_id INTEGER NOT NULL,
        region_id INTEGER NOT NULL,
        charges DOUBLE NOT NULL,
        charges_bucket VARCHAR(20) NOT NULL,
        risk_score DOUBLE NOT NULL,
        pipeline_version VARCHAR(10) NOT NULL,
        loaded_at TIMESTAMP NOT NULL,
        FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
        FOREIGN KEY (region_id) REFERENCES dim_regions(region_id)
    );
    """
    )
    logger.info("Warehouse schema created successfully")


def download_latest_parquet(client, bucket: str) -> pd.DataFrame:
    """
    Downloads the latest transformed Parquet folder from MinIO and reads it into a pandas DataFrame.

    In production Redshift COPY command reads directly from S3:
    COPY table FROM 's3://bucket/prefix' IAM_ROLE... FORMAT PARQUET
    We simulate this pattern by downloading then loading locally.
    """
    logger.info(f"Listing objects in bucket: {bucket}")
    response = client.list_objects_v2(Bucket=bucket)

    if "Contents" not in response:
        raise FileNotFoundError(f"No files found in bucket: {bucket}")

    # Find all parquet files (exclude SUCCESS marker files)
    parquet_files = [obj for obj in response["Contents"] if obj["Key"].endswith(".parquet")]

    if not parquet_files:
        raise FileNotFoundError("No Parquet files found in processed bucket")

    # Get the latest parquet file by last modified date
    latest = max(parquet_files, key=lambda x: x["LastModified"])
    latest_key = latest["Key"]
    logger.info(f"Reading: s3://{bucket}/{latest_key}")

    # Download to local temp file
    # tempfile.gettempdir() returns correct path on any OS (Windows, Linux, Mac)
    local_path = os.path.join(tempfile.gettempdir(), "latest_transformed.parquet")
    ## Ensure local directory exists for fallback paths
    # Path(local_path).parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, latest_key, local_path)

    # Read into pandas DataFrame
    df = pd.read_parquet(local_path)
    logger.info(f"Loaded {len(df)} records from Parquet")
    return df


def load_dim_customers(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    """
    Loads policyholder demographic data into dim_customers.
    Uses INSERT OR REPLACE for idempotent loading - safe to run multiple times without duplicate records.
    This mirrors MERGE/UPSERT patterns in Redshift and Snowflake.
    """
    logger.info("Loading dim_customers...")

    # Clear existing data for clean reload (truncate equivalent)
    conn.execute("DELETE FROM dim_customers")

    # Build dimension dataframe
    columns_to_extract = [
        "age",
        "sex",
        "age_group",
        "bmi",
        "bmi_category",
        "smoker",
        "children",
        "ingestion_timestamp",
    ]
    dim_df = df[columns_to_extract].drop_duplicates().reset_index(drop=True)

    dim_df.insert(0, "customer_id", range(1, len(dim_df) + 1))
    dim_df["ingestion_ts"] = pd.to_datetime(dim_df["ingestion_timestamp"])
    dim_df = dim_df.drop(columns=["ingestion_timestamp"])

    conn.execute("INSERT INTO dim_customers SELECT * FROM dim_df")

    count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
    logger.info(f"dim_customers loaded: {count} records")
    return dim_df


def load_dim_regions(conn: duckdb.DuckDBPyConnection, df: pd.DataFrame):
    """Loads unique regions into dim_regions reference table."""
    logger.info("Loading dim_regions...")

    conn.execute("DELETE FROM dim_regions")

    regions = df["region"].unique()
    region_df = pd.DataFrame({"region_id": range(1, len(regions) + 1), "region_name": regions})

    conn.execute("INSERT INTO dim_regions SELECT * FROM region_df")

    count = conn.execute("SELECT COUNT(*) FROM dim_regions").fetchone()[0]
    logger.info(f"dim_regions loaded: {count} records")
    return region_df


def load_fact_claims(
    conn: duckdb.DuckDBPyConnection,
    df: pd.DataFrame,
    dim_customers: pd.DataFrame,
    dim_regions: pd.DataFrame,
):
    """
    Loads insurance claims into the central fact table.
    Joins dimension tables to resolve foreign keys - same pattern as Glue jobs joining staging tables before loading into Redshift.
    """
    logger.info("Loading fact_claims...")
    conn.execute("DELETE FROM fact_claims")

    # Resolve customer foreign keys
    fact_df = df.merge(
        dim_customers[["customer_id", "age", "sex", "bmi", "smoker", "children"]],
        on=["age", "sex", "bmi", "smoker", "children"],
        how="left",
    )

    # Resolve region foreign keys
    fact_df = fact_df.merge(dim_regions, left_on="region", right_on="region_name", how="left")

    fact_df = fact_df[
        ["charges", "charges_bucket", "risk_score", "pipeline_version", "customer_id", "region_id"]
    ].reset_index(drop=True)

    fact_df.insert(0, "claim_id", range(1, len(fact_df) + 1))
    fact_df["loaded_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

    conn.execute(
        """
    INSERT INTO fact_claims
    SELECT 
        claim_id,
        customer_id,
        region_id,
        charges,
        charges_bucket,
        risk_score,
        pipeline_version,
        loaded_at::TIMESTAMP
    FROM fact_df
    """
    )

    count = conn.execute("SELECT COUNT(*) FROM fact_claims").fetchone()[0]
    logger.info(f"fact_claims loaded: {count} records")


def run_validation_queries(conn: duckdb.DuckDBPyConnection):
    """
    Runs post-load validation queries to confirm data integrity.
    These are the same reconciliation checks similar to Datadog monitoring - confirming record counts, checking for nulls, validating referential integrity.
    """
    logger.info("Running post-load validation...")

    # Row counts per table
    tables = ["dim_customers", "dim_regions", "fact_claims"]
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
        logger.info(f"{table}: {count} records")

    # Referential integrity check
    orphans = conn.execute(
        """
    SELECT COUNT(*) 
    FROM fact_claims f 
    LEFT JOIN dim_customers c ON f.customer_id = c.customer_id 
    WHERE c.customer_id IS NULL
    """
    ).fetchone()[0]
    logger.info(f"Orphaned fact records (should be 0): {orphans}")

    # Sample analytical query - avg charges by region
    logger.info("Sample analytics - avg charges by region:")
    results = conn.execute(
        """
    SELECT 
        r.region_name,
        ROUND(AVG(f.charges), 2) AS avg_charges,
        COUNT(*) AS policy_count 
    FROM fact_claims f
    JOIN dim_regions r ON f.region_id = r.region_id 
    GROUP BY r.region_name
    ORDER BY avg_charges DESC
    """
    ).fetchall()

    for row in results:
        logger.info(f"[{row[0]}]: ${row[1]} avg ({row[2]} policies)")


def main():
    db_path = os.getenv("DUCKDB_DATABASE_PATH", "./data/warehouse/insurance.duckdb")
    bucket_processed = os.getenv("MINIO_BUCKET_PROCESSED", "insurance-processed")

    logger.info("Starting warehouse loading job")

    try:
        # Step 1: Connect to warehouse
        conn = get_warehouse_connection(db_path)

        # Step 2: Create schema
        create_warehouse_schema(conn)

        # Step 3: Download latest Parquet from MinIO
        client = get_minio_client()
        df = download_latest_parquet(client, bucket_processed)

        # Step 4: Load dimensions first (foreign key order)
        dim_customers = load_dim_customers(conn, df)
        dim_regions = load_dim_regions(conn, df)

        # Step 5: Load fact table
        load_fact_claims(conn, df, dim_customers, dim_regions)

        # Step 6: Validate
        run_validation_queries(conn)

        conn.close()
        logger.info("Warehouse loading job completed successfully")

    except Exception as e:
        logger.error(f"Loading job failed: {e}")
        raise


if __name__ == "__main__":
    main()
