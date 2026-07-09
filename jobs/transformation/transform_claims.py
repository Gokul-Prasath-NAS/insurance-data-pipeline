"""
transform_claims.py

Transformation layer of the Insurance Data Pipeline.
Reads raw insurance CSV from MinIO (S3 equivalent), applies PySpark transformations 
to clean, validate and enrich the data, then writes the processed output back to 
MinIO processed bucket in Parquet format.

AWS equivalent: AWS Glue PySpark job reading from S3 raw prefix and writing to S3 processed prefix.

Dataset columns:
- age: policyholder age
- sex: policyholder gender
- bmi: body mass index
- children: number of dependents
- smoker: smoking status (yes/no)
- region: residential region in US
- charges: annual insurance premium charged
"""

import os
import sys
import logging
from datetime import datetime, timezone

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("transform_claims")


def create_spark_session() -> SparkSession:
    """
    Creates a local PySpark session with S3/MinIO connectivity.
    In AWS Glue, SparkSession is pre-created automatically.
    Locally we create it manually with the hadoop-aws package which provides the S3A connector - this is how pyspark
    talks to any S3-compatible storage (AWS S3 or MinIO).
    """
    return (
        SparkSession.builder.appName("InsuranceDataTransformation")
        .master("local[*]")  # use all available CPU cores
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        )
        .config("spark.hadoop.fs.s3a.endpoint", os.getenv("MINIO_ENDPOINT", "http://minio:9000"))
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_ROOT_USER", "minioadmin"))
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"))
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def define_schema() -> StructType:
    """
    Explicit schema definition for the insurance CSV.
    Always define schemas explicitly in production PySpark jobs -
    never rely on schema inference. Inference reads the entire
    dataset to guess types (slow and unreliable on large data).
    Explicit schemas are faster, safer and self-documenting.
    This is standard practice in AWS Glue jobs as well,
    where you define the schema in the Glue Data Catalog or directly in the job code.
    """
    return StructType(
        [
            StructField("age", IntegerType(), nullable=False),
            StructField("sex", StringType(), nullable=False),
            StructField("bmi", DoubleType(), nullable=False),
            StructField("children", IntegerType(), nullable=False),
            StructField("smoker", StringType(), nullable=False),
            StructField("region", StringType(), nullable=False),
            StructField("charges", DoubleType(), nullable=False),
        ]
    )


def read_latest_raw_file(spark: SparkSession, bucket: str) -> DataFrame:
    """Reads the most recently ingested CSV from MinIO raw bucket.

    Uses boto3 to list objects in the bucket and find the latest timestamped file,
    then reads it with PySpark. This mirrors how glue jobs uses dynamoDB timestamps
    to find the latest file to process.
    """

    import boto3
    from botocore.client import Config

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("MINIO_ENDPOINT", "http://localhost:9000"),
        aws_access_key_id=os.getenv("MINIO_ROOT_USER", "minioadmin"),
        aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD", "minioadmin123"),
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    # List all objects and find the latest one
    response = client.list_objects_v2(Bucket=bucket)
    if "Contents" not in response:
        raise FileNotFoundError(f"No files found in bucket: {bucket}")

    latest = max(response["Contents"], key=lambda x: x["LastModified"])
    latest_key = latest["Key"]
    logger.info(f"Reading latest file: s3a://{bucket}/{latest_key}")

    schema = define_schema()
    df = spark.read.csv(f"s3a://{bucket}/{latest_key}", header=True, schema=schema)
    logger.info(f"Loaded {df.count()} records")
    return df


def validate_data(df: DataFrame) -> DataFrame:
    """Basic data validation before transformation.

    Drops records that would corruopt downstream analytics:
    - Null values in critical columns
    - Invalid age (must be 0-120)
    - Invalid BMI (must be > 0)
    - Invalid charges (must be > 0)

    In Production this would be Great Expectations - we'll add
    that layer separatly. This is the first line of defence.
    """
    initial_count = df.count()

    # Drop null values in critical columns
    df = df.dropna(subset=["age", "bmi", "charges", "smoker", "region"])

    # Filter bounds
    df = df.filter(
        (F.col("age") > 0) & (F.col("age") <= 120) & (F.col("bmi") > 0) & (F.col("charges") > 0)
    )

    final_count = df.count()
    dropped = initial_count - final_count
    logger.info(f"Validation: {initial_count} in → " f"{final_count} valid →  {dropped} dropped")
    return df


def transform_data(df: DataFrame) -> DataFrame:
    """Core business transformations applied to the insurance data.

    Each transformation adds analytical value - these are the kind
    of enrichments that make raw operational data useful for BI reporting.
    """
    logger.info("Applying transformations...")

    # 1. Standardize text columns to lowercase, trim whitespace
    df = df.withColumn("sex", F.lower(F.trim(F.col("sex"))))
    df = df.withColumn("smoker", F.lower(F.trim(F.col("smoker"))))
    df = df.withColumn("region", F.lower(F.trim(F.col("region"))))

    # 2. BMI category standard medical classification
    df = df.withColumn(
        "bmi_category",
        F.when(F.col("bmi") < 18.5, "underweight")
        .when(F.col("bmi") < 25.0, "normal")
        .when(F.col("bmi") < 30.0, "overweight")
        .otherwise("obese"),
    )

    # 3. Age group segmentation
    df = df.withColumn(
        "age_group",
        F.when(F.col("age") < 25, "young adult")
        .when(F.col("age") < 40, "adult")
        .when(F.col("age") < 60, "middle_aged")
        .otherwise("senior"),
    )

    # 4. Risk score composite risk indicator for underwriting
    # Higher BMI + smoker + older age = higher risk
    df = df.withColumn(
        "risk_score",
        F.round(
            (F.col("bmi") * 0.3)
            + (F.when(F.col("smoker") == "yes", 50).otherwise(0))
            + (F.col("age") * 0.2),
            2,
        ),
    )

    # 5. Charges bucket premium tier classification
    df = df.withColumn(
        "charges_bucket",
        F.when(F.col("charges") < 5000, "low")
        .when(F.col("charges") < 15000, "medium")
        .when(F.col("charges") < 30000, "high")
        .otherwise("very_high"),
    )

    # 6. Pipeline metadata columns critical for data lineage
    # Always add these in production pipelines for auditability
    df = df.withColumn(
        "ingestion_timestamp", F.lit(datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S"))
    )
    df = df.withColumn("pipeline_version", F.lit("1.0.0"))

    logger.info("Transformations applied successfully")
    return df


def write_to_processed(df: DataFrame, bucket: str, timestamp: str):
    """Writes transformed data to MinIO processed bucket in Parquet format.

    why parquet instead of CSV?
    - columnar format - reads only the columns needed (faster analytics)
    - Built-in compression - typically 3-5x smaller than CSV
    - Schema embedded - no guessing column types on read
    - Industry standard for data lakes (S3, Redshift Spectrum, Glue)
    """
    output_path = f"s3a://{bucket}/transformed_{timestamp}"
    df.write.mode("overwrite").parquet(output_path)
    logger.info(f"Written {df.count()} records to {output_path}")


def main():
    bucket_raw = os.getenv("MINIO_BUCKET_RAW", "insurance-raw")
    bucket_processed = os.getenv("MINIO_BUCKET_PROCESSED", "insurance-processed")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")

    logger.info("Starting transformation job")
    spark = create_spark_session()

    # Suppress verbose Spark logs show only our logger
    spark.sparkContext.setLogLevel("ERROR")

    try:
        # Step 1: Read
        df_raw = read_latest_raw_file(spark, bucket_raw)

        # Step 2: Validate
        df_valid = validate_data(df_raw)

        # Step 3: Transform
        df_transformed = transform_data(df_valid)

        # Step 4: Write
        write_to_processed(df_transformed, bucket_processed, timestamp)

        logger.info("Transformation job completed successfully")

    except Exception as e:
        logger.error(f"Transformation job failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    main()
