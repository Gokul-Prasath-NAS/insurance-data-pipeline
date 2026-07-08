"""
test_transformation.py

Unit tests for the PySpark transformation layer.

Tests transformation logic using a small in-memory DataFrame instead of reading from MinIO - fast, isolated, no infrastructure needed. 
This is the standard approach for testing Spark jobs.

Run with: pytest tests/test_transformation.py -v
"""

import os
import sys
import pytest

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Spark session fixture
# Shared across all tests in this file - creating SparkSession
# is expensive so we create it once and reuse it.
# Same pattern used in enterprise Spark test suites.


@pytest.fixture(scope="module")
def spark():
    """Creates a local SparkSession for testing."""
    from pyspark.sql import SparkSession
    
    spark = (
        SparkSession.builder
        .appName("InsuranceTransformationTests")
        .master("local[1]")  # single thread for tests
        .config("spark.ui.enabled", "false")  # disable Spark UI for speed
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

@pytest.fixture
def sample_df(spark):
    """
    Creates a small sample DataFrame for testing.
    Represents a clean subset of the insurance dataset.
    """
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    schema = StructType([
        StructField("age", IntegerType(), False),
        StructField("sex", StringType(), False),
        StructField("bmi", DoubleType(), False),
        StructField("children", IntegerType(), False),
        StructField("smoker", StringType(), False),
        StructField("region", StringType(), False),
        StructField("charges", DoubleType(), False)
    ])

    data = [
        (19, "female", 27.9, 0, "yes", "southwest", 16884.92),
        (18, "male", 33.77, 1, "no", "southeast", 1725.55),
        (28, "male", 33.0, 3, "no", "southeast", 4449.46),
        (33, "male", 22.705, 0, "no", "northwest", 21984.47),
        (32, "male", 28.88, 8, "no", "northwest", 3866.86),
        (31, "female", 25.74, 0, "no", "southeast", 3756.62),
        (46, "female", 33.44, 1, "no", "southeast", 8240.59),
        (37, "female", 27.74, 3, "no", "northwest", 7281.51),
        (37, "male", 29.83, 2, "no", "northeast", 6406.41),
        (60, "female", 25.84, 0, "no", "northwest", 28923.14)
    ]

    return spark.createDataFrame(data, schema)

@pytest.fixture
def dirty_df(spark):
    """DataFrame with invalid records for validation testing."""
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

    schema = StructType([
        StructField("age", IntegerType(), True),
        StructField("sex", StringType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("children", IntegerType(), True),
        StructField("smoker", StringType(), True),
        StructField("region", StringType(), True),
        StructField("charges", DoubleType(), True)
    ])

    data = [
        (19, "female", 27.9, 0, "yes", "southwest", 16884.92),  # valid
        (None, "male", 33.77, 1, "no", "southeast", 1725.55),   # null age
        (28, "male", -5.0, 3, "no", "southeast", 4449.46),      # invalid bmi
        (200, "male", 28.88, 0, "no", "northwest", 3866.86),    # invalid age
        (33, "male", 22.70, 0, "no", "northwest", -100.0)       # invalid charges
    ]

    return spark.createDataFrame(data, schema)

class TestValidateData:
    """Tests for data validation function."""

    def test_valid_records_pass_through(self, sample_df):
        """All valid records should pass validation unchanged."""
        from jobs.transformation.transform_claims import validate_data
        result = validate_data(sample_df)
        assert result.count() == 10

    def test_null_values_dropped(self, dirty_df):
        """Records with null critical fields should be dropped."""
        from jobs.transformation.transform_claims import validate_data
        result = validate_data(dirty_df)
        assert result.count() < 5

    def test_invalid_age_dropped(self, dirty_df):
        """Records with age > 120 or age <= 0 should be dropped."""
        from jobs.transformation.transform_claims import validate_data
        result = validate_data(dirty_df)
        ages = [row.age for row in result.collect()]
        assert all(0 < age <= 120 for age in ages)

    def test_negative_charges_dropped(self, dirty_df):
        """Records with charges <= 0 should be dropped."""
        from jobs.transformation.transform_claims import validate_data
        result = validate_data(dirty_df)
        charges = [row.charges for row in result.collect()]
        assert all(c > 0 for c in charges)

    def test_negative_bmi_dropped(self, dirty_df):
        """Records with bmi <= 0 should be dropped."""
        from jobs.transformation.transform_claims import validate_data
        result = validate_data(dirty_df)
        bmis = [row.bmi for row in result.collect()]
        assert all(b > 0 for b in bmis)

class TestTransformData:
    """Tests for transformation enrichment functions."""

    def test_bmi_category_column_added(self, sample_df):
        """bmi_category column should be present after transform."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        assert "bmi_category" in result.columns

    def test_age_group_column_added(self, sample_df):
        """age_group column should be present after transform."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        assert "age_group" in result.columns

    def test_risk_score_column_added(self, sample_df):
        """risk score column should be present after transform."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        assert "risk_score" in result.columns

    def test_charges_bucket_column_added(self, sample_df):
        """charges bucket column should be present after transform."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        assert "charges_bucket" in result.columns

    def test_bmi_categories_correct(self, sample_df):
        """BMI categories should follow medical classification."""
        from jobs.transformation.transform_claims import transform_data
        from pyspark.sql import functions as F
        result = transform_data(sample_df)

        # BMI 27.9 should be overweight (25-30)
        row1 = result.filter(F.col("bmi") == 27.9).first()
        assert row1.bmi_category == "overweight"

        # BMI 22.705 should be normal (18.5-25)
        row2 = result.filter(F.col("bmi") == 22.705).first()
        assert row2.bmi_category == "normal"

    def test_age_groups_correct(self, sample_df):
        """Age groups should be correctly assigned."""
        from jobs.transformation.transform_claims import transform_data
        from pyspark.sql import functions as F
        result = transform_data(sample_df)

        # Age 19 should be young adult (<25)
        row1 = result.filter(F.col("age") == 19).first()
        assert row1.age_group == "young adult"

        # Age 60 should be senior (>= 60)
        row2 = result.filter(F.col("age") == 60).first()
        assert row2.age_group == "senior"

    def test_smoker_column_lowercased(self, sample_df):
        """Smoker column should be lowercase after transform."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        smoker_values = [row.smoker for row in result.select("smoker").collect()]
        assert all(v == v.lower() for v in smoker_values)

    def test_pipeline_metadata_added(self, sample_df):
        """Pipeline metadata columns should be present."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        assert "ingestion_timestamp" in result.columns
        assert "pipeline_version" in result.columns

    def test_record_count_unchanged(self, sample_df):
        """Transformation should not add or remove records."""
        from jobs.transformation.transform_claims import transform_data
        result = transform_data(sample_df)
        assert result.count() == sample_df.count()