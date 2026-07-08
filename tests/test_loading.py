"""
test_loading.py

Unit tests for the warehouse loading layer.
Tests DuckDB schema creation, dimension loading, fact table loading and post-load validation queries.
Uses an in-memory DuckDB database - no MinIO needed.

Run with: pytest tests/test_loading.py -v
"""

import os
import sys
import pytest
import pandas as pd
import duckdb
from datetime import datetime, timezone

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobs.loading.load_to_warehouse import (
    get_warehouse_connection,
    create_warehouse_schema,
    load_dim_customers,
    load_dim_regions,
    load_fact_claims,
    run_validation_queries,
)

# In-memory DuckDB fixture
# Uses: :memory: database - creates fresh DB for each test
# No files created, no cleanup needed - fast and isolated
@pytest.fixture
def conn():
    """Creates fresh in-memory DuckDB connection for each test."""
    connection = duckdb.connect(":memory:")
    create_warehouse_schema(connection)
    yield connection
    connection.close()

@pytest.fixture
def sample_df():
    """Sample transformed DataFrame matching pipeline output."""
    return pd.DataFrame({
        "age": [19, 18, 28, 33, 32],
        "sex": ["female", "male", "male", "male", "male"],
        "bmi": [27.9, 33.77, 33.0, 22.705, 28.88],
        "children": [0, 1, 3, 0, 0],
        "smoker": ["yes", "no", "no", "no", "no"],
        "region": ["southwest", "southeast", "southeast", "northwest", "northwest"],
        "charges": [16884.92, 1725.55, 4449.46, 21984.47, 3866.86],
        "age_group": ["young_adult", "young_adult", "adult", "adult", "adult"],
        "bmi_category": ["overweight", "obese", "obese", "normal", "overweight"],
        "risk_score": [61.17, 13.63, 15.5, 13.36, 15.14],
        "charges_bucket": ["very high", "low", "low", "very high", "low"],
        "ingestion_timestamp": ["2026-06-24 12:00:00"] * 5,
        "pipeline_version": ["1.0.0"] * 5,
    })

class TestCreateWarehouseSchema:
    """Tests for schema creation."""

    def test_dim_customers_table_created(self, conn):
        """dim_customers table should exist after schema creation."""
        result = conn.execute("""
            SELECT table_name FROM information_schema.tables WHERE table_name = 'dim_customers'
        """).fetchall()
        assert len(result) == 1

    def test_dim_regions_table_created(self, conn):
        """dim_regions table should exist after schema creation."""
        result = conn.execute("""
            SELECT table_name FROM information_schema.tables WHERE table_name = 'dim_regions'
        """).fetchall()
        assert len(result) == 1

    def test_fact_claims_table_created(self, conn):
        """fact_claims table should exist after schema creation."""
        result = conn.execute("""
            SELECT table_name FROM information_schema.tables WHERE table_name = 'fact_claims'
        """).fetchall()
        assert len(result) == 1

    def test_schema_is_idempotent(self, conn):
        """Running schema creation twice should not raise errors."""
        create_warehouse_schema(conn)  # run again
        result = conn.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_name IN ('dim_customers', 'dim_regions', 'fact_claims')
        """).fetchone()[0]
        assert result == 3

class TestLoadDimCustomers:
    """Tests for customer dimension loading."""

    def test_customers_loaded_correctly(self, conn, sample_df):
        """Should load correct number of unique customers."""
        load_dim_customers(conn, sample_df)
        count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        assert count == len(sample_df.drop_duplicates(subset=["age", "sex", "bmi", "smoker", "children"]))

    def test_customer_id_is_sequential(self, conn, sample_df):
        """customer_id should start at 1 and be sequential."""
        load_dim_customers(conn, sample_df)
        ids = conn.execute("SELECT customer_id FROM dim_customers ORDER BY customer_id").fetchall()
        expected = list(range(1, len(ids) + 1))
        assert [r[0] for r in ids] == expected

    def test_load_is_idempotent(self, conn, sample_df):
        """Loading twice should not create duplicate records."""
        load_dim_customers(conn, sample_df)
        load_dim_customers(conn, sample_df)
        count = conn.execute("SELECT COUNT(*) FROM dim_customers").fetchone()[0]
        assert count == len(sample_df.drop_duplicates(subset=["age", "sex", "bmi", "smoker", "children"]))

class TestLoadDimRegions:
    """Tests for region dimension loading."""

    def test_regions_loaded_correctly(self, conn, sample_df):
        """Should load correct number of unique regions."""
        load_dim_regions(conn, sample_df)
        count = conn.execute("SELECT COUNT(*) FROM dim_regions").fetchone()[0]
        assert count == sample_df["region"].nunique()

    def test_region_names_correct(self, conn, sample_df):
        """Region names should match source data."""
        load_dim_regions(conn, sample_df)
        regions = conn.execute("SELECT region_name FROM dim_regions ORDER BY region_name").fetchall()
        region_names = sorted([r[0] for r in regions])
        expected = sorted(sample_df["region"].unique().tolist())
        assert region_names == expected

    def test_load_is_idempotent(self, conn, sample_df):
        """Loading regions twice should not create duplicates."""
        load_dim_regions(conn, sample_df)
        load_dim_regions(conn, sample_df)
        count = conn.execute("SELECT COUNT(*) FROM dim_regions").fetchone()[0]
        assert count == sample_df["region"].nunique()

class TestLoadFactClaims:
    """Tests for fact table loading."""

    def test_fact_claims_loaded_correctly(self, conn, sample_df):
        """Should load all records into fact_claims."""
        dim_customers = load_dim_customers(conn, sample_df)
        dim_regions = load_dim_regions(conn, sample_df)
        load_fact_claims(conn, sample_df, dim_customers, dim_regions)
        count = conn.execute("SELECT COUNT(*) FROM fact_claims").fetchone()[0]
        assert count == len(sample_df)

    def test_no_orphaned_records(self, conn, sample_df):
        """All fact records should have valid dimension references."""
        dim_customers = load_dim_customers(conn, sample_df)
        dim_regions = load_dim_regions(conn, sample_df)
        load_fact_claims(conn, sample_df, dim_customers, dim_regions)
        orphans = conn.execute("""
            SELECT COUNT(*) FROM fact_claims f 
            LEFT JOIN dim_customers c ON f.customer_id = c.customer_id 
            WHERE c.customer_id IS NULL
        """).fetchone()[0]
        assert orphans == 0

    def test_charges_values_preserved(self, conn, sample_df):
        """Charges values should be preserved accurately."""
        dim_customers = load_dim_customers(conn, sample_df)
        dim_regions = load_dim_regions(conn, sample_df)
        load_fact_claims(conn, sample_df, dim_customers, dim_regions)
        min_charge = conn.execute("SELECT MIN(charges) FROM fact_claims").fetchone()[0]
        assert min_charge > 0

    def test_load_is_idempotent(self, conn, sample_df):
        """Loading facts twice should not create duplicates."""
        dim_customers = load_dim_customers(conn, sample_df)
        dim_regions = load_dim_regions(conn, sample_df)
        load_fact_claims(conn, sample_df, dim_customers, dim_regions)
        load_fact_claims(conn, sample_df, dim_customers, dim_regions)
        count = conn.execute("SELECT COUNT(*) FROM fact_claims").fetchone()[0]
        assert count == len(sample_df)

class TestRunValidationQueries:
    """Tests for post-load validation."""

    def test_validation_runs_without_error(self, conn, sample_df):
        """Validation queries should run without raising exceptions."""
        dim_customers = load_dim_customers(conn, sample_df)
        dim_regions = load_dim_regions(conn, sample_df)
        load_fact_claims(conn, sample_df, dim_customers, dim_regions)
        try:
            run_validation_queries(conn)
        except Exception as e:
            pytest.fail(f"Validation raised unexpected exception: {e}")