"""
test_ingestion.py

Unit tests for the ingestion layer.

Tests the ingestion job functions in isolation using mocks - we don't actually call MinIO or download real data in unit tests. 
Mocking external dependencies is standard practice in production test suites - same approach used in enterprise CI/CD pipelines.

Run with: pytest tests/test_ingestion.py -v
"""

import os
import sys
import pytest
from unittest.mock import patch, MagicMock

# Add project root to Python path so imports work
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from jobs.ingestion.ingest_raw_data import (
    download_dataset,
    upload_to_minio,
    get_minio_client,
)

class TestGetMinioClient:
    """Tests for MinIO client creation."""

    def test_client_created_with_default_values(self):
        """Client should be created successfully with default env values."""
        client = get_minio_client()
        assert client is not None

    def test_client_uses_env_variables(self):
        """Client should use MINIO_ENDPOINT from environment."""
        with patch.dict(os.environ, {
            "MINIO_ENDPOINT": "http://localhost:9000", 
            "MINIO_ROOT_USER": "testuser", 
            "MINIO_ROOT_PASSWORD": "testpass"
        }):
            client = get_minio_client()
            assert client is not None

class TestDownloadDataset:
    """Tests for dataset download function."""

    @patch("jobs.ingestion.ingest_raw_data.requests.get")
    def test_successful_download(self, mock_get):
        """Should return content bytes on successful download."""
        # Arrange - set up mock response
        mock_response = MagicMock()
        mock_response.content = b"age, sex, bmi, children, smoker, region, charges\n19, female, 27.9, 0, yes, southwest, 16884.92"
        mock_response.raise_for_status = MagicMock()
        mock_get.return_value = mock_response

        # Act
        result = download_dataset("http://fake-url.com/insurance.csv")

        # Assert
        assert result == mock_response.content
        assert len(result) > 0
        mock_get.assert_called_once_with("http://fake-url.com/insurance.csv", timeout=30)

    @patch("jobs.ingestion.ingest_raw_data.requests.get")
    def test_download_raises_on_http_error(self, mock_get):
        """Should raise an exception when HTTP request fails."""
        import requests
        mock_response = MagicMock()
        mock_response.raise_for_status.side_effect = (
            requests.exceptions.HTTPError("404 Not Found")
        )
        mock_get.return_value = mock_response

        with pytest.raises(requests.exceptions.HTTPError):
            download_dataset("http://fake-url.com/notfound.csv")

    @patch("jobs.ingestion.ingest_raw_data.requests.get")
    def test_download_raises_on_connection_error(self, mock_get):
        """Should raise exception when connection fails."""
        import requests
        mock_get.side_effect = requests.exceptions.ConnectionError("Connection refused")

        with pytest.raises(requests.exceptions.ConnectionError):
            download_dataset("http://unreachable-host.com/data.csv")

class TestUploadToMinio:
    """Tests for MinIO upload function."""

    def test_successful_upload(self):
        """Should call put_object with correct parameters."""
        # Arrange
        mock_client = MagicMock()
        test_data = b"test.csv,content"
        bucket = "insurance-raw"
        key = "insurance_20260624_120000.csv"

        # Act
        upload_to_minio(mock_client, bucket, key, test_data)

        # Assert
        mock_client.put_object.assert_called_once_with(
            Bucket=bucket, 
            Key=key, 
            Body=test_data
        )

    def test_gather_raises_on_client_error(self):
        """Should propagate exceptions from boto3 client."""
        from botocore.exceptions import ClientError
        mock_client = MagicMock()
        mock_client.put_object.side_effect = ClientError(
            {"Error": {"Code": "NoSuchBucket", "Message": "Bucket not found"}}, 
            "PutObject"
        )

        with pytest.raises(ClientError):
            upload_to_minio(
                mock_client, 
                "nonexistent-bucket", 
                "test.csv", 
                b"data"
            )

    def test_upload_with_empty_data(self):
        """Should handle empty file upload without error."""
        mock_client = MagicMock()
        upload_to_minio(mock_client, "insurance-raw", "empty.csv", b"")
        mock_client.put_object.assert_called_once()