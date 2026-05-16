import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import io
import sys
import json

# Fix: Import from the correct module path
from src.jobs.etl_csv_to_rds import (
    get_args, 
    transform, 
    read_csv_from_s3, 
    main, 
    load_config, 
    get_rds_engine,
    load_to_rds
)

class TestLoadConfig(unittest.TestCase):
    @patch("boto3.client")
    def test_load_config_ok(self, mock_boto):
        """Test loading valid config from S3."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(json.dumps({"test": "value"}).encode("utf-8"))
        }
        mock_boto.return_value = mock_s3
        
        config = load_config("s3://my-bucket/config.json")
        self.assertEqual(config, {"test": "value"})
    
    @patch("boto3.client")
    def test_load_config_invalid_json_raises(self, mock_boto):
        """Test loading invalid JSON raises exception."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"invalid json")
        }
        mock_boto.return_value = mock_s3
        
        with self.assertRaises(json.JSONDecodeError):
            load_config("s3://my-bucket/config.json")

class TestReadCsvFromS3(unittest.TestCase):
    @patch("boto3.client")
    def test_read_returns_dataframe(self, mock_boto):
        """Test reading CSV from S3 returns a DataFrame."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"col1,col2\nval1,val2")
        }
        mock_boto.return_value = mock_s3
        
        df = read_csv_from_s3("fake-bucket", "fake.csv")
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(len(df), 1)
    
    @patch("boto3.client")
    def test_read_correct_columns(self, mock_boto):
        """Test reading CSV returns correct columns."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"col1,col2\nval1,val2")
        }
        mock_boto.return_value = mock_s3
        
        df = read_csv_from_s3("fake-bucket", "fake.csv")
        self.assertIn("col1", df.columns)
        self.assertIn("col2", df.columns)

class TestGetRdsEngine(unittest.TestCase):
    @patch("src.jobs.etl_csv_to_rds.sqlalchemy.create_engine")
    def test_default_port_5432(self, mock_create_engine):
        """Test engine uses default port 5432 when not specified."""
        config = {
            "DB_HOST": "localhost",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
        }
        get_rds_engine(config)
        call_args = mock_create_engine.call_args[0][0]
        self.assertIn(":5432/", call_args)
    
    @patch("src.jobs.etl_csv_to_rds.sqlalchemy.create_engine")
    def test_engine_created_with_correct_url(self, mock_create_engine):
        """Test engine is created with correct database URL."""
        config = {
            "DB_HOST": "localhost",
            "DB_PORT": "5433",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
        }
        get_rds_engine(config)
        expected_url = "postgresql+psycopg2://user:pass@localhost:5433/testdb"
        mock_create_engine.assert_called_once()
        actual_url = mock_create_engine.call_args[0][0]
        self.assertEqual(actual_url, expected_url)
    
    @patch("src.jobs.etl_csv_to_rds.sqlalchemy.create_engine")
    def test_engine_strips_port_from_host(self, mock_create_engine):
        """Test engine strips port number from host if present."""
        config = {
            "DB_HOST": "localhost:5432",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
        }
        get_rds_engine(config)
        call_args = mock_create_engine.call_args[0][0]
        self.assertIn("@localhost:", call_args)
        self.assertNotIn("@localhost:5432:", call_args)

class TestMain(unittest.TestCase):
    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    def test_main_end_to_end(self, mock_load_config, mock_read_csv, mock_get_engine, mock_load_to_rds):
        """Test main function end-to-end with mocks."""
        mock_load_config.return_value = {
            "INPUT_BUCKET_NAME": "test-bucket",
            "INPUT_KEY_NAME": "test.csv",
            "DB_HOST": "localhost",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
        }
        mock_read_csv.return_value = pd.DataFrame({"col1": ["value1"]})
        mock_get_engine.return_value = MagicMock()
        
        test_args = ["job.py", "--CONFIG_PATH", "s3://config-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            main()
        
        mock_load_config.assert_called_once()
        mock_read_csv.assert_called_once()
        mock_get_engine.assert_called_once()
        mock_load_to_rds.assert_called_once()
    
    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    def test_main_uses_default_table_if_missing(self, mock_load_config, mock_read_csv, mock_get_engine, mock_load_to_rds):
        """Test main uses default table name when DB_TABLE not in config."""
        mock_load_config.return_value = {
            "INPUT_BUCKET_NAME": "test-bucket",
            "INPUT_KEY_NAME": "test.csv",
            "DB_HOST": "localhost",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
            # No DB_TABLE key
        }
        mock_read_csv.return_value = pd.DataFrame({"col1": ["value1"]})
        mock_get_engine.return_value = MagicMock()
        
        test_args = ["job.py", "--CONFIG_PATH", "s3://config-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            main()
        
        # Verify load_to_rds was called with default table name "etl_output"
        args, kwargs = mock_load_to_rds.call_args
        self.assertEqual(args[2], "etl_output")  # table name is the third positional argument

class TestTransform(unittest.TestCase):
    def test_string_values_stripped(self):
        """Test that string values are stripped of whitespace."""
        df = pd.DataFrame({"name": [" Alice ", "Bob "], "value": [100, 200]})
        df_out = transform(df)
        self.assertEqual(df_out["name"].iloc[0], "Alice")
        self.assertEqual(df_out["name"].iloc[1], "Bob")
