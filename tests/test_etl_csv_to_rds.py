import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import io
import sys
import json

# Fix: Import from the correct module path
from src.jobs.etl_csv_to_rds import (
    get_args, transform, read_csv_from_s3, main, 
    load_config, get_rds_engine, load_to_rds
)

class TestETL(unittest.TestCase):

    def test_get_args(self):
        """Vérifie que l'argument CONFIG_PATH est bien capturé."""
        test_args = ["job.py", "--CONFIG_PATH", "s3://my-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            args = get_args()
            self.assertEqual(args["CONFIG_PATH"], "s3://my-bucket/config.json")

    def test_transform_logic(self):
        """Vérifie le nettoyage des colonnes et des espaces."""
        df = pd.DataFrame({" Name ": [" Alice "], " Value ": [100]})
        df_out = transform(df)
        self.assertIn("name", df_out.columns)
        self.assertEqual(df_out["name"].iloc[0], "Alice")
        self.assertIn("ingestion_timestamp", df_out.columns)

    @patch("boto3.client")
    def test_read_csv_from_s3(self, mock_boto):
        """Test de lecture S3 mocké."""
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(b"col1,col2\nval1,val2")
        }
        mock_boto.return_value = mock_s3

        df = read_csv_from_s3("fake-bucket", "fake.csv")
        self.assertEqual(len(df), 1)
        self.assertEqual(df["col1"].iloc[0], "val1")

    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    def test_main_workflow(self, mock_cfg, mock_read, mock_engine, mock_load):
        """Test d'intégration complet avec mocks."""
        mock_cfg.return_value = {
            "INPUT_BUCKET_NAME": "test-bkt",
            "INPUT_KEY_NAME": "test.csv",
            "DB_HOST": "localhost",
            "DB_NAME": "db", 
            "DB_USER": "u", 
            "DB_PASSWORD": "p"
        }
        mock_read.return_value = pd.DataFrame({"id": [1]})
        mock_engine.return_value = MagicMock()

        test_args = ["job.py", "--CONFIG_PATH", "s3://test-bkt/conf.json"]
        with patch.object(sys, "argv", test_args):
            main()
            
            # Vérifications des appels
            mock_cfg.assert_called_once_with("s3://test-bkt/conf.json")
            mock_read.assert_called_once_with("test-bkt", "test.csv")
            self.assertTrue(mock_load.called)

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
    
    @patch("src.jobs.etl_csv_to_rds.sqlalchemy.create_engine")
    def test_get_rds_engine_default_port_5432(self, mock_create_engine):
        """Test engine creation with default port."""
        config = {
            "DB_HOST": "localhost",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
        }
        get_rds_engine(config)
        mock_create_engine.assert_called_once()
        # Check that URL contains port 5432
        call_args = mock_create_engine.call_args[0][0]
        self.assertIn(":5432/", call_args)
    
    @patch("src.jobs.etl_csv_to_rds.sqlalchemy.create_engine")
    def test_get_rds_engine_custom_port(self, mock_create_engine):
        """Test engine creation with custom port."""
        config = {
            "DB_HOST": "localhost",
            "DB_PORT": "5433",
            "DB_USER": "user",
            "DB_PASSWORD": "pass",
            "DB_NAME": "testdb"
        }
        get_rds_engine(config)
        call_args = mock_create_engine.call_args[0][0]
        self.assertIn(":5433/", call_args)
    
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    def test_main_end_to_end(self, mock_load, mock_engine, mock_config, mock_read):
        """Test main end-to-end with mocks."""
        mock_config.return_value = {
            "INPUT_BUCKET_NAME": "test-bucket",
            "INPUT_KEY_NAME": "test.csv",
            "DB_HOST": "localhost",
            "DB_NAME": "testdb",
            "DB_USER": "user",
            "DB_PASSWORD": "pass"
        }
        mock_read.return_value = pd.DataFrame({"col1": ["value1"], "col2": ["value2"]})
        mock_engine.return_value = MagicMock()
        
        test_args = ["job.py", "--CONFIG_PATH", "s3://config-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            main()
        
        mock_config.assert_called_once()
        mock_read.assert_called_once()
        mock_engine.assert_called_once()
        mock_load.assert_called_once()
    
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    def test_main_uses_default_table_if_missing(self, mock_load, mock_engine, mock_config, mock_read):
        """Test main uses default table name when DB_TABLE not in config."""
        mock_config.return_value = {
            "INPUT_BUCKET_NAME": "test-bucket",
            "INPUT_KEY_NAME": "test.csv",
            "DB_HOST": "localhost",
            "DB_NAME": "testdb",
            "DB_USER": "user",
            "DB_PASSWORD": "pass"
            # No DB_TABLE key
        }
        mock_read.return_value = pd.DataFrame({"col1": ["value1"]})
        mock_engine.return_value = MagicMock()
        
        test_args = ["job.py", "--CONFIG_PATH", "s3://config-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            main()
        
        # Verify load_to_rds was called with default table name "etl_output"
        mock_load.assert_called_once()
        call_args = mock_load.call_args[0]
        self.assertEqual(call_args[2], "etl_output")  # table name is the third argument
