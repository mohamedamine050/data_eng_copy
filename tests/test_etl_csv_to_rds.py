"""
test_etl_csv_to_rds.py
═══════════════════════
Unit tests for src.jobs.etl_csv_to_rds
"""

import io
import json
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

# ─────────────────────────────────────────────
# IMPORT MODULE UNDER TEST
# ─────────────────────────────────────────────
import src.jobs.etl_csv_to_rds as etl

# IMPORTANT: alias pour les patchs globaux
sys.modules["etl_csv_to_rds"] = etl


# ─────────────────────────────────────────────
# MOCK AWS GLUE (IMPORTANT FIX CI)
# ─────────────────────────────────────────────
awsglue_mock = types.ModuleType("awsglue")
awsglue_utils_mock = types.ModuleType("awsglue.utils")

def fake_get_resolved_options(argv, options):
    return {"CONFIG_PATH": "s3://bucket/config.json"}

awsglue_utils_mock.getResolvedOptions = fake_get_resolved_options

sys.modules["awsglue"] = awsglue_mock
sys.modules["awsglue.utils"] = awsglue_utils_mock


# ─────────────────────────────────────────────
# TESTS
# ─────────────────────────────────────────────

class TestGetArgs(unittest.TestCase):
    def test_get_args(self):
        test_args = ["etl_csv_to_rds.py", "--CONFIG_PATH", "s3://my-bucket/config.json"]

        with patch.object(sys, "argv", test_args):
            args = etl.get_args()
            self.assertEqual(args["CONFIG_PATH"], "s3://my-bucket/config.json")


class TestTransform(unittest.TestCase):
    def test_transform_basic_cleaning(self):
        df = pd.DataFrame({
            " Name ": [" a ", " b "],
            "value ": ["1", "2"]
        })

        out = etl.transform(df)

        self.assertIn("name", out.columns)
        self.assertIn("value", out.columns)
        self.assertIn("ingestion_timestamp", out.columns)

        self.assertEqual(out["name"].iloc[0], "a")


class TestReadCsvFromS3(unittest.TestCase):
    @patch("boto3.client")
    def test_read_csv(self, mock_boto):
        fake_csv = "a,b\n1,2"
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(fake_csv.encode())
        }
        mock_boto.return_value = mock_s3

        df = etl.read_csv_from_s3("bucket", "key.csv")

        self.assertEqual(list(df.columns), ["a", "b"])
        self.assertEqual(len(df), 1)


class TestRdsEngine(unittest.TestCase):
    def test_default_port(self):
        cfg = {
            "DB_HOST": "localhost",
            "DB_PORT": "5432",
            "DB_NAME": "db",
            "DB_USER": "u",
            "DB_PASSWORD": "p"
        }

        engine = etl.get_rds_engine(cfg)
        self.assertIsNotNone(engine)


class TestMain(unittest.TestCase):
    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    def test_main(self, mock_cfg, mock_engine, mock_read, mock_load):
        mock_cfg.return_value = {
            "INPUT_BUCKET_NAME": "b",
            "INPUT_KEY_NAME": "k",
            "DB_TABLE": "t",
            "DB_HOST": "localhost",
            "DB_NAME": "db",
            "DB_USER": "u",
            "DB_PASSWORD": "p",
        }

        mock_read.return_value = pd.DataFrame({"a": [1]})
        mock_engine.return_value = MagicMock()
        mock_load.return_value = 1

        test_args = ["etl_csv_to_rds.py", "--CONFIG_PATH", "s3://bucket/config.json"]

        with patch.object(sys, "argv", test_args):
            etl.main()
