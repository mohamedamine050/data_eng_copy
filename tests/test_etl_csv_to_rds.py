"""
test_etl_csv_to_rds.py
═══════════════════════
Unit tests for etl_csv_to_rds.py
"""

import io
import json
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

# ─────────────────────────────────────────────
# FIX MODULE IMPORT (IMPORTANT)
# ─────────────────────────────────────────────
# Permet aux tests de cibler src.jobs.etl_csv_to_rds
import src.jobs.etl_csv_to_rds as etl

# alias pour patch("etl_csv_to_rds....")
sys.modules.setdefault("etl_csv_to_rds", etl)


# ─────────────────────────────────────────────
# MOCK AWS GLUE
# ─────────────────────────────────────────────
awsglue_mock = types.ModuleType("awsglue")
awsglue_utils_mock = types.ModuleType("awsglue.utils")
awsglue_utils_mock.getResolvedOptions = MagicMock(
    return_value={"CONFIG_PATH": "s3://bucket/config.json"}
)

sys.modules["awsglue"] = awsglue_mock
sys.modules["awsglue.utils"] = awsglue_utils_mock


# ─────────────────────────────────────────────
# FIXTURES
# ─────────────────────────────────────────────

SAMPLE_CONFIG = {
    "INPUT_BUCKET_NAME": "my-bucket",
    "INPUT_KEY_NAME": "raw/data.csv",
    "DB_HOST": "localhost",
    "DB_PORT": "5432",
    "DB_NAME": "testdb",
    "DB_USER": "admin",
    "DB_PASSWORD": "secret",
    "DB_TABLE": "test_table",
}

SAMPLE_CSV = """id,Title,Price ,category,rating
1,Backpack,109.95,clothing,3.9
2,T-Shirt ,22.3,clothing,4.1
3,Jacket,55.99,clothing,4.7
2,T-Shirt ,22.3,clothing,4.1
4,,,,
"""


def make_df():
    return pd.read_csv(io.StringIO(SAMPLE_CSV))


# ─────────────────────────────────────────────
# TEST LOAD CONFIG
# ─────────────────────────────────────────────

class TestLoadConfig(unittest.TestCase):

    @patch("etl_csv_to_rds.boto3.client")
    def test_load_config_ok(self, mock_boto):
        body = json.dumps(SAMPLE_CONFIG).encode("utf-8")

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io.BytesIO(body)}
        mock_boto.return_value = mock_s3

        config = etl.load_config("s3://my-bucket/config.json")

        self.assertEqual(config["INPUT_BUCKET_NAME"], "my-bucket")
        self.assertEqual(config["DB_TABLE"], "test_table")

    @patch("etl_csv_to_rds.boto3.client")
    def test_load_config_invalid_json_raises(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io.BytesIO(b"not-json")}
        mock_boto.return_value = mock_s3

        with self.assertRaises(json.JSONDecodeError):
            etl.load_config("s3://my-bucket/config.json")


# ─────────────────────────────────────────────
# TEST READ CSV
# ─────────────────────────────────────────────

class TestReadCsvFromS3(unittest.TestCase):

    @patch("etl_csv_to_rds.boto3.client")
    def test_read_returns_dataframe(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(SAMPLE_CSV.encode())
        }
        mock_boto.return_value = mock_s3

        df = etl.read_csv_from_s3("my-bucket", "raw/data.csv")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)

    @patch("etl_csv_to_rds.boto3.client")
    def test_read_correct_columns(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(SAMPLE_CSV.encode())
        }
        mock_boto.return_value = mock_s3

        df = etl.read_csv_from_s3("my-bucket", "raw/data.csv")

        self.assertIn("id", df.columns)
        self.assertIn("Title", df.columns)


# ─────────────────────────────────────────────
# TEST TRANSFORM
# ─────────────────────────────────────────────

class TestTransform(unittest.TestCase):

    def setUp(self):
        self.df = make_df()

    def test_returns_dataframe(self):
        df = etl.transform(self.df.copy())
        self.assertIsInstance(df, pd.DataFrame)

    def test_empty_dataframe(self):
        df = etl.transform(pd.DataFrame())
        self.assertTrue(df.empty)

    def test_duplicates_removed(self):
        df = etl.transform(self.df.copy())
        self.assertEqual(
            len(df),
            len(df.drop_duplicates())
        )

    def test_timestamp_added(self):
        df = etl.transform(self.df.copy())
        self.assertIn("ingestion_timestamp", df.columns)


# ─────────────────────────────────────────────
# TEST RDS ENGINE
# ─────────────────────────────────────────────

class TestGetRdsEngine(unittest.TestCase):

    @patch("etl_csv_to_rds.sqlalchemy.create_engine")
    def test_engine_created(self, mock_create):
        mock_create.return_value = MagicMock()

        etl.get_rds_engine(SAMPLE_CONFIG)

        args = mock_create.call_args[0][0]
        self.assertIn("localhost", args)
        self.assertIn("5432", args)


# ─────────────────────────────────────────────
# TEST LOAD TO RDS
# ─────────────────────────────────────────────

class TestLoadToRds(unittest.TestCase):

    def test_returns_count(self):
        df = etl.transform(make_df())
        engine = MagicMock()

        with patch.object(pd.DataFrame, "to_sql", return_value=None):
            rows = etl.load_to_rds(df, engine, "table")

        self.assertEqual(rows, len(df))


# ─────────────────────────────────────────────
# TEST MAIN
# ─────────────────────────────────────────────

class TestMain(unittest.TestCase):

    @patch("etl_csv_to_rds.load_to_rds")
    @patch("etl_csv_to_rds.get_rds_engine")
    @patch("etl_csv_to_rds.read_csv_from_s3")
    @patch("etl_csv_to_rds.load_config")
    @patch("etl_csv_to_rds.get_args")
    def test_main_flow(
        self,
        mock_args,
        mock_config,
        mock_read,
        mock_engine,
        mock_load,
    ):
        mock_args.return_value = {"CONFIG_PATH": "s3://bucket/config.json"}
        mock_config.return_value = SAMPLE_CONFIG
        mock_read.return_value = make_df()
        mock_engine.return_value = MagicMock()
        mock_load.return_value = 3

        etl.main()

        mock_config.assert_called_once()
        mock_read.assert_called_once()
        mock_engine.assert_called_once()
        mock_load.assert_called_once()


if __name__ == "__main__":
    unittest.main(verbosity=2)
