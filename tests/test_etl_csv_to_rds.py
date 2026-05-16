"""
test_etl_csv_to_rds.py
═══════════════════════
Unit tests for etl_csv_to_rds.py

Run:
    pip install pytest pandas sqlalchemy psycopg2-binary boto3 moto
    pytest test_etl_csv_to_rds.py -v
"""

import io
import json
import sys
import types
import unittest
from datetime import timezone
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

# ── Mock awsglue avant l'import du module ──────────────────────────────────
awsglue_mock = types.ModuleType("awsglue")
awsglue_utils_mock = types.ModuleType("awsglue.utils")
awsglue_utils_mock.getResolvedOptions = MagicMock(return_value={"CONFIG_PATH": "s3://bucket/config.json"})
sys.modules.setdefault("awsglue", awsglue_mock)
sys.modules.setdefault("awsglue.utils", awsglue_utils_mock)

from src.jobs import etl_csv_to_rds


# ══════════════════════════════════════════════════════════════════════════════
# FIXTURES
# ══════════════════════════════════════════════════════════════════════════════

SAMPLE_CONFIG = {
    "INPUT_BUCKET_NAME": "my-bucket",
    "INPUT_KEY_NAME":    "raw/data.csv",
    "DB_HOST":           "localhost",
    "DB_PORT":           "5432",
    "DB_NAME":           "testdb",
    "DB_USER":           "admin",
    "DB_PASSWORD":       "secret",
    "DB_TABLE":          "test_table",
}

SAMPLE_CSV = """id,Title,Price ,category,rating
1,Backpack,109.95,clothing,3.9
2,T-Shirt ,22.3,clothing,4.1
3,Jacket,55.99,clothing,4.7
2,T-Shirt ,22.3,clothing,4.1
4,,,,
"""


def make_df() -> pd.DataFrame:
    return pd.read_csv(io.StringIO(SAMPLE_CSV))


# ══════════════════════════════════════════════════════════════════════════════
# load_config
# ══════════════════════════════════════════════════════════════════════════════

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
        mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="config.json")

    @patch("etl_csv_to_rds.boto3.client")
    def test_load_config_invalid_json_raises(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io.BytesIO(b"not-json")}
        mock_boto.return_value = mock_s3

        with self.assertRaises(json.JSONDecodeError):
            etl.load_config("s3://my-bucket/config.json")


# ══════════════════════════════════════════════════════════════════════════════
# read_csv_from_s3
# ══════════════════════════════════════════════════════════════════════════════

class TestReadCsvFromS3(unittest.TestCase):

    @patch("etl_csv_to_rds.boto3.client")
    def test_read_returns_dataframe(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io.BytesIO(SAMPLE_CSV.encode())}
        mock_boto.return_value = mock_s3

        df = etl.read_csv_from_s3("my-bucket", "raw/data.csv")

        self.assertIsInstance(df, pd.DataFrame)
        self.assertGreater(len(df), 0)
        mock_s3.get_object.assert_called_once_with(Bucket="my-bucket", Key="raw/data.csv")

    @patch("etl_csv_to_rds.boto3.client")
    def test_read_correct_columns(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io.BytesIO(SAMPLE_CSV.encode())}
        mock_boto.return_value = mock_s3

        df = etl.read_csv_from_s3("my-bucket", "raw/data.csv")

        self.assertIn("id", df.columns)
        self.assertIn("Price ", df.columns)   # avant transform → espaces encore présents


# ══════════════════════════════════════════════════════════════════════════════
# transform
# ══════════════════════════════════════════════════════════════════════════════

class TestTransform(unittest.TestCase):

    def setUp(self):
        self.df = make_df()

    def test_columns_normalized(self):
        df = etl.transform(self.df.copy())
        # "Price " → "price_", "Title" → "title"  (lowercase + strip)
        for col in df.columns:
            self.assertEqual(col, col.strip())
            self.assertEqual(col, col.lower())

    def test_empty_rows_removed(self):
        df = etl.transform(self.df.copy())
        # La ligne entièrement vide (id=4) doit être supprimée
        self.assertFalse(df.isnull().all(axis=1).any())

    def test_duplicates_removed(self):
        df = etl.transform(self.df.copy())
        # SAMPLE_CSV a 1 doublon (id=2 apparaît deux fois)
        self.assertEqual(len(df), df.drop_duplicates(
            subset=[c for c in df.columns if c != "ingestion_timestamp"]
        ).shape[0])

    def test_string_values_stripped(self):
        df = etl.transform(self.df.copy())
        # "T-Shirt " → "T-Shirt"
        str_cols = df.select_dtypes(include="object").columns
        for col in str_cols:
            for val in df[col].dropna():
                self.assertEqual(val, val.strip())

    def test_ingestion_timestamp_added(self):
        df = etl.transform(self.df.copy())
        self.assertIn("ingestion_timestamp", df.columns)
        # toutes les valeurs doivent être remplies
        self.assertFalse(df["ingestion_timestamp"].isnull().any())

    def test_ingestion_timestamp_is_utc_iso(self):
        df = etl.transform(self.df.copy())
        ts = df["ingestion_timestamp"].iloc[0]
        # doit contenir le marqueur UTC (+00:00 ou Z)
        self.assertTrue("+" in ts or "Z" in ts or "UTC" in ts)

    def test_numeric_conversion(self):
        df = etl.transform(self.df.copy())
        # "price" doit être numérique après transform
        if "price" in df.columns:
            self.assertTrue(pd.api.types.is_numeric_dtype(df["price"]))

    def test_returns_dataframe(self):
        df = etl.transform(self.df.copy())
        self.assertIsInstance(df, pd.DataFrame)

    def test_index_reset(self):
        df = etl.transform(self.df.copy())
        self.assertEqual(list(df.index), list(range(len(df))))

    def test_empty_dataframe_returns_empty(self):
        df_empty = pd.DataFrame()
        df = etl.transform(df_empty)
        self.assertTrue(df.empty)


# ══════════════════════════════════════════════════════════════════════════════
# get_rds_engine
# ══════════════════════════════════════════════════════════════════════════════

class TestGetRdsEngine(unittest.TestCase):

    @patch("etl_csv_to_rds.sqlalchemy.create_engine")
    def test_engine_created_with_correct_url(self, mock_create):
        mock_create.return_value = MagicMock()
        etl.get_rds_engine(SAMPLE_CONFIG)

        call_url = mock_create.call_args[0][0]
        self.assertIn("localhost", call_url)
        self.assertIn("5432", call_url)
        self.assertIn("testdb", call_url)
        self.assertIn("admin", call_url)

    @patch("etl_csv_to_rds.sqlalchemy.create_engine")
    def test_engine_strips_port_from_host(self, mock_create):
        """DB_HOST peut contenir le port collé (ex: host.rds.amazonaws.com:5432)"""
        mock_create.return_value = MagicMock()
        config = {**SAMPLE_CONFIG, "DB_HOST": "myhost.rds.amazonaws.com:5432"}
        etl.get_rds_engine(config)

        call_url = mock_create.call_args[0][0]
        # le port ne doit pas apparaître deux fois
        self.assertNotIn("5432:5432", call_url)
        self.assertIn("myhost.rds.amazonaws.com", call_url)

    @patch("etl_csv_to_rds.sqlalchemy.create_engine")
    def test_default_port_5432(self, mock_create):
        mock_create.return_value = MagicMock()
        config = {k: v for k, v in SAMPLE_CONFIG.items() if k != "DB_PORT"}
        etl.get_rds_engine(config)

        call_url = mock_create.call_args[0][0]
        self.assertIn("5432", call_url)


# ══════════════════════════════════════════════════════════════════════════════
# load_to_rds
# ══════════════════════════════════════════════════════════════════════════════

class TestLoadToRds(unittest.TestCase):

    def _clean_df(self) -> pd.DataFrame:
        return etl.transform(make_df())

    def test_returns_row_count(self):
        df = self._clean_df()
        mock_engine = MagicMock()

        with patch.object(df.__class__, "to_sql", return_value=None):
            rows = etl.load_to_rds(df, mock_engine, "test_table")

        self.assertEqual(rows, len(df))

    def test_to_sql_called_with_correct_table(self):
        df = self._clean_df()
        mock_engine = MagicMock()

        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            etl.load_to_rds(df, mock_engine, "my_table")
            mock_to_sql.assert_called_once()
            kwargs = mock_to_sql.call_args[1]
            self.assertEqual(kwargs["name"], "my_table")

    def test_to_sql_uses_append_by_default(self):
        df = self._clean_df()
        mock_engine = MagicMock()

        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            etl.load_to_rds(df, mock_engine, "my_table")
            kwargs = mock_to_sql.call_args[1]
            self.assertEqual(kwargs["if_exists"], "append")

    def test_to_sql_index_false(self):
        df = self._clean_df()
        mock_engine = MagicMock()

        with patch("pandas.DataFrame.to_sql") as mock_to_sql:
            etl.load_to_rds(df, mock_engine, "my_table")
            kwargs = mock_to_sql.call_args[1]
            self.assertFalse(kwargs["index"])


# ══════════════════════════════════════════════════════════════════════════════
# main() — test d'intégration end-to-end (tout mocké)
# ══════════════════════════════════════════════════════════════════════════════

class TestMain(unittest.TestCase):

    @patch("etl_csv_to_rds.load_to_rds")
    @patch("etl_csv_to_rds.get_rds_engine")
    @patch("etl_csv_to_rds.read_csv_from_s3")
    @patch("etl_csv_to_rds.load_config")
    @patch("etl_csv_to_rds.get_args")
    def test_main_end_to_end(
        self,
        mock_get_args,
        mock_load_config,
        mock_read_csv,
        mock_get_engine,
        mock_load_rds,
    ):
        mock_get_args.return_value    = {"CONFIG_PATH": "s3://bucket/config.json"}
        mock_load_config.return_value = SAMPLE_CONFIG
        mock_read_csv.return_value    = make_df()
        mock_get_engine.return_value  = MagicMock()
        mock_load_rds.return_value    = 3

        etl.main()   # ne doit pas lever d'exception

        mock_load_config.assert_called_once_with("s3://bucket/config.json")
        mock_read_csv.assert_called_once_with("my-bucket", "raw/data.csv")
        mock_get_engine.assert_called_once_with(SAMPLE_CONFIG)
        mock_load_rds.assert_called_once()

    @patch("etl_csv_to_rds.load_to_rds")
    @patch("etl_csv_to_rds.get_rds_engine")
    @patch("etl_csv_to_rds.read_csv_from_s3")
    @patch("etl_csv_to_rds.load_config")
    @patch("etl_csv_to_rds.get_args")
    def test_main_uses_default_table_if_missing(
        self,
        mock_get_args,
        mock_load_config,
        mock_read_csv,
        mock_get_engine,
        mock_load_rds,
    ):
        config_no_table = {k: v for k, v in SAMPLE_CONFIG.items() if k != "DB_TABLE"}
        mock_get_args.return_value    = {"CONFIG_PATH": "s3://bucket/config.json"}
        mock_load_config.return_value = config_no_table
        mock_read_csv.return_value    = make_df()
        mock_get_engine.return_value  = MagicMock()
        mock_load_rds.return_value    = 3

        etl.main()

        _, _, table_arg = mock_load_rds.call_args[0]
        self.assertEqual(table_arg, "etl_output")   # valeur par défaut


if __name__ == "__main__":
    unittest.main(verbosity=2)
