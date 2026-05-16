import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import io
import sys

# On importe depuis le chemin réel
from src.jobs.etl_csv_to_rds import get_args, transform, read_csv_from_s3, main

class TestETL(unittest.TestCase):

    def test_get_args(self):
        """Vérifie que les arguments sont bien lus depuis sys.argv."""
        test_args = ["job.py", "--CONFIG_PATH", "s3://my-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            args = get_args()
            self.assertEqual(args["CONFIG_PATH"], "s3://my-bucket/config.json")

    def test_transform_logic(self):
        """Vérifie le nettoyage des données."""
        df = pd.DataFrame({" Name ": [" Alice "], "Age": [25]})
        df_out = transform(df)
        self.assertEqual(df_out["name"].iloc[0], "Alice")
        self.assertIn("ingestion_timestamp", df_out.columns)

    @patch("boto3.client")
    def test_read_csv(self, mock_boto):
        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {"Body": io.BytesIO(b"id,val\n1,test")}
        mock_boto.return_value = mock_s3
        
        df = read_csv_from_s3("my-bucket", "data.csv")
        self.assertEqual(df["val"].iloc[0], "test")

    # Correction des chemins de patch pour éviter ModuleNotFoundError
    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    def test_main_flow(self, mock_cfg, mock_read, mock_engine, mock_load):
        mock_cfg.return_value = {
            "INPUT_BUCKET_NAME": "my-bucket",
            "INPUT_KEY_NAME": "data.csv",
            "DB_HOST": "localhost",
            "DB_NAME": "db", "DB_USER": "u", "DB_PASSWORD": "p"
        }
        mock_read.return_value = pd.DataFrame({"a": [1]})
        
        test_args = ["job.py", "--CONFIG_PATH", "s3://my-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            main()
            mock_cfg.assert_called_once_with("s3://my-bucket/config.json")
            self.assertTrue(mock_load.called)
