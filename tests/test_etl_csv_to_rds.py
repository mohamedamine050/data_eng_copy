import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import io
import sys

# Import direct depuis le package src
from src.jobs.etl_csv_to_rds import get_args, transform, read_csv_from_s3, get_rds_engine, main

class TestETL(unittest.TestCase):

    def test_get_args(self):
        """Vérifie la récupération des arguments via argparse (fallback Glue)."""
        test_args = ["etl_csv_to_rds.py", "--CONFIG_PATH", "s3://my-bucket/config.json"]
        with patch.object(sys, "argv", test_args):
            args = get_args()
            self.assertEqual(args["CONFIG_PATH"], "s3://my-bucket/config.json")

    def test_transform_cleaning(self):
        """Vérifie le nettoyage des colonnes et des strings."""
        df = pd.DataFrame({
            " Full Name ": [" Alice ", " Bob "],
            "Age": [25, 30]
        })
        df_out = transform(df)
        
        # Vérification colonnes
        self.assertIn("full_name", df_out.columns)
        # Vérification strip()
        self.assertEqual(df_out["full_name"].iloc[0], "Alice")
        # Vérification timestamp
        self.assertIn("ingestion_timestamp", df_out.columns)

    @patch("boto3.client")
    def test_read_csv_from_s3(self, mock_boto):
        """Vérifie la lecture S3 via mock boto3."""
        mock_s3 = MagicMock()
        fake_csv = "id,name\n1,test"
        mock_s3.get_object.return_value = {
            "Body": io.BytesIO(fake_csv.encode("utf-8"))
        }
        mock_boto.return_value = mock_s3

        df = read_csv_from_s3("my-bucket", "data.csv")
        self.assertEqual(len(df), 1)
        self.assertEqual(df["name"].iloc[0], "test")

    @patch("src.jobs.etl_csv_to_rds.load_to_rds")
    @patch("src.jobs.etl_csv_to_rds.get_rds_engine")
    @patch("src.jobs.etl_csv_to_rds.read_csv_from_s3")
    @patch("src.jobs.etl_csv_to_rds.load_config")
    def test_main_flow(self, mock_cfg, mock_read, mock_engine, mock_load):
        """Test d'intégration du workflow main() avec mocks de fonctions."""
        # Configuration simulée
        mock_cfg.return_value = {
            "INPUT_BUCKET_NAME": "in-bkt",
            "INPUT_KEY_NAME": "file.csv",
            "DB_HOST": "localhost",
            "DB_NAME": "testdb",
            "DB_USER": "admin",
            "DB_PASSWORD": "password"
        }
        mock_read.return_value = pd.DataFrame({"col1": [1]})
        mock_engine.return_value = MagicMock()

        test_args = ["job.py", "--CONFIG_PATH", "s3://bkt/cfg.json"]
        with patch.object(sys, "argv", test_args):
            main()
            
            # Vérifications
            mock_cfg.assert_called_once_with("s3://bkt/cfg.json")
            mock_read.assert_called_once_with("in-bkt", "file.csv")
            self.assertTrue(mock_load.called)

if __name__ == "__main__":
    unittest.main()
