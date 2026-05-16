import unittest
from unittest.mock import MagicMock, patch
import pandas as pd
import io
import sys

# Importation propre depuis le module réel
from src.jobs.etl_csv_to_rds import get_args, transform, read_csv_from_s3, main

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

    # Utilisation du chemin complet du module pour le patching
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
            "DB_NAME": "db", "DB_USER": "u", "DB_PASSWORD": "p"
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
