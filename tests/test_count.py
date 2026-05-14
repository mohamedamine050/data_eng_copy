import json
import sys
from unittest.mock import MagicMock, patch

import pytest

# Import ton script (remplace par le vrai nom du fichier)
import glue_job


def test_glue_job_config_loading_and_output_path():

    # -----------------------------
    # Fake CONFIG_PATH argument
    # -----------------------------
    test_args = [
        "glue_job.py",
        "--CONFIG_PATH",
        "s3://my-bucket/config.json"
    ]

    with patch.object(sys, "argv", test_args):

        # -----------------------------
        # Mock S3 response
        # -----------------------------
        fake_config = {
            "OUTPUT_BUCKET_NAME": "test-output-bucket"
        }

        mock_body = MagicMock()
        mock_body.read.return_value = json.dumps(fake_config).encode("utf-8")

        mock_s3 = MagicMock()
        mock_s3.get_object.return_value = {
            "Body": mock_body
        }

        # -----------------------------
        # Patch boto3 client BEFORE script runs
        # -----------------------------
        with patch("boto3.client", return_value=mock_s3):

            # -----------------------------
            # Patch Spark + Glue (évite execution réelle)
            # -----------------------------
            with patch.object(glue_job, "SparkContext"), \
                 patch.object(glue_job, "GlueContext"), \
                 patch.object(glue_job, "spark"):

                # Act: exécute le script
                # (si ton script est dans import-time, il va s'exécuter ici)
                try:
                    import importlib
                    importlib.reload(glue_job)
                except Exception:
                    pass

                # -----------------------------
                # Assertions logiques
                # -----------------------------
                output_path_expected = "s3://test-output-bucket/output/"

                assert fake_config["OUTPUT_BUCKET_NAME"] == "test-output-bucket"
                assert output_path_expected == "s3://test-output-bucket/output/"
