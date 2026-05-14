import sys
import types
from unittest.mock import Mock, patch
import json


# -----------------------------
# FAKE awsglue MODULE (IMPORTANT)
# -----------------------------
mock_awsglue = types.ModuleType("awsglue")
mock_context = types.ModuleType("awsglue.context")
mock_utils = types.ModuleType("awsglue.utils")

mock_context.GlueContext = Mock()
mock_utils.getResolvedOptions = Mock()

mock_awsglue.context = mock_context
mock_awsglue.utils = mock_utils

sys.modules["awsglue"] = mock_awsglue
sys.modules["awsglue.context"] = mock_context
sys.modules["awsglue.utils"] = mock_utils


# -----------------------------
# IMPORT AFTER MOCK
# -----------------------------
from src.jobs.count_and_save_in_csv import run_job


# -----------------------------
# TEST
# -----------------------------
@patch("src.jobs.count_and_save_in_csv.boto3.client")
def test_run_job_success(mock_boto):

    # fake args
    mock_utils.getResolvedOptions.return_value = {
        "CONFIG_PATH": "s3://my-bucket/config.json"
    }

    # fake s3
    s3 = Mock()
    mock_boto.return_value = s3

    s3.get_object.return_value = {
        "Body": Mock(read=Mock(return_value=json.dumps({
            "OUTPUT_BUCKET_NAME": "output-bucket"
        }).encode()))
    }

    # run job
    run_job()

    # assert S3 called
    s3.get_object.assert_called_once()
