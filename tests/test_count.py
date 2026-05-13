import sys
import importlib
from unittest.mock import MagicMock, patch


def test_glue_job_main():
    # Simule les arguments Glue
    sys.argv = ["job.py", "--output_path", "s3://fake-bucket/output"]

    mock_get_resolved = MagicMock(return_value={
        "output_path": "s3://fake-bucket/output"
    })

    # Mock DataFrame
    mock_df = MagicMock()

    # Mock writer chain Spark
    mock_writer = MagicMock()
    mock_df.write = mock_writer
    mock_writer.mode.return_value = mock_writer
    mock_writer.option.return_value = mock_writer
    mock_writer.csv.return_value = None

    # Mock Spark session
    mock_spark = MagicMock()
    mock_spark.createDataFrame.return_value = mock_df

    # Mock GlueContext
    mock_glue_context = MagicMock()
    mock_glue_context.spark_session = mock_spark

    with patch("awsglue.utils.getResolvedOptions", mock_get_resolved), \
         patch("pyspark.context.SparkContext"), \
         patch("awsglue.context.GlueContext", return_value=mock_glue_context):

        import job
        importlib.reload(job)

        job.main()

        # ✅ Vérifie création DataFrame
        mock_spark.createDataFrame.assert_called_once_with(
            [(i,) for i in range(1, 21)],
            ["number"]
        )

        # ✅ Vérifie write CSV vers S3
        mock_writer.csv.assert_called_once_with("s3://fake-bucket/output")
