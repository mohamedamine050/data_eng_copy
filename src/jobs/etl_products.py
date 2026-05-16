"""
etl_products.py - Product Catalog ETL
════════════════════════════════════

Use case
--------
Nightly job that pulls the full product catalog from the FakeStore REST API,
enriches each product with business labels (price tier, rating label), and
stores the result as a clean CSV in S3 for downstream analytics / BI tools.

Pipeline
--------
FakeStore API -> transform -> S3 CSV (products/processed/catalog.csv)

Config JSON stored in S3 (loaded via --CONFIG_PATH arg):
--------------------------------------------------------
{
  "API_URL": "https://fakestoreapi.com/products",
  "OUTPUT_PREFIX": "products/processed",
  "OUTPUT_FILE_NAME": "catalog.csv"
}
"""

import io
import json
import logging
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pandas as pd
import requests

try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    def getResolvedOptions(argv: list, options: list) -> dict:
        import argparse
        parser = argparse.ArgumentParser()
        for opt in options:
            parser.add_argument(f"--{opt}")
        args, _ = parser.parse_known_args(argv[1:])
        return vars(args)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)

logger = logging.getLogger("first_etl")


def get_args() -> dict:
    return getResolvedOptions(sys.argv, ["CONFIG_PATH"])


def load_config(config_path: str) -> dict:
    logger.info("Loading config from %s", config_path)

    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)

    return json.loads(response["Body"].read().decode("utf-8"))


def fetch_products(api_url: str) -> list[dict]:
    logger.info("Fetching products from %s", api_url)

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    return response.json()


def _price_tier(price: float) -> str:
    if price < 20:
        return "budget"
    if price <= 100:
        return "mid-range"
    return "premium"


def _rating_label(rate: float) -> str:
    if rate < 3.0:
        return "low"
    if rate < 4.0:
        return "medium"
    return "high"


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("Transforming %d rows", len(df))

    critical = [c for c in ("price", "category", "rating.rate", "rating.count") if c in df.columns]
    df = df.dropna(subset=critical)

    df = df.rename(columns={
        "id": "product_id",
        "rating.rate": "rating_rate",
        "rating.count": "rating_count",
    })

    df["category"] = df["category"].str.strip().str.lower()
    df["price_tier"] = df["price"].apply(_price_tier)
    df["rating_label"] = df["rating_rate"].apply(_rating_label)

    if "image" in df.columns:
        df = df.drop(columns=["image"])

    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    return df.reset_index(drop=True)


def save_to_s3(df: pd.DataFrame, bucket: str, key: str) -> str:
    logger.info("Uploading to s3://%s/%s", bucket, key)

    buf = io.StringIO()
    df.to_csv(buf, index=False)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    return f"s3://{bucket}/{key}"


def main():
    logger.info("START ETL")

    args = get_args()
    config = load_config(args["CONFIG_PATH"])

    api_url = config["API_URL"]
    output_bucket = config["OUTPUT_BUCKET_NAME"]
    output_prefix = config.get("OUTPUT_PREFIX", "products/processed").rstrip("/")
    output_file = config.get("OUTPUT_FILE_NAME", "catalog.csv")

    output_key = f"{output_prefix}/{output_file}"

    raw = fetch_products(api_url)
    df = pd.json_normalize(raw)

    df_clean = transform_products(df)

    uri = save_to_s3(df_clean, output_bucket, output_key)

    logger.info("DONE -> %s", uri)


if __name__ == "__main__":
    main()
