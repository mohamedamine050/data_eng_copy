"""
etl_products.py — Product Catalog ETL
════════════════════════════════════
Use case
────────
Nightly job that pulls the full product catalog from the FakeStore REST API,
enriches each product with business labels (price tier, rating label), and
stores the result as a clean CSV in S3 for downstream analytics / BI tools.

Pipeline
────────
  FakeStore API  ──►  transform  ──►  S3 CSV (products/processed/catalog.csv)

Config JSON stored in S3  (loaded via --CONFIG_PATH arg):
──────────────────────────────────────────────────────────
{
  "API_URL":            "https://fakestoreapi.com/products",
 
  "OUTPUT_PREFIX":      "products/processed",
  "OUTPUT_FILE_NAME":   "catalog.csv"
}



import io
import json
import logging
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pandas as pd
import requests

# ── Glue import with local / test fallback ────────────────────────────────────
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

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  [%(levelname)s]  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("first_etl")


# ═════════════════════════════════════════════════════════════════════════════
# 1 ▸ Config
# ═════════════════════════════════════════════════════════════════════════════

def get_args() -> dict:
    """Return resolved Glue job arguments."""
    return getResolvedOptions(sys.argv, ["CONFIG_PATH"])


def load_config(config_path: str) -> dict:
    """
    Download and parse a JSON config file stored in S3.

    Args:
        config_path: S3 URI — e.g. ``s3://my-bucket/glue/config.json``

    Returns:
        Parsed configuration dictionary.

    Raises:
        botocore.exceptions.ClientError: Object not found or access denied.
        json.JSONDecodeError: File is not valid JSON.
    """
    logger.info("Loading config from %s", config_path)

    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key    = parsed.path.lstrip("/")

    s3       = boto3.client("s3")
    response = s3.get_object(Bucket=bucket, Key=key)
    config   = json.loads(response["Body"].read().decode("utf-8"))

    logger.info("Config loaded — keys: %s", list(config.keys()))
    return config


# ═════════════════════════════════════════════════════════════════════════════
# 2 ▸ Extract
# ═════════════════════════════════════════════════════════════════════════════

def fetch_products(
    api_url: str,
    headers: dict | None = None,
    params:  dict | None = None,
) -> list[dict]:
    """
    Fetch product records from the FakeStore REST API.

    Each record looks like:
    {
      "id": 1, "title": "Backpack", "price": 109.95,
      "category": "men's clothing",
      "rating": { "rate": 3.9, "count": 120 }
    }

    Args:
        api_url: Full endpoint URL.
        headers: Optional HTTP headers.
        params:  Optional query-string parameters (e.g. ``{"limit": 20}``).

    Returns:
        List of raw product dicts.

    Raises:
        requests.HTTPError: Non-2xx response.
        requests.Timeout:   Request exceeded 30 s.
    """
    logger.info("Fetching products from %s", api_url)

    response = requests.get(
        api_url,
        headers=headers or {},
        params=params  or {},
        timeout=30,
    )
    response.raise_for_status()

    products = response.json()
    logger.info("Fetched %d products.", len(products))
    return products


# ═════════════════════════════════════════════════════════════════════════════
# 3 ▸ Transform
# ═════════════════════════════════════════════════════════════════════════════

def _price_tier(price: float) -> str:
    """Classify a product price into a business tier."""
    if price < 20:
        return "budget"
    if price <= 100:
        return "mid-range"
    return "premium"


def _rating_label(rate: float) -> str:
    """Classify a product rating into a human-readable label."""
    if rate < 3.0:
        return "low"
    if rate < 4.0:
        return "medium"
    return "high"


def transform_products(df: pd.DataFrame) -> pd.DataFrame:
    """
    Enrich and clean the raw product catalog DataFrame.

    Input columns (after ``pd.json_normalize``):
        id, title, price, description, category, image,
        rating.rate, rating.count

    Transformations applied
    ───────────────────────
    1. Drop rows missing any of:  price, category, rating.rate, rating.count
    2. Rename: ``id`` → ``product_id``,
               ``rating.rate`` → ``rating_rate``,
               ``rating.count`` → ``rating_count``
    3. Normalise ``category`` — strip whitespace + lower-case
    4. Add ``price_tier``   — "budget" / "mid-range" / "premium"
    5. Add ``rating_label`` — "low" / "medium" / "high"
    6. Drop the ``image`` column  (not useful for analytics)
    7. Add ``ingestion_timestamp`` (ISO-8601, UTC)

    Args:
        df: Flat DataFrame produced by ``pd.json_normalize(raw_products)``.

    Returns:
        Enriched DataFrame with a reset integer index.
    """
    logger.info("Transforming %d product records …", len(df))

    # 1 — Drop rows missing critical fields
    critical = [c for c in ("price", "category", "rating.rate", "rating.count") if c in df.columns]
    before = len(df)
    df = df.dropna(subset=critical)
    logger.info("  Dropped %d rows with missing critical fields.", before - len(df))

    # 2 — Rename columns to snake_case / business names
    df = df.rename(columns={
        "id":           "product_id",
        "rating.rate":  "rating_rate",
        "rating.count": "rating_count",
    })

    # 3 — Normalise category
    if "category" in df.columns:
        df["category"] = df["category"].str.strip().str.lower()

    # 4 — Price tier
    if "price" in df.columns:
        df["price_tier"] = df["price"].apply(_price_tier)

    # 5 — Rating label
    if "rating_rate" in df.columns:
        df["rating_label"] = df["rating_rate"].apply(_rating_label)

    # 6 — Drop image URL (not needed for analytics)
    if "image" in df.columns:
        df = df.drop(columns=["image"])

    # 7 — Ingestion timestamp
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    logger.info("Transformation done — output shape: %s", df.shape)
    return df.reset_index(drop=True)


# ═════════════════════════════════════════════════════════════════════════════
# 4 ▸ Load
# ═════════════════════════════════════════════════════════════════════════════

def save_to_s3(df: pd.DataFrame, bucket: str, key: str) -> str:
    """
    Serialise *df* to CSV (in-memory) and upload it to S3.

    Args:
        df:     DataFrame to persist.
        bucket: Destination S3 bucket name.
        key:    Destination object key inside the bucket.

    Returns:
        Full ``s3://bucket/key`` URI of the created object.
    """
    s3_uri = f"s3://{bucket}/{key}"
    logger.info("Uploading %d rows to %s …", len(df), s3_uri)

    buf = io.StringIO()
    df.to_csv(buf, index=False)

    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=buf.getvalue().encode("utf-8"),
        ContentType="text/csv",
    )

    logger.info("Upload complete → %s", s3_uri)
    return s3_uri


# ═════════════════════════════════════════════════════════════════════════════
# Orchestrator
# ═════════════════════════════════════════════════════════════════════════════

def main() -> None:
    """Entry point — orchestrates the Product Catalog ETL pipeline."""
    logger.info("══ Product Catalog ETL — START ══")

    # Config
    args   = get_args()
    config = load_config(args["CONFIG_PATH"])

    api_url       = config["API_URL"]
    api_headers   = config.get("API_HEADERS", {})
    api_params    = config.get("API_PARAMS", {})
    output_bucket = config["OUTPUT_BUCKET_NAME"]
    output_prefix = config.get("OUTPUT_PREFIX", "products/processed").rstrip("/")
    output_file   = config.get("OUTPUT_FILE_NAME", "catalog.csv")
    output_key    = f"{output_prefix}/{output_file}"

    # Extract
    raw_products = fetch_products(api_url, headers=api_headers, params=api_params)
    df_raw = pd.json_normalize(raw_products)          # flattens nested rating dict
    logger.info("Raw DataFrame shape: %s", df_raw.shape)

    # Transform
    df_clean = transform_products(df_raw)
    logger.info("Transformed DataFrame shape: %s", df_clean.shape)

    # Load
    uri = save_to_s3(df_clean, output_bucket, output_key)

    logger.info("══ Product Catalog ETL — DONE ══  →  %s", uri)


if __name__ == "__main__":
    main()
