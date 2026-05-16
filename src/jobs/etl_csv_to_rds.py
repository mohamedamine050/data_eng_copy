import io
import json
import logging
import sys
from datetime import datetime, timezone
from urllib.parse import urlparse

import boto3
import pandas as pd
import sqlalchemy

# Configuration du logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("etl_csv_to_rds")

# --- COMPATIBILITÉ GLUE / LOCAL ---
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    def getResolvedOptions(argv, options):
        import argparse
        parser = argparse.ArgumentParser()
        for opt in options:
            parser.add_argument(f"--{opt}", required=False)
        args, _ = parser.parse_known_args(argv[1:])
        return {opt: getattr(args, opt) for opt in options if getattr(args, opt) is not None}

# --- LOGIQUE ETL ---

def get_args() -> dict:
    """Récupère les arguments (--CONFIG_PATH)."""
    return getResolvedOptions(sys.argv, ["CONFIG_PATH"])

def load_config(config_path: str) -> dict:
    """Charge le JSON depuis S3."""
    logger.info(f"Loading config from {config_path}")
    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Lecture du CSV S3."""
    logger.info(f"Reading s3://{bucket}/{key}")
    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(resp["Body"].read()))

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoyage et transformation."""
    if df.empty:
        return df

    # 1. Nettoyage colonnes
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # 2. Strings strip (Fix Pandas Warning)
    str_cols = df.select_dtypes(include=["object"]).columns
    df[str_cols] = df[str_cols].astype(str).apply(lambda x: x.str.strip())

    # 3. Timestamp
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    return df.reset_index(drop=True)

def get_rds_engine(config: dict):
    """Moteur SQLAlchemy."""
    host = config["DB_HOST"].split(":")[0]
    port = config.get("DB_PORT", "5432")
    url = (
        f"postgresql+psycopg2://{config['DB_USER']}:{config['DB_PASSWORD']}"
        f"@{host}:{port}/{config['DB_NAME']}"
    )
    return sqlalchemy.create_engine(url, connect_args={"connect_timeout": 10})

def load_to_rds(df: pd.DataFrame, engine, table: str):
    """Chargement final."""
    logger.info(f"Loading {len(df)} rows into {table}")
    df.to_sql(name=table, con=engine, if_exists="append", index=False, method="multi")
    return len(df)

def main():
    logger.info("START")
    args = get_args()
    conf_path = args.get("CONFIG_PATH")
    
    if not conf_path:
        logger.error("No CONFIG_PATH provided")
        return

    config = load_config(conf_path)
    
    df = read_csv_from_s3(config["INPUT_BUCKET_NAME"], config["INPUT_KEY_NAME"])
    df_clean = transform(df)
    
    engine = get_rds_engine(config)
    load_to_rds(df_clean, engine, config.get("DB_TABLE", "etl_output"))
    logger.info("DONE")

if __name__ == "__main__":
    main()
