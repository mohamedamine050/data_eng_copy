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
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("etl_csv_to_rds")

# --- COMPATIBILITÉ GLUE ---
try:
    from awsglue.utils import getResolvedOptions
except ImportError:
    # Fallback pour exécution locale/pytest
    def getResolvedOptions(argv, options):
        import argparse
        parser = argparse.ArgumentParser()
        for opt in options:
            parser.add_argument(f"--{opt}", required=False)
        args, _ = parser.parse_known_args(argv[1:])
        return {opt: getattr(args, opt) for opt in options if getattr(args, opt) is not None}

# --- LOGIQUE ETL ---

def get_args() -> dict:
    """Récupère les arguments de la ligne de commande ou de Glue."""
    return getResolvedOptions(sys.argv, ["CONFIG_PATH"])

def load_config(config_path: str) -> dict:
    """Charge le JSON de configuration depuis S3."""
    logger.info(f"Loading config from {config_path}")
    parsed = urlparse(config_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")

    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(resp["Body"].read().decode("utf-8"))

def read_csv_from_s3(bucket: str, key: str) -> pd.DataFrame:
    """Extrait un CSV de S3 vers un DataFrame Pandas."""
    logger.info(f"Reading CSV from s3://{bucket}/{key}")
    s3 = boto3.client("s3")
    resp = s3.get_object(Bucket=bucket, Key=key)
    return pd.read_csv(io.BytesIO(resp["Body"].read()))

def transform(df: pd.DataFrame) -> pd.DataFrame:
    """Nettoie et transforme les données."""
    if df.empty:
        return df

    # 1. Nettoyage des noms de colonnes
    df.columns = (
        df.columns.str.strip().str.lower()
        .str.replace(r"\s+", "_", regex=True)
        .str.replace(r"[^\w]", "_", regex=True)
    )

    # 2. Suppression des lignes vides
    df = df.dropna(how="all")

    # 3. Nettoyage des espaces dans les chaînes (Correction Warning Pandas 4)
    # On cible explicitement 'object' et 'string'
    str_cols = df.select_dtypes(include=["object", "string"]).columns
    for col in str_cols:
        df[col] = df[col].astype(str).str.strip()

    # 4. Horodatage d'ingestion
    df["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()

    return df.reset_index(drop=True)

def get_rds_engine(config: dict) -> sqlalchemy.engine.Engine:
    """Crée l'engine SQLAlchemy pour RDS."""
    host = config["DB_HOST"].split(":")[0]  # Sécurité si port inclus dans host
    port = config.get("DB_PORT", "5432")
    url = (
        f"postgresql+psycopg2://{config['DB_USER']}:{config['DB_PASSWORD']}"
        f"@{host}:{port}/{config['DB_NAME']}"
    )
    return sqlalchemy.create_engine(url, connect_args={"connect_timeout": 10})

def load_to_rds(df: pd.DataFrame, engine, table: str) -> int:
    """Charge le DataFrame dans la table RDS."""
    logger.info(f"Loading {len(df)} rows into table: {table}")
    df.to_sql(name=table, con=engine, if_exists="append", index=False, method="multi")
    return len(df)

def main():
    logger.info("START ETL PROCESS")
    args = get_args()
    
    if not args.get("CONFIG_PATH"):
        logger.error("Missing --CONFIG_PATH argument")
        sys.exit(1)

    config = load_config(args["CONFIG_PATH"])
    
    # Extraction
    df_raw = read_csv_from_s3(config["INPUT_BUCKET_NAME"], config["INPUT_KEY_NAME"])
    
    # Transformation
    df_clean = transform(df_raw)
    
    # Chargement
    engine = get_rds_engine(config)
    table_name = config.get("DB_TABLE", "etl_output")
    load_to_rds(df_clean, engine, table_name)
    
    logger.info("ETL PROCESS COMPLETED SUCCESSFULLY")

if __name__ == "__main__":
    main()
