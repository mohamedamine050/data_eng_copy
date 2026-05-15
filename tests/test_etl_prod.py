import json
import os
import sys
from unittest.mock import patch

import boto3
import pandas as pd
import pytest
import requests
from moto import mock_aws

# On importe les fonctions du script d'origine
# Note : Ajuste le nom de l'import si ton fichier ne s'appelle pas exactement 'first_etl'

from src.jobs.etl_products import main
from src.jobs.etl_products import (
    _price_tier,
    _rating_label,
    fetch_products,
    get_args,
    load_config,
    save_to_s3,
    transform_products,
)


# ═════════════════════════════════════════════════════════════════════════════
# 1 ▸ Tests de Configuration
# ═════════════════════════════════════════════════════════════════════════════

def test_get_args():
    """Vérifie que get_args récupère bien l'argument --CONFIG_PATH."""
    test_args = ["first_etl.py", "--CONFIG_PATH", "s3://my-bucket/config.json"]
    with patch.object(sys, "argv", test_args):
        args = get_args()
        assert args["CONFIG_PATH"] == "s3://my-bucket/config.json"


@mock_aws
def test_load_config_success():
    """Vérifie le chargement et le parsing d'un JSON valide depuis S3."""
    # Configuration du mock S3
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="my-bucket")
    
    mock_config = {"API_URL": "https://fake.com", "OUTPUT_BUCKET_NAME": "dest-bucket"}
    s3.put_object(
        Bucket="my-bucket",
        Key="glue/config.json",
        Body=json.dumps(mock_config)
    )

    # Appel de la fonction
    config = load_config("s3://my-bucket/glue/config.json")
    assert config == mock_config


# ═════════════════════════════════════════════════════════════════════════════
# 2 ▸ Tests d'Extraction (API)
# ═════════════════════════════════════════════════════════════════════════════

def test_fetch_products_success(requests_mock):
    """Vérifie que fetch_products retourne bien les données JSON en cas de succès HTTP 200."""
    mock_url = "https://fakestoreapi.com/products"
    mock_response = [{"id": 1, "title": "Product A", "price": 10.0}]
    
    requests_mock.get(mock_url, json=mock_response, status_code=200)

    result = fetch_products(mock_url)
    assert result == mock_response


def test_fetch_products_failure(requests_mock):
    """Vérifie que la fonction lève une exception en cas d'erreur HTTP."""
    mock_url = "https://fakestoreapi.com/products"
    requests_mock.get(mock_url, status_code=500)

    with pytest.raises(requests.exceptions.HTTPError):
        fetch_products(mock_url)


# ═════════════════════════════════════════════════════════════════════════════
# 3 ▸ Tests de Transformation (Pandas)
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.parametrize(
    "price,expected_tier",
    [
        (10.0, "budget"),
        (20.0, "mid-range"),
        (50.0, "mid-range"),
        (100.0, "mid-range"),
        (100.01, "premium"),
    ],
)
def test_price_tier(price, expected_tier):
    """Vérifie les règles métiers de catégorisation des prix."""
    assert _price_tier(price) == expected_tier


@pytest.mark.parametrize(
    "rate,expected_label",
    [
        (2.5, "low"),
        (3.0, "medium"),
        (3.9, "medium"),
        (4.0, "high"),
        (5.0, "high"),
    ],
)
def test_rating_label(rate, expected_label):
    """Vérifie les règles métiers de labellisation des notes."""
    assert _rating_label(rate) == expected_label


def test_transform_products():
    """Teste le pipeline complet de transformation Pandas (Nettoyage, Enrichessement, Renommage)."""
    # Données d'entrée brutes simulées (après pd.json_normalize)
    raw_data = [
        {
            "id": 1,
            "title": "  Boots  ",
            "price": 120.0,
            "category": " Men's Clothing ",
            "image": "http://img.com/1.jpg",
            "rating.rate": 4.5,
            "rating.count": 99,
        },
        {
            "id": 2,
            "title": "Socks",
            "price": 5.0,
            # Ligne qui devrait sauter (Pas de category ni de rating)
            "category": None, 
            "image": "http://img.com/2.jpg",
            "rating.rate": None,
            "rating.count": None,
        },
    ]
    df_in = pd.DataFrame(raw_data)

    # Exécution
    df_out = transform_products(df_in)

    # 1. Vérification du drop (la ligne 2 est invalide)
    assert len(df_out) == 1

    # 2. Vérification des renommages
    assert "product_id" in df_out.columns
    assert "rating_rate" in df_out.columns
    assert "rating_count" in df_out.columns
    assert "image" not in df_out.columns  # Doit être supprimé

    # 3. Vérification de la normalisation des chaînes
    assert df_out.loc[0, "category"] == "men's clothing"

    # 4. Vérification des nouvelles colonnes calculées
    assert df_out.loc[0, "price_tier"] == "premium"
    assert df_out.loc[0, "rating_label"] == "high"
    assert "ingestion_timestamp" in df_out.columns


# ═════════════════════════════════════════════════════════════════════════════
# 4 ▸ Tests de Chargement (S3)
# ═════════════════════════════════════════════════════════════════════════════

@mock_aws
def test_save_to_s3():
    """Vérifie que le DataFrame est correctement sérialisé en CSV et poussé sur S3."""
    bucket_name = "analytics-bucket"
    file_key = "outputs/catalog.csv"
    
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=bucket_name)

    df = pd.DataFrame([{"product_id": 1, "price": 10.0}])

    # Appel
    uri = save_to_s3(df, bucket_name, file_key)

    # Vérifications
    assert uri == f"s3://{bucket_name}/{file_key}"
    
    # On récupère l'objet directement sur notre faux S3 pour inspecter le contenu
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    csv_content = response["Body"].read().decode("utf-8")
    
    assert "product_id,price" in csv_content
    assert "1,10.0" in csv_content


# ═════════════════════════════════════════════════════════════════════════════
# 5 ▸ Test d'Orchestration (Main)
# ═════════════════════════════════════════════════════════════════════════════

@mock_aws
def test_main_pipeline(requests_mock):
    """Test d'intégration global : simule l'exécution complète du main()."""
    # 1. Mock des arguments sys.argv
    test_args = ["first_etl.py", "--CONFIG_PATH", "s3://config-bkt/etl.json"]
    
    # 2. Mock de S3 pour la config de départ
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket="config-bkt")
    s3.create_bucket(Bucket="ecommerce-data-lake") # Seconde boîte cible de l'ETL
    
    mock_config = {
        "API_URL": "https://fakestoreapi.com/products",
        "OUTPUT_BUCKET_NAME": "ecommerce-data-lake",
        "OUTPUT_PREFIX": "products/processed",
        "OUTPUT_FILE_NAME": "catalog.csv"
    }
    s3.put_object(Bucket="config-bkt", Key="etl.json", Body=json.dumps(mock_config))

    # 3. Mock de l'API HTTP externe
    mock_api_response = [
        {
            "id": 99,
            "title": "Mock Product",
            "price": 45.0,
            "category": "electronics",
            "rating": {"rate": 4.1, "count": 10}
        }
    ]
    requests_mock.get("https://fakestoreapi.com/products", json=mock_api_response)

    # 4. Exécution globale en patchant les arguments system
    with patch.object(sys, "argv", test_args):
        first_etl.main()

    # 5. Contrôle final : Le fichier CSV a-t-il atterri dans le bucket cible ?
    obj = s3.get_object(Bucket="ecommerce-data-lake", Key="products/processed/catalog.csv")
    csv_string = obj["Body"].read().decode("utf-8")
    
    assert "product_id,title,price,category,rating_rate,rating_count,price_tier,rating_label" in csv_string
    assert "99,Mock Product,45.0,electronics,4.1,10,mid-range,high" in csv_string
