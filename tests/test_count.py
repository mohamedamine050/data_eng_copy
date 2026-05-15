import pytest
from your_script import process_data, COUNTRIES_SCHEMA
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("Tests").getOrCreate()

def test_process_data_filtering(spark):
    # GIVEN: Un jeu de données avec un pays invalide (population <= 0)
    data = [
        {"name": "France", "region": "Europe", "population": 67000000, "area_km2": 550000.0, "capital": "Paris", "independent": True},
        {"name": "Ghost Island", "region": "Oceania", "population": 0, "area_km2": 10.0, "capital": None, "independent": False}
    ]
    
    # WHEN: On applique la transformation
    df_detail, df_summary = process_data(spark, data)
    
    # THEN: Il ne reste qu'un seul pays
    assert df_detail.count() == 1
    assert df_detail.collect()[0]["name"] == "France"

def test_aggregation_logic(spark):
    # GIVEN: Deux pays dans la même région
    data = [
        {"name": "A", "region": "Test", "population": 100, "area_km2": 10.0, "capital": "X", "independent": True},
        {"name": "B", "region": "Test", "population": 200, "area_km2": 20.0, "capital": "Y", "independent": True}
    ]
    
    # WHEN
    _, df_summary = process_data(spark, data)
    res = df_summary.collect()[0]
    
    # THEN
    assert res["nb_countries"] == 2
    assert res["total_population"] == 300
