import pytest
from pyspark.sql import SparkSession
from src.silver_cleaning import clean_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

def test_clean_data_logic(spark):
    
    data = [("ORD1", "C1", -10.0, 1, "shipped", "2026-01-01")] 
    cols = ["order_id", "customer_id", "price", "quantity", "status", "timestamp"]
    df = spark.createDataFrame(data, cols)

    
    result = clean_data(df)

    
    assert result.count() == 0