import pytest
from pyspark.sql import SparkSession
from src.silver_cleaning import clean_data

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("unit-tests").getOrCreate()

def test_clean_data_logic(spark):
    # നമുക്ക് ടെസ്റ്റ് ചെയ്യാൻ ഒരു ചെറിയ ഡാറ്റ ഉണ്ടാക്കാം
    data = [("ORD1", "C1", -10.0, 1, "shipped", "2026-01-01")] # വില നെഗറ്റീവ് ആണ്
    cols = ["order_id", "customer_id", "price", "quantity", "status", "timestamp"]
    df = spark.createDataFrame(data, cols)

    # ലോജിക് റൺ ചെയ്യുന്നു
    result = clean_data(df)

    # നെഗറ്റീവ് വില ഉള്ളതുകൊണ്ട് റിസൾട്ട് 0 ആയിരിക്കണം (കാരണം നമ്മൾ സിൽവറിൽ filter വെച്ചിട്ടുണ്ട്)
    assert result.count() == 0