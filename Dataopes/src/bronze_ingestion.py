from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
def ingest_bronze(spark):
    data = [
        ("ORD101", 1001, "PROD-A", 2, 250.50, "2026-02-28T10:00:00", "completed"),
        ("ORD102", 1002, "PROD-B", 1, 120.00, "2026-02-28T10:05:00", "pending"),
        ("ORD103", 1003, "PROD-C", 5, 450.75, "2026-02-28T10:10:00", "completed")
    ]

    schema = StructType([
        StructField("order_id", StringType(), True),
        StructField("customer_id", IntegerType(), True),
        StructField("product_id", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", StringType(), True),
        StructField("status", StringType(), True)
    ])
    df = spark.createDataFrame(data, schema= schema)
    df.write.format("delta").mode("overwrite").saveAsTable("fmcg.default.ecommerce_bronze")
    return df


