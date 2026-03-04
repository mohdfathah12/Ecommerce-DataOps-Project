from pyspark.sql import functions as F
def clean_data(df):
    cleaned_df = df \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["order_id"]) \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("status", F.lower(F.col("status")))
    return cleaned_df



