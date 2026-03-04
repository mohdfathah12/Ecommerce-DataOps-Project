from pyspark.sql import functions as F
def clean_data(df):
    cleaned_df = df \
    .dropDuplicates(["order_id"]) \
    .dropna(subset=["order_id"]) \
    .withColumn("timestamp", F.to_timestamp("timestamp")) \
    .withColumn("price", F.col("price").cast("double")) \
    .withColumn("status", F.lower(F.col("status")))

    final_df = cleaned_df \
    .filter(F.col("price") > 0) \
    .filter(F.col("quantity") > 0)
    print (f"Silver Layer Cleaning: Filtered out bad records. Final row count:{final_df.count()}")
    return final_df





