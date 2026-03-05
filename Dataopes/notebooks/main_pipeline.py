import sys
import os
from pyspark.sql import SparkSession

current_dir = os.getcwd()
parent_dir = os.path.dirname(current_dir)
src_path = os.path.join(parent_dir, "src")

if src_path not in sys.path:
    sys.path.append(src_path)
    print(f"Added to path: {src_path}")

try:
    from bronze_ingestion import ingest_bronze
    from silver_cleaning import clean_data
    from gold_analytics import create_gold_report

    print("Success: Modules imported!")
except ModuleNotFoundError:
    root_dir = os.path.dirname(parent_dir)
    src_path_alt = os.path.join(root_dir, "src")
    
    sys.path.append(src_path_alt)

    from bronze_ingestion import ingest_bronze
    from silver_cleaning import clean_data
    from gold_analytics import create_gold_report

    print(f"Success: Modules imported from alternative path: {src_path_alt}")

spark = SparkSession.builder.appName("MedallionPipeline").getOrCreate()


try:

    print("Starting Bronze Ingestion...")

    df_bronze = ingest_bronze(spark)


    print("Starting Silver Cleaning...")
    df_silver = clean_data(df_bronze)
    df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_orders")


    print("Starting Gold Analytics...")
    df_gold = create_gold_report(df_silver)
    df_gold.write.format("delta").mode("overwrite").saveAsTable("gold_order_metrics")

    print(" SUCCESS: All layers (Bronze, Silver, Gold) processed!")

except Exception as e:
    print(f" ERROR: Pipeline failed due to: {e}")