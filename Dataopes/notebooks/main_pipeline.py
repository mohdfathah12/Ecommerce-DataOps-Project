import sys
import os
sys.path.append(os.path.abspath('/Workspace/Repos/mohdfathah12@gmail.com/Ecommerce-DataOps-Project/src'))

from bronze_ingestion import ingest_bronze
from silver_cleaning import clean_data
from gold_analytics import create_gold_report

bronze_df = ingest_bronze(spark)

silver_df = clean_data(bronze_df)
silver_df.write.format("delta").mode("overwrite").saveAsTable("fmcg.default.ecommerce_silver")

gold_df = create_gold_report(silver_df)
gold_df.write.format("delta").mode("overwrite").saveAsTable("fmcg.default.ecommerce_gold")

print("End-to-End DataOps Pipeline Successful!")
display(spark.table("fmcg.default.ecommerce_gold"))

