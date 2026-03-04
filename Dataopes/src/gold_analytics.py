from pyspark.sql import functions as F
def create_gold_report(df):
    report = df.groupBy("status").agg(F.count("order_id").alias("total_orders"),
                                      F.sum("price").alias("total_revenue"))
    return report 