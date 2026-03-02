from pyspark import pipelines as dp
from pyspark.sql.functions import *
@dp.table(name="catalog2_we47.schema2_we47.silver_shipments_dlt1")
def load_silver():
    return spark.read.table("catalog2_we47.schema2_we47.bronze_shipments1").withColumn("loadts", current_timestamp())