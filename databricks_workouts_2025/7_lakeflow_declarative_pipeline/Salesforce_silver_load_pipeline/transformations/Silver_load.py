from pyspark import pipelines as dp
from pyspark.sql.functions import lit

@dp.table()
def salesforce_silver_table():
    df=spark.readStream.table("catalog1_dropme.default.account")
    df1=df.withColumn("system",lit("salesforce"))
    return df1