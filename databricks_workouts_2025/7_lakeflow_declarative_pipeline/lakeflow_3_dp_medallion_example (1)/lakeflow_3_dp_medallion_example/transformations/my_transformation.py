from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table(name="catalog2_we47.default.drugstbl_medal_bronze3")
def drugstbl_bronze_tbl():
    return spark.read.table("lakehousecat.deltadb.drugstbl")#requirement is to collect ins/upd/deleted data and merge to bronze table
    #return spark.readStream.table("lakehousecat.deltadb.drugstbl")#requirement is to collect only ins data and merge to bronze table

#This is equivalent to spark createOrReplaceTempView("drugstbl_silverview2")
@dp.view(name="drugstbl_silverview2")#We use view very specifically, if we don't need the data to replicated in silver layer also (because in this project no one is going to use this silver data)
@dp.expect_all({"rating_is_valid":"rating <= 10","nonnull_drug":"drugname IS NOT NULL"}) #even if one or both expectation is not met data is loaded
#@dp.expect_all_or_fail({"rating_is_valid":"rating <= 10","nonnull_drug":"drugname IS NOT NULL"}) #job fails if one or both expectation are not met and returns the error
#@dp.expect_all_or_drop({"rating_is_valid":"rating <= 10","nonnull_drug":"drugname IS NOT NULL"}) #even if one or both expectation are not met data is dropped
@dp.expect_all({"usefulcount_is_positive": "usefulcount > 1"}) #even if expectation is not met data is loaded
def drugstbl_silvernewtbl():
    return spark.read.table("catalog2_we47.default.drugstbl_medal_bronze3")

@dp.materialized_view(name="catalog2_we47.default.drugstbl_medal_gold_mv2")
def drugstbl_medal_gold():
    df = spark.read.table("drugstbl_silverview2")
    return (
        df.groupBy("drugname")
        .agg(F.sum("usefulcount").alias("sum_usefulcount"),
             F.avg("rating").alias("avg_rating")))
