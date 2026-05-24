# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()
print(CATALOG)
print(SCHEMA)

# COMMAND ----------

import json

config_nb_output = dbutils.notebook.run(
    "/Workspace/Users/suganyadevi2803@gmail.com/databricks-code-repo/databricks_workouts_2025/4_logistics_usecase/generic_project/general_conf_utils_1_2/configs_path1",
    120,{"catalog": CATALOG,"schema": SCHEMA})

config_dict = json.loads(config_nb_output)

BRONZE = config_dict["BRONZE"]
SILVER = config_dict["SILVER"]
SILVERDB = config_dict["SILVERDB"]
print(BRONZE)
print(SILVER)

# COMMAND ----------

# MAGIC %run /Workspace/Users/suganyadevi2803@gmail.com/databricks-code-repo/databricks_workouts_2025/4_logistics_usecase/generic_project/general_conf_utils_1_2/util_functions2

# COMMAND ----------

#Adapting Generic Framework
spark=get_spark_session("Logistics Data Engineering Project")

# COMMAND ----------

# DBTITLE 1,Untitled
#staff = spark.read.format("delta").load(f"{BRONZE}/staff")#inline clumsy code 
staff=read_delta_df(spark,f"{BRONZE}/staff")
geotag=read_delta_df(spark,f"{BRONZE}/geotag")
shipments = read_delta_df(spark,f"{BRONZE}/shipments")


# COMMAND ----------

silver_staff = standardize_staff(staff)

silver_geotag=scrub_geotag(geotag).distinct()

#transform is a special spark function to help us transform a datframe applying some function to it.
silver_filtered_shipment=shipments.where("shipment_weight_kg>0")
silver_shipments = (silver_filtered_shipment
    .transform(standardize_shipments)
    .transform(enrich_shipments)
    .transform(split_columns))

'''
#or we can rewrite the transform in a traditional way as given below
filterdf=shipments.where("shipment_weight_kg>0")
standardizedf=standardize_shipments(filterdf)
enricheddf=enrich_shipments(standardizedf)
silver_shipments=split_columns(enricheddf)
'''


# COMMAND ----------

#Writing silver data to tables is more efficient and better standard than writing to files, ,just for learning purpose we are writing to file also...
write_file(silver_staff,f"{SILVER}/staff",mode="overwrite",format="delta")
write_file(silver_geotag,f"{SILVER}/geotag",mode="overwrite",format="delta")
write_file(silver_shipments,f"{SILVER}/shipments",mode="overwrite",format="delta")

write_table(silver_staff,f"{SILVERDB}.silver_staff", mode="overwrite")
write_table(silver_geotag,f"{SILVERDB}.silver_geotag", mode="overwrite")
write_table(silver_shipments,f"{SILVERDB}.silver_shipments", mode="overwrite")

# COMMAND ----------

# MAGIC %sql
# MAGIC --drop table catalog2_we47.schema2_we47.silver_shipments;

# COMMAND ----------

'''
spark.sql(f"""create table {SILVERDB}.silver_shipments_liquid
using delta
cluster by (shipment_id) as
(
select * from {SILVERDB}.silver_shipments
)
;""")
'''

# COMMAND ----------

# MAGIC %sql
# MAGIC --describe detail catalog2_we47.schema2_we47.silver_shipments_liquid;

# COMMAND ----------

#display(spark.sql("select * from catalog2_we47.schema2_we47.silver_shipments_liquid;"))
