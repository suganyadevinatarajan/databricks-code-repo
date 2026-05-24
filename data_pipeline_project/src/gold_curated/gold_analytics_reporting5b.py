# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

import json

config_nb_output = dbutils.notebook.run(
    "/Workspace/Users/suganyadevi2803@gmail.com/databricks-code-repo/databricks_workouts_2025/4_logistics_usecase/generic_project/general_conf_utils_1_2/configs_path1",
    120,{"catalog": CATALOG,"schema": SCHEMA})

config_dict = json.loads(config_nb_output)

CATALOG = config_dict["CATALOG"]
SCHEMA = config_dict["SCHEMA"]
SRC=config_dict["SRC"]
BRONZE = config_dict["BRONZE"]
SILVER = config_dict["SILVER"]
GOLD = config_dict["GOLD"]
SILVERDB = config_dict["SILVERDB"]
GOLDDB = config_dict["GOLDDB"]

# COMMAND ----------

# MAGIC %run /Workspace/Users/suganyadevi2803@gmail.com/databricks-code-repo/databricks_workouts_2025/4_logistics_usecase/generic_project/general_conf_utils_1_2/util_functions2

# COMMAND ----------

# CUBE AGGREGATION
cube_df = df.cube("origin_hub_city") \
    .agg(F.sum("shipment_cost").alias("total_cost"))

write_file(top3,f"{GOLD}/top3_drivers")
write_file(lag_df,f"{GOLD}/prev_shipment_days")
write_file(cube_df,f"{GOLD}/cube_costs")
