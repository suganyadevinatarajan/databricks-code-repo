# Databricks notebook source
# MAGIC %md
# MAGIC This notebook is going do 2 activities
# MAGIC 1. Once for all - Create all necessary path and table components needed for this project such as <br>
# MAGIC Catalog(parameter) -> Schema(parameter) -> Volume (sourcedatalake ,bronze ,silver ,gold).
# MAGIC 2. Defining all configuration parameters such as volume path, table information etc.,  once for all, rather than we hardcode all these in every notebook

# COMMAND ----------

dbutils.widgets.text("catalog","prodcatalogdefault")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","prodschemadefault")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

#print({CATALOG})

# COMMAND ----------

# DBTITLE 1,Cell 2
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA};")

# COMMAND ----------

spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.datalake;""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.bronze;""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.silver;""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.gold;""")

# COMMAND ----------

SRC=f"/Volumes/{CATALOG}/{SCHEMA}/datalake"
#print(SRC)
BRONZE = f"/Volumes/{CATALOG}/{SCHEMA}/bronze"
SILVER = f"/Volumes/{CATALOG}/{SCHEMA}/silver"
GOLD   = f"/Volumes/{CATALOG}/{SCHEMA}/gold"
SILVERDB= f"{CATALOG}.{SCHEMA}"
GOLDDB   = f"{CATALOG}.{SCHEMA}"
print("source datalake location(datalake)",SRC)
print("bronze datalake location(datalake)",BRONZE)
print("silver database (lakehouse)",SILVERDB)
print("gold database(lakehouse)",GOLDDB)

# COMMAND ----------

import json
dbutils.notebook.exit(
    json.dumps({
        "CATALOG": CATALOG,
        "SCHEMA": SCHEMA,
        "SRC": SRC,
        "BRONZE": BRONZE,
        "SILVER": SILVER,
        "GOLD": GOLD,
        "SILVERDB": SILVERDB,
        "GOLDDB": GOLDDB
    })
)

