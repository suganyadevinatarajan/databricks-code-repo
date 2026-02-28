from pyspark import pipelines as dp
#We are modernizing our Imperative Pipeline to Declarative
#We can load the entire data using (Run Pipeline with Full table Refresh) into bronze/silver/gold completely by truncating the target tables only for the first time to load all backlog/historical data present in the source cloud path
#We are using Lakeflow Ingestion (Auto Loader) - Load Incremental files from the cloud storage going forward
@dp.table(name="bronze_staff_data1")
def bronze_staff_data():
    # Fetch the base path from configuration
    #This config setting is made automatically when we go to settings -> configuration -> add key(base_path) & value(some cloud path) pairs-> behind the scene it will run spark.conf.set("base_path", "some cloud path")
    base_path = spark.conf.get("base_path")
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
            .load(f"{base_path}/staff")) 

@dp.table(name="bronze_geotag_data1")
def bronze_geotag_data():
    base_path = spark.conf.get("base_path")    
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("inferColumnTypes", "true")
            .load(f"{base_path}/geotag"))

@dp.table(name="bronze_shipments_data1")
def bronze_shipments_data():
    base_path = spark.conf.get("base_path")    
    return (spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "json")
            .option("inferColumnTypes", "true")
            .option("multiLine", "true")
            .load(f"{base_path}/shipment/")
            .select(
                "shipment_id", "order_id", "source_city", "destination_city",
                "shipment_status", "cargo_type", "vehicle_type", "payment_mode",
                "shipment_weight_kg", "shipment_cost", "shipment_date"
            ))
'''
Parameterizing the base_path
Option A: In the Pipeline Settings (Persistent)
If this path rarely changes, set it directly in the DLT Pipeline definition.
Open your Pipeline in Databricks.
Click Settings.
Under Advanced, find the Configuration section.
Add a new Key-Value pair:
Key: base_path
Value: /Volumes/catalog1_we47/schema1_we47/datalake

Option B: In the Lakeflow Job (Dynamic/Overridable)
If you want to reuse this pipeline code for different environments (e.g., Dev vs. Prod paths) triggered by a Job:
Go to your Databricks Job (Lakeflow Job).
Select the Task that runs this Pipeline.
Look for the Parameters or Configuration override section (specifically for DLT tasks).
Enter the configuration JSON or Key/Value pairs:
{
  "source.base_path": "/Volumes/catalog1_we47/schema1_we47/datalake"
}
'''