--Incremental data ingestion/load of newly added/updated data is insert
--Extraction - We are doing Change Data Capture (CDC) (Inserted/updated)
--Load - We are doing Slowly Changing Dimension Type 2
/*INSERT INTO catalog2_we47.schema2_we47.bronze_shipments1
SELECT
  shipment_id,
  first_name,
  last_name,
  age,
  role,
  updated_at
FROM gcp_mysql_fc_wd36.logistics.shipments1
WHERE updated_at >
(
  SELECT COALESCE(MAX(updated_at), '1970-01-01')
  FROM catalog2_we47.schema2_we47.bronze_shipments1
);*/

CREATE OR REFRESH MATERIALIZED VIEW catalog2_we47.schema2_we47.bronze_shipments_fc_1
COMMENT "Bronze layer ingestion from GCP MySQL shipments table"
AS
SELECT
  shipment_id,
  first_name,
  last_name,
  age,
  role,
  updated_at
FROM gcp_mysql_fc_wd36.logistics.shipments1;