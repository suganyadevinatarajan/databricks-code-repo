CREATE OR REFRESH STREAMING TABLE lakehousecat.deltadb.drugstbl_silvertbl2
(
  CONSTRAINT valid_drug_id EXPECT (uniqueid IS NOT NULL) ON VIOLATION DROP ROW
)
TBLPROPERTIES (
  "pipelines.autoOptimize.managed" = "true",
  "delta.enableChangeDataFeed" = "true")
AS
SELECT 
  *, 
  _metadata.file_path AS source_file,
  current_timestamp() AS ingestion_time
FROM STREAM(lakehousecat.deltadb.drugstbl);


CREATE OR REFRESH MATERIALIZED VIEW lakehousecat.deltadb.drugstbl_gold_view2
CLUSTER BY (uniqueid) 
COMMENT "Cleaned and optimized drug data for reporting"
TBLPROPERTIES (
  "delta.enableDeletionVectors" = "true" -- Speeds up row-level deletes/updates
)
AS
SELECT 
*,
    upper(drugname) AS drug_name_clean
    FROM lakehousecat.deltadb.drugstbl_silvertbl2;