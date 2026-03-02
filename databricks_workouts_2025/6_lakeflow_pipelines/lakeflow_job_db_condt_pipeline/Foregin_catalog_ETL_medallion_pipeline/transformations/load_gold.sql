CREATE OR REFRESH MATERIALIZED VIEW catalog2_we47.schema2_we47.gold_shipments_agg
AS
SELECT
  role,avg(age) avgage,count(1) cnt
FROM catalog2_we47.schema2_we47.silver_shipments_dlt1
GROUP BY role;