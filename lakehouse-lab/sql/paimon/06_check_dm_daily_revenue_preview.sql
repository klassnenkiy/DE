CREATE CATALOG IF NOT EXISTS paimon WITH (
  'type' = 'paimon',
  'warehouse' = 's3a://lake/paimon'
);

USE CATALOG paimon;
USE `default`;

SET 'table.dml-sync' = 'true';
SET 'execution.runtime-mode' = 'BATCH';
SET 'sql-client.execution.result-mode' = 'TABLEAU';

SELECT *
FROM dm_daily_revenue_by_merchant
ORDER BY dt DESC, merchant_id
LIMIT 20;
