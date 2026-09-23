-- ============================================================
-- Object Name : TASK_AEROSPACE_PARTS_DAILY
-- Purpose     : Scheduled task — Daily at 03:00 UTC
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE OR REPLACE TASK TASK_AEROSPACE_PARTS_DAILY
  WAREHOUSE = SNOWFLAKE_LEARNING_WH
  SCHEDULE  = 'USING CRON 0 3 * * * UTC'
AS
CALL SP_LOAD_AEROSPACE_PARTS_SCD1('INCREMENTAL');