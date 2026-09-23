-- ============================================================
-- Object Name : TASK_LOAD_AEROSPACE_PARTS
-- Purpose     : Scheduled task — daily at 04:00 UTC per BRD schedule requirement
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-24
-- ============================================================

CREATE OR REPLACE TASK IDEA_2_DB.PUBLIC.TASK_LOAD_AEROSPACE_PARTS
    WAREHOUSE = SNOWFLAKE_LEARNING_WH
    SCHEDULE = 'USING CRON 0 4 * * * UTC'
AS
    CALL IDEA_2_DB.PUBLIC.LOAD_AEROSPACE_PARTS('INCREMENTAL');