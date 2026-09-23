-- ============================================================
-- Object Name : TASK_AEROSPACE_PARTS_DAILY
-- Purpose     : Scheduled task — daily at 02:00 UTC invoking
--               SP_LOAD_AEROSPACE_PARTS_SCD1 for automated pipeline execution
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

BEGIN
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TASK TASK_AEROSPACE_PARTS_DAILY'
        || ' WAREHOUSE = ' || CURRENT_WAREHOUSE()
        || ' SCHEDULE = ''USING CRON 0 2 * * * UTC'''
        || ' COMMENT = ''Daily aerospace parts inventory pipeline — SP_LOAD_AEROSPACE_PARTS_SCD1'''
        || ' AS CALL SP_LOAD_AEROSPACE_PARTS_SCD1(''INCREMENTAL'')';
    EXECUTE IMMEDIATE 'ALTER TASK TASK_AEROSPACE_PARTS_DAILY RESUME';
END;