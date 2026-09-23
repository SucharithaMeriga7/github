-- ============================================================
-- Object Name : TASK_LOAD_AEROSPACE_PARTS_DAILY
-- Purpose     : Scheduled task — daily at 02:00 UTC invoking SP_LOAD_AEROSPACE_PARTS_SCD1
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

BEGIN
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TASK TASK_LOAD_AEROSPACE_PARTS_DAILY
             WAREHOUSE = ' || CURRENT_WAREHOUSE() || '
             SCHEDULE  = ''USING CRON 0 2 * * * UTC''
             COMMENT   = ''Daily scheduled task — aerospace parts SCD1 pipeline at 02:00 UTC''
         AS
             CALL SP_LOAD_AEROSPACE_PARTS_SCD1(''INCREMENTAL'')';

    EXECUTE IMMEDIATE 'ALTER TASK TASK_LOAD_AEROSPACE_PARTS_DAILY RESUME';
END;