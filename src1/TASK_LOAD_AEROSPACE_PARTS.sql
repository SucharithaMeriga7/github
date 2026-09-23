-- ============================================================
-- Object Name : TASK_LOAD_AEROSPACE_PARTS
-- Purpose     : Scheduled task — Daily at 02:00 UTC for LOAD_AEROSPACE_PARTS
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
BEGIN
    EXECUTE IMMEDIATE
        'CREATE OR REPLACE TASK TASK_LOAD_AEROSPACE_PARTS'
        || ' WAREHOUSE = ''' || CURRENT_WAREHOUSE() || ''''
        || ' SCHEDULE = ''USING CRON 0 2 * * * UTC'''
        || ' AS CALL LOAD_AEROSPACE_PARTS(''INCREMENTAL'')';
    EXECUTE IMMEDIATE 'ALTER TASK TASK_LOAD_AEROSPACE_PARTS RESUME';
END;