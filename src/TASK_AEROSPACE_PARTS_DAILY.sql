-- ============================================================
-- Object Name : TASK_AEROSPACE_PARTS_DAILY
-- Purpose     : Scheduled task — daily at 02:00 UTC to invoke
--               SP_LOAD_AEROSPACE_PARTS_SCD1 in INCREMENTAL mode
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

BEGIN
  EXECUTE IMMEDIATE
    'CREATE OR REPLACE TASK IDEA_2_DB.PUBLIC.TASK_AEROSPACE_PARTS_DAILY
       WAREHOUSE = ' || CURRENT_WAREHOUSE() || '
       SCHEDULE  = ''USING CRON 0 2 * * * UTC''
     AS
     CALL IDEA_2_DB.PUBLIC.SP_LOAD_AEROSPACE_PARTS_SCD1(''INCREMENTAL'')';

  EXECUTE IMMEDIATE
    'ALTER TASK IDEA_2_DB.PUBLIC.TASK_AEROSPACE_PARTS_DAILY RESUME';
END;