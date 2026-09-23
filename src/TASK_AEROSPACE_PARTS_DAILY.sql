BEGIN
  -- ============================================================
  -- Object Name : TASK_AEROSPACE_PARTS_DAILY
  -- Purpose     : Scheduled task - daily execution at 02:00 UTC (CRON 0 2 * * *)
  -- Author      : SUCHARITHAS
  -- Generated   : 2026-09-23
  -- ============================================================
  EXECUTE IMMEDIATE 'CREATE OR REPLACE TASK TASK_AEROSPACE_PARTS_DAILY'
    || ' WAREHOUSE = ' || CURRENT_WAREHOUSE()
    || ' SCHEDULE = ''USING CRON 0 2 * * * UTC'''
    || ' AS CALL SP_LOAD_AEROSPACE_PARTS_SCD1(''INCREMENTAL'')';
  EXECUTE IMMEDIATE 'ALTER TASK TASK_AEROSPACE_PARTS_DAILY RESUME';
END;