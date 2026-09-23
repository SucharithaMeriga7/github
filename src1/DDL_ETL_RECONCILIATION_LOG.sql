-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Mandatory per-run reconciliation audit log for all ETL pipeline executions
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS ETL_RECONCILIATION_LOG (
    LOG_ID               VARCHAR(36)      NOT NULL DEFAULT UUID_STRING(),
    SP_NAME              VARCHAR(200)     NOT NULL,
    RUN_TIMESTAMP        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    LOAD_TYPE            VARCHAR(20)      NOT NULL,
    WATERMARK_FROM       TIMESTAMP_NTZ,
    WATERMARK_TO         TIMESTAMP_NTZ,
    SOURCE_COUNT         NUMBER(18,0)     DEFAULT 0,
    TARGET_COUNT_BEFORE  NUMBER(18,0)     DEFAULT 0,
    RECORDS_INSERTED     NUMBER(18,0)     DEFAULT 0,
    RECORDS_UPDATED      NUMBER(18,0)     DEFAULT 0,
    RECORDS_SOFT_DELETED NUMBER(18,0)     DEFAULT 0,
    RECORDS_EXCLUDED     NUMBER(18,0)     DEFAULT 0,
    STATUS               VARCHAR(20)      NOT NULL DEFAULT 'SUCCESS',
    ERROR_MESSAGE        VARCHAR(4000),
    RUN_DURATION_SECS    NUMBER(10,2),
    CONSTRAINT PK_ETL_RECONCILIATION_LOG PRIMARY KEY (LOG_ID)
);