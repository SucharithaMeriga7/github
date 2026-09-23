-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Reconciliation log table - per-run audit trail for pipeline metrics
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS ETL_RECONCILIATION_LOG (
    RUN_ID               VARCHAR(36)      NOT NULL DEFAULT UUID_STRING(),
    PIPELINE_NAME        VARCHAR(255)     NOT NULL,
    RUN_TIMESTAMP        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    LOAD_TYPE            VARCHAR(20),
    SOURCE_COUNT         NUMBER(15,0),
    TARGET_COUNT_BEFORE  NUMBER(15,0),
    TARGET_COUNT_AFTER   NUMBER(15,0),
    INSERTED_COUNT       NUMBER(15,0),
    UPDATED_COUNT        NUMBER(15,0),
    SOFT_DELETED_COUNT   NUMBER(15,0),
    EXCLUDED_COUNT       NUMBER(15,0),
    WATERMARK_USED       TIMESTAMP_NTZ,
    EXECUTION_STATUS     VARCHAR(20),
    ERROR_MESSAGE        VARCHAR(4000),
    CONSTRAINT PK_ETL_RECONCILIATION_LOG PRIMARY KEY (RUN_ID)
);