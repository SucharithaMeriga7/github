-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Reconciliation log DDL for pipeline audit per run
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

CREATE TABLE IF NOT EXISTS ETL_RECONCILIATION_LOG (
    RUN_ID               VARCHAR(36)      NOT NULL DEFAULT UUID_STRING(),
    SP_NAME              VARCHAR(200),
    RUN_TIMESTAMP        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    LOAD_TYPE            VARCHAR(20),
    SOURCE_COUNT         NUMBER(18,0),
    TARGET_COUNT_BEFORE  NUMBER(18,0),
    TARGET_COUNT_AFTER   NUMBER(18,0),
    INSERTED_COUNT       NUMBER(18,0),
    UPDATED_COUNT        NUMBER(18,0),
    SOFT_DELETED_COUNT   NUMBER(18,0),
    EXCLUDED_COUNT       NUMBER(18,0),
    STATUS               VARCHAR(20),
    ERROR_MESSAGE        VARCHAR(4000)
);