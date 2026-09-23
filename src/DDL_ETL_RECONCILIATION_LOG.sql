-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Reconciliation log DDL for pipeline audit and run-level tracking
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

CREATE TABLE IF NOT EXISTS ETL_RECONCILIATION_LOG (
    LOG_ID                     NUMBER          AUTOINCREMENT PRIMARY KEY,
    RUN_TIMESTAMP              TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    PIPELINE_NAME              VARCHAR(200)    NOT NULL,
    LOAD_TYPE                  VARCHAR(20)     NOT NULL,
    WATERMARK_USED             TIMESTAMP_NTZ,
    SOURCE_COUNT               NUMBER          DEFAULT 0,
    AFTER_DEDUP_COUNT          NUMBER          DEFAULT 0,
    EXCLUDED_COUNT             NUMBER          DEFAULT 0,
    INSERTED_COUNT             NUMBER          DEFAULT 0,
    UPDATED_COUNT              NUMBER          DEFAULT 0,
    SOFT_DELETED_COUNT         NUMBER          DEFAULT 0,
    TARGET_COUNT_AFTER_LOAD    NUMBER          DEFAULT 0,
    EXECUTION_STATUS           VARCHAR(20)     NOT NULL DEFAULT 'RUNNING',
    ERROR_MESSAGE              VARCHAR(4000),
    EXECUTION_DURATION_SECONDS NUMBER(10,2)
);