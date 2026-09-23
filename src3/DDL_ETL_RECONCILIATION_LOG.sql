-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Reconciliation log DDL for pipeline run audit
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS ETL_RECONCILIATION_LOG (
    RUN_ID              VARCHAR(36)       NOT NULL,
    PIPELINE_NAME       VARCHAR(200)      NOT NULL,
    RUN_TIMESTAMP       TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    WATERMARK_USED      TIMESTAMP_NTZ,
    SOURCE_COUNT        INTEGER           NOT NULL DEFAULT 0,
    TARGET_COUNT        INTEGER           NOT NULL DEFAULT 0,
    INSERT_COUNT        INTEGER           NOT NULL DEFAULT 0,
    UPDATE_COUNT        INTEGER           NOT NULL DEFAULT 0,
    SOFT_DELETE_COUNT   INTEGER           NOT NULL DEFAULT 0,
    REJECTION_COUNT     INTEGER           NOT NULL DEFAULT 0,
    STATUS              VARCHAR(20)       NOT NULL,
    EXECUTION_SECONDS   FLOAT,
    CONSTRAINT PK_ETL_RECONCILIATION_LOG PRIMARY KEY (RUN_ID)
);