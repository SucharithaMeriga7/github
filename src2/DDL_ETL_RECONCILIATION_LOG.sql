-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Per-run reconciliation audit log for aerospace parts pipeline
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
    RUN_ID               VARCHAR(36)     NOT NULL DEFAULT UUID_STRING(),
    PIPELINE_NAME        VARCHAR(200),
    RUN_TIMESTAMP        TIMESTAMP_NTZ   NOT NULL DEFAULT CURRENT_TIMESTAMP(),
    LOAD_TYPE            VARCHAR(20),
    SOURCE_COUNT         NUMBER(18,0),
    TARGET_COUNT         NUMBER(18,0),
    INSERTED_COUNT       NUMBER(18,0),
    UPDATED_COUNT        NUMBER(18,0),
    DELETED_COUNT        NUMBER(18,0),
    EXCLUDED_COUNT       NUMBER(18,0),
    DEDUP_COUNT          NUMBER(18,0),
    NULL_WEIGHT_COUNT    NUMBER(18,0),
    INVALID_CERT_COUNT   NUMBER(18,0),
    STATUS               VARCHAR(20),
    ERROR_MESSAGE        VARCHAR(4000),
    LAST_LOAD_TIMESTAMP  TIMESTAMP_NTZ,
    RUN_DURATION_SECONDS NUMBER(10,2)
)