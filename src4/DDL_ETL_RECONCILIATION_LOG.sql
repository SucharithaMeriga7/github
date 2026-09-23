-- ============================================================
-- Object Name : ETL_RECONCILIATION_LOG
-- Purpose     : Pipeline reconciliation audit log — mandatory per BRD FR-010
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-24
-- ============================================================

CREATE TABLE IF NOT EXISTS IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
    RUN_ID               VARCHAR(36)      NOT NULL,
    PIPELINE_NAME        VARCHAR(100)     NOT NULL,
    RUN_TIMESTAMP        TIMESTAMP_NTZ    NOT NULL,
    LOAD_TYPE            VARCHAR(20),
    SOURCE_COUNT         NUMBER(18)       DEFAULT 0,
    TARGET_COUNT         NUMBER(18)       DEFAULT 0,
    INSERTED_COUNT       NUMBER(18)       DEFAULT 0,
    UPDATED_COUNT        NUMBER(18)       DEFAULT 0,
    DELETED_COUNT        NUMBER(18)       DEFAULT 0,
    SKIPPED_COUNT        NUMBER(18)       DEFAULT 0,
    STATUS               VARCHAR(20)      NOT NULL,
    ERROR_MESSAGE        VARCHAR(4000),
    LAST_LOAD_TIMESTAMP  TIMESTAMP_NTZ,
    RUN_DURATION_SECONDS NUMBER(18)       DEFAULT 0
);