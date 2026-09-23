-- ============================================================
-- Object Name : AEROSPACE_PARTS_TARGET
-- Purpose     : Target table DDL for aerospace parts inventory analytics
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS AEROSPACE_PARTS_TARGET (
    PART_NUMBER          VARCHAR(100)     NOT NULL,
    MANUFACTURER         VARCHAR(255),
    WEIGHT_KG            NUMBER(10,3),
    UNIT_PRICE_USD       NUMBER(18,2),
    LIFECYCLE_STATUS     VARCHAR(50),
    INSTALLATION_DATE    DATE,
    UPDATED_AT           TIMESTAMP_NTZ,
    CERTIFICATION_STATUS VARCHAR(50),
    LEAD_TIME_DAYS       NUMBER(5,0),
    RISK_SCORE           VARCHAR(20)      NOT NULL DEFAULT 'Medium Risk',
    RECORD_STATUS        VARCHAR(20)      NOT NULL DEFAULT 'Active',
    IS_ACTIVE            BOOLEAN          NOT NULL DEFAULT TRUE,
    ETL_LOAD_TIMESTAMP   TIMESTAMP_NTZ    NOT NULL,
    CONSTRAINT PK_AEROSPACE_PARTS_TARGET PRIMARY KEY (PART_NUMBER)
);