-- ============================================================
-- Object Name : AEROSPACE_PARTS_TARGET
-- Purpose     : Target table for cleansed, enriched aerospace parts inventory
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET (
    PART_NUMBER          VARCHAR(100)    NOT NULL,
    PART_NAME            VARCHAR(500),
    MANUFACTURER         VARCHAR(255),
    CATEGORY             VARCHAR(100),
    WEIGHT_KG            NUMBER(10,4),
    UNIT_PRICE_USD       NUMBER(18,2),
    LIFECYCLE_STATUS     VARCHAR(50),
    INSTALLATION_DATE    DATE,
    CERTIFICATION_STATUS VARCHAR(50),
    LEAD_TIME_DAYS       NUMBER(10,0),
    UPDATED_AT           TIMESTAMP_NTZ,
    RISK_SCORE           VARCHAR(10)     NOT NULL DEFAULT 'Medium',
    ETL_LOAD_TIMESTAMP   TIMESTAMP_NTZ   NOT NULL,
    CONSTRAINT PK_AEROSPACE_PARTS_TARGET PRIMARY KEY (PART_NUMBER)
)