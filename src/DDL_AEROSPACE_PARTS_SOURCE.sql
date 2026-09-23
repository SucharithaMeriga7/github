-- ============================================================
-- Object Name : AEROSPACE_PARTS_SOURCE
-- Purpose     : Source table DDL for aerospace parts inventory data
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

CREATE TABLE IF NOT EXISTS IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE (
    PART_NUMBER          VARCHAR(100)     NOT NULL,
    MANUFACTURER         VARCHAR(200),
    WEIGHT_KG            NUMBER(10,3),
    UNIT_PRICE_USD       NUMBER(18,4),
    LIFECYCLE_STATUS     VARCHAR(50),
    INSTALLATION_DATE    DATE,
    UPDATED_AT           TIMESTAMP_NTZ,
    CERTIFICATION_STATUS VARCHAR(50),
    LEAD_TIME_DAYS       NUMBER(10,0),
    CONSTRAINT PK_AEROSPACE_PARTS_SOURCE PRIMARY KEY (PART_NUMBER)
);