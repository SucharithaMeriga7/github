-- ============================================================
-- Object Name : AEROSPACE_PARTS_SOURCE
-- Purpose     : Source table DDL for aerospace parts inventory data
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-24
-- ============================================================

CREATE TABLE IF NOT EXISTS IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE (
    PART_NUMBER          VARCHAR(100)     NOT NULL,
    PART_NAME            VARCHAR(255),
    MANUFACTURER         VARCHAR(255),
    CATEGORY             VARCHAR(100),
    WEIGHT_KG            NUMBER(18, 4),
    UNIT_PRICE_USD       NUMBER(18, 4),
    LIFECYCLE_STATUS     VARCHAR(50),
    INSTALLATION_DATE    DATE,
    CERTIFICATION_STATUS VARCHAR(50),
    LEAD_TIME_DAYS       NUMBER(10),
    UPDATED_AT           TIMESTAMP_NTZ
);