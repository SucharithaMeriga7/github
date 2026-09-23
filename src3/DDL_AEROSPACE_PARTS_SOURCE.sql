-- ============================================================
-- Object Name : AEROSPACE_PARTS_SOURCE
-- Purpose     : Source table DDL for aerospace parts inventory data
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE TABLE IF NOT EXISTS AEROSPACE_PARTS_SOURCE (
    PART_NUMBER          VARCHAR(100)      NOT NULL,
    PART_NAME            VARCHAR(500),
    CATEGORY             VARCHAR(200),
    MANUFACTURER         VARCHAR(300),
    WEIGHT_KG            FLOAT,
    CERTIFICATION_STATUS VARCHAR(50),
    LEAD_TIME_DAYS       INTEGER,
    UNIT_PRICE_USD       FLOAT,
    STOCK_QUANTITY       INTEGER,
    SUPPLIER_ID          VARCHAR(100),
    UPDATED_AT           TIMESTAMP_NTZ     NOT NULL,
    CONSTRAINT PK_AEROSPACE_PARTS_SOURCE PRIMARY KEY (PART_NUMBER)
);