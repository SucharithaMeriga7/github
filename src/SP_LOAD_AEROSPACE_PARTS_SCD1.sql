-- ============================================================
-- Object Name : SP_LOAD_AEROSPACE_PARTS_SCD1
-- Purpose     : SCD Type 1 pipeline — AEROSPACE_PARTS_SOURCE to
--               AEROSPACE_PARTS_TARGET with incremental/full load,
--               deduplication by PART_NUMBER, transformation rules,
--               RISK_SCORE derivation, soft delete, and mandatory
--               reconciliation logging to ETL_RECONCILIATION_LOG
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

CREATE OR REPLACE PROCEDURE IDEA_2_DB.PUBLIC.SP_LOAD_AEROSPACE_PARTS_SCD1(
    P_MODE VARCHAR DEFAULT 'INCREMENTAL'
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id              VARCHAR        DEFAULT UUID_STRING();
    v_pipeline_name       VARCHAR        DEFAULT 'SP_LOAD_AEROSPACE_PARTS_SCD1';
    v_last_ts             TIMESTAMP_NTZ  DEFAULT '1900-01-01 00:00:00'::TIMESTAMP_NTZ;
    v_source_count        NUMBER         DEFAULT 0;
    v_post_target_count   NUMBER         DEFAULT 0;
    v_inserted_count      NUMBER         DEFAULT 0;
    v_updated_count       NUMBER         DEFAULT 0;
    v_soft_deleted_count  NUMBER         DEFAULT 0;
    v_excluded_count      NUMBER         DEFAULT 0;
    v_error_message       VARCHAR        DEFAULT NULL;
    v_execution_status    VARCHAR        DEFAULT 'SUCCESS';
BEGIN

    -- --------------------------------------------------------
    -- Section 1: Watermark Lookup
    -- INCREMENTAL: last successful run timestamp from ETL_RECONCILIATION_LOG
    -- FULL: default timestamp covers all source records
    -- --------------------------------------------------------
    IF (P_MODE = 'INCREMENTAL') THEN
        SELECT COALESCE(MAX(RUN_TIMESTAMP), '1900-01-01 00:00:00'::TIMESTAMP_NTZ)
          INTO :v_last_ts
          FROM IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG
         WHERE PIPELINE_NAME = :v_pipeline_name
           AND EXECUTION_STATUS = 'SUCCESS';
    END IF;

    -- --------------------------------------------------------
    -- Section 2: Source Extraction + Deduplication
    -- Dedup: Retain record with MAX(UPDATED_AT) per PART_NUMBER
    -- Incremental delta or full load based on P_MODE
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE AEROSPACE_PARTS_STAGE AS
    WITH RANKED_SOURCE AS (
        SELECT
            PART_NUMBER,
            MANUFACTURER,
            WEIGHT_KG,
            UNIT_PRICE_USD,
            LIFECYCLE_STATUS,
            INSTALLATION_DATE,
            UPDATED_AT,
            CERTIFICATION_STATUS,
            LEAD_TIME_DAYS,
            ROW_NUMBER() OVER (
                PARTITION BY PART_NUMBER
                ORDER BY UPDATED_AT DESC NULLS LAST
            ) AS RN
        FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
        WHERE :P_MODE = 'FULL'
           OR UPDATED_AT > :v_last_ts
    )
    SELECT
        PART_NUMBER,
        MANUFACTURER,
        WEIGHT_KG,
        UNIT_PRICE_USD,
        LIFECYCLE_STATUS,
        INSTALLATION_DATE,
        UPDATED_AT,
        CERTIFICATION_STATUS,
        LEAD_TIME_DAYS
    FROM RANKED_SOURCE
    WHERE RN = 1;

    -- --------------------------------------------------------
    -- Section 3: Exclusion Filter
    -- Exclude: LIFECYCLE_STATUS = 'End of Life' AND
    --          INSTALLATION_DATE older than 3 years from current date
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_excluded_count
    FROM AEROSPACE_PARTS_STAGE
    WHERE LIFECYCLE_STATUS = 'End of Life'
      AND INSTALLATION_DATE < DATEADD('year', -3, CURRENT_DATE());

    CREATE OR REPLACE TEMPORARY TABLE AEROSPACE_PARTS_FILTERED AS
    SELECT *
    FROM AEROSPACE_PARTS_STAGE
    WHERE NOT (
              LIFECYCLE_STATUS = 'End of Life'
          AND INSTALLATION_DATE < DATEADD('year', -3, CURRENT_DATE())
    );

    SELECT COUNT(*) INTO :v_source_count FROM AEROSPACE_PARTS_FILTERED;

    -- --------------------------------------------------------
    -- Section 4: Transformation Rules + RISK_SCORE Derivation
    -- MANUFACTURER   : UPPER() — standardize to uppercase
    -- WEIGHT_KG      : NULL sentinel for values <= 0
    -- UNIT_PRICE_USD : ROUND to 2 decimal places
    -- RISK_SCORE     : CASE precedence — High / Medium / Low Risk
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE AEROSPACE_PARTS_TRANSFORMED AS
    SELECT
        PART_NUMBER,
        UPPER(MANUFACTURER)                                               AS MANUFACTURER,
        CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END             AS WEIGHT_KG,
        ROUND(UNIT_PRICE_USD, 2)                                          AS UNIT_PRICE_USD,
        LIFECYCLE_STATUS,
        INSTALLATION_DATE,
        UPDATED_AT,
        CERTIFICATION_STATUS,
        LEAD_TIME_DAYS,
        CASE
            WHEN CERTIFICATION_STATUS = 'Pending'
             AND LEAD_TIME_DAYS > 90                                      THEN 'High Risk'
            WHEN (CERTIFICATION_STATUS = 'Pending'
             OR   LEAD_TIME_DAYS > 120)                                   THEN 'Medium Risk'
            WHEN CERTIFICATION_STATUS IN ('FAA', 'EASA', 'Dual')
             AND LEAD_TIME_DAYS <= 60                                     THEN 'Low Risk'
            ELSE                                                               'Medium Risk'
        END                                                               AS RISK_SCORE,
        CURRENT_TIMESTAMP()                                               AS ETL_LOAD_TIMESTAMP
    FROM AEROSPACE_PARTS_FILTERED;

    -- --------------------------------------------------------
    -- Section 5: Pre-Merge Reconciliation Counts
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_inserted_count
    FROM AEROSPACE_PARTS_TRANSFORMED SRC
    WHERE NOT EXISTS (
        SELECT 1 FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
        WHERE TGT.PART_NUMBER = SRC.PART_NUMBER
    );

    v_updated_count := v_source_count - v_inserted_count;

    -- --------------------------------------------------------
    -- Section 6: SCD Type 1 MERGE — Business Key: PART_NUMBER
    -- MATCHED     : UPDATE all columns (overwrite, no history)
    -- NOT MATCHED : INSERT new record marked as Active
    -- --------------------------------------------------------
    MERGE INTO IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET AS TGT
    USING AEROSPACE_PARTS_TRANSFORMED AS SRC
       ON TGT.PART_NUMBER = SRC.PART_NUMBER
    WHEN MATCHED THEN
        UPDATE SET
            TGT.MANUFACTURER         = SRC.MANUFACTURER,
            TGT.WEIGHT_KG            = SRC.WEIGHT_KG,
            TGT.UNIT_PRICE_USD       = SRC.UNIT_PRICE_USD,
            TGT.LIFECYCLE_STATUS     = SRC.LIFECYCLE_STATUS,
            TGT.INSTALLATION_DATE    = SRC.INSTALLATION_DATE,
            TGT.UPDATED_AT           = SRC.UPDATED_AT,
            TGT.CERTIFICATION_STATUS = SRC.CERTIFICATION_STATUS,
            TGT.LEAD_TIME_DAYS       = SRC.LEAD_TIME_DAYS,
            TGT.RISK_SCORE           = SRC.RISK_SCORE,
            TGT.RECORD_STATUS        = 'Active',
            TGT.IS_ACTIVE            = TRUE,
            TGT.ETL_LOAD_TIMESTAMP   = SRC.ETL_LOAD_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            PART_NUMBER,    MANUFACTURER,         WEIGHT_KG,
            UNIT_PRICE_USD, LIFECYCLE_STATUS,     INSTALLATION_DATE,
            UPDATED_AT,     CERTIFICATION_STATUS, LEAD_TIME_DAYS,
            RISK_SCORE,     RECORD_STATUS,        IS_ACTIVE,
            ETL_LOAD_TIMESTAMP
        )
        VALUES (
            SRC.PART_NUMBER,    SRC.MANUFACTURER,         SRC.WEIGHT_KG,
            SRC.UNIT_PRICE_USD, SRC.LIFECYCLE_STATUS,     SRC.INSTALLATION_DATE,
            SRC.UPDATED_AT,     SRC.CERTIFICATION_STATUS, SRC.LEAD_TIME_DAYS,
            SRC.RISK_SCORE,     'Active',                 TRUE,
            SRC.ETL_LOAD_TIMESTAMP
        );

    SELECT COUNT(*) INTO :v_post_target_count
    FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET
    WHERE IS_ACTIVE = TRUE;

    -- --------------------------------------------------------
    -- Section 7: Soft Delete — Decommission Missing Source Records
    -- Active target records absent from full AEROSPACE_PARTS_SOURCE
    -- are marked RECORD_STATUS = 'Decommissioned', IS_ACTIVE = FALSE
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_soft_deleted_count
    FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
    WHERE TGT.IS_ACTIVE = TRUE
      AND NOT EXISTS (
              SELECT 1
              FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE SRC
              WHERE SRC.PART_NUMBER = TGT.PART_NUMBER
          );

    UPDATE IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
       SET TGT.RECORD_STATUS      = 'Decommissioned',
           TGT.IS_ACTIVE          = FALSE,
           TGT.ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
     WHERE TGT.IS_ACTIVE = TRUE
       AND NOT EXISTS (
               SELECT 1
               FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE SRC
               WHERE SRC.PART_NUMBER = TGT.PART_NUMBER
           );

    -- --------------------------------------------------------
    -- Section 8: Mandatory Reconciliation Logging
    -- One record per run in ETL_RECONCILIATION_LOG
    -- --------------------------------------------------------
    INSERT INTO IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
        RUN_ID,              PIPELINE_NAME,       RUN_TIMESTAMP,
        LOAD_TYPE,           SOURCE_COUNT,        TARGET_COUNT,
        INSERTED_COUNT,      UPDATED_COUNT,       SOFT_DELETED_COUNT,
        EXCLUDED_COUNT,      EXECUTION_STATUS,    ERROR_MESSAGE
    )
    VALUES (
        :v_run_id,           :v_pipeline_name,    CURRENT_TIMESTAMP(),
        :P_MODE,             :v_source_count,     :v_post_target_count,
        :v_inserted_count,   :v_updated_count,    :v_soft_deleted_count,
        :v_excluded_count,   :v_execution_status, :v_error_message
    );

    -- --------------------------------------------------------
    -- Section 9: Return Result Variant
    -- --------------------------------------------------------
    RETURN OBJECT_CONSTRUCT(
        'run_id',             :v_run_id,
        'pipeline',           :v_pipeline_name,
        'load_type',          :P_MODE,
        'source_count',       :v_source_count,
        'inserted_count',     :v_inserted_count,
        'updated_count',      :v_updated_count,
        'soft_deleted_count', :v_soft_deleted_count,
        'excluded_count',     :v_excluded_count,
        'post_target_count',  :v_post_target_count,
        'status',             :v_execution_status
    );

EXCEPTION
    WHEN OTHER THEN
        v_error_message    := SQLERRM;
        v_execution_status := 'FAILED';

        INSERT INTO IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
            RUN_ID,          PIPELINE_NAME,    RUN_TIMESTAMP,
            LOAD_TYPE,       SOURCE_COUNT,     TARGET_COUNT,
            INSERTED_COUNT,  UPDATED_COUNT,    SOFT_DELETED_COUNT,
            EXCLUDED_COUNT,  EXECUTION_STATUS, ERROR_MESSAGE
        )
        VALUES (
            :v_run_id,       :v_pipeline_name, CURRENT_TIMESTAMP(),
            :P_MODE,         :v_source_count,  0,
            0,               0,                0,
            :v_excluded_count, :v_execution_status, :v_error_message
        );

        RETURN OBJECT_CONSTRUCT(
            'run_id', :v_run_id,
            'status', :v_execution_status,
            'error',  :v_error_message
        );
END;
$$;