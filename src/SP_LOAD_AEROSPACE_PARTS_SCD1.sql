-- ============================================================
-- Object Name : SP_LOAD_AEROSPACE_PARTS_SCD1
-- Purpose     : Automated SCD Type 1 ETL pipeline from AEROSPACE_PARTS_SOURCE
--               to AEROSPACE_PARTS_TARGET with incremental load, deduplication,
--               soft delete, RISK_SCORE derivation, and reconciliation logging
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_LOAD_AEROSPACE_PARTS_SCD1(
    P_MODE VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_load_type         VARCHAR         DEFAULT 'INCREMENTAL';
    v_last_ts           TIMESTAMP_NTZ   DEFAULT NULL;
    v_raw_count         NUMBER          DEFAULT 0;
    v_source_count      NUMBER          DEFAULT 0;
    v_target_before     NUMBER          DEFAULT 0;
    v_target_after      NUMBER          DEFAULT 0;
    v_inserted          NUMBER          DEFAULT 0;
    v_updated           NUMBER          DEFAULT 0;
    v_soft_deleted      NUMBER          DEFAULT 0;
    v_excluded          NUMBER          DEFAULT 0;
    v_run_id            VARCHAR         DEFAULT NULL;
    v_error_msg         VARCHAR         DEFAULT NULL;
    v_exec_status       VARCHAR         DEFAULT 'SUCCESS';
    v_result            VARIANT;

BEGIN
    v_run_id := UUID_STRING();

    -- --------------------------------------------------------
    -- SECTION 1: Watermark Lookup (Incremental Load Control)
    -- --------------------------------------------------------
    IF (UPPER(P_MODE) = 'FULL') THEN
        v_load_type := 'FULL';
        v_last_ts   := '1900-01-01 00:00:00'::TIMESTAMP_NTZ;
    ELSE
        SELECT MAX(WATERMARK_USED)
        INTO   :v_last_ts
        FROM   ETL_RECONCILIATION_LOG
        WHERE  PIPELINE_NAME    = 'SP_LOAD_AEROSPACE_PARTS_SCD1'
          AND  EXECUTION_STATUS = 'SUCCESS';

        IF (v_last_ts IS NULL) THEN
            v_load_type := 'FULL';
            v_last_ts   := '1900-01-01 00:00:00'::TIMESTAMP_NTZ;
        END IF;
    END IF;

    -- --------------------------------------------------------
    -- SECTION 2: Capture Target Record Count Before Load
    -- --------------------------------------------------------
    SELECT COUNT(*)
    INTO   :v_target_before
    FROM   AEROSPACE_PARTS_TARGET
    WHERE  IS_ACTIVE = TRUE;

    -- --------------------------------------------------------
    -- SECTION 3: Deduplication Staging
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE STAGE_RAW_DEDUPED AS
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
    FROM AEROSPACE_PARTS_SOURCE
    WHERE (:v_load_type = 'FULL' OR UPDATED_AT > :v_last_ts);

    SELECT COUNT(*) INTO :v_raw_count
    FROM   STAGE_RAW_DEDUPED
    WHERE  RN = 1;

    -- --------------------------------------------------------
    -- SECTION 4: Exclusion Filter + Transformations + RISK_SCORE Derivation
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE STAGE_AEROSPACE_PARTS AS
    SELECT
        PART_NUMBER,
        UPPER(MANUFACTURER)                                         AS MANUFACTURER,
        CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END      AS WEIGHT_KG,
        ROUND(UNIT_PRICE_USD, 2)                                    AS UNIT_PRICE_USD,
        LIFECYCLE_STATUS,
        INSTALLATION_DATE,
        UPDATED_AT,
        CERTIFICATION_STATUS,
        LEAD_TIME_DAYS,
        CASE
            WHEN CERTIFICATION_STATUS = 'Pending' AND LEAD_TIME_DAYS > 90  THEN 'High Risk'
            WHEN CERTIFICATION_STATUS = 'Pending' OR  LEAD_TIME_DAYS > 120 THEN 'Medium Risk'
            WHEN CERTIFICATION_STATUS IN ('FAA','EASA','Dual')
             AND LEAD_TIME_DAYS <= 60                               THEN 'Low Risk'
            ELSE 'Medium Risk'
        END                                                         AS RISK_SCORE
    FROM STAGE_RAW_DEDUPED
    WHERE RN = 1
      AND NOT (
            LIFECYCLE_STATUS = 'End of Life'
            AND INSTALLATION_DATE < DATEADD('year', -3, CURRENT_DATE())
          );

    SELECT COUNT(*) INTO :v_source_count FROM STAGE_AEROSPACE_PARTS;
    v_excluded := v_raw_count - v_source_count;

    -- --------------------------------------------------------
    -- SECTION 5: Pre-Merge Insert vs Update Count
    -- --------------------------------------------------------
    SELECT COALESCE(SUM(CASE WHEN tgt.PART_NUMBER IS NULL THEN 1 ELSE 0 END), 0)
    INTO   :v_inserted
    FROM   STAGE_AEROSPACE_PARTS src
    LEFT   JOIN AEROSPACE_PARTS_TARGET tgt ON src.PART_NUMBER = tgt.PART_NUMBER;

    SELECT COALESCE(SUM(CASE WHEN tgt.PART_NUMBER IS NOT NULL THEN 1 ELSE 0 END), 0)
    INTO   :v_updated
    FROM   STAGE_AEROSPACE_PARTS src
    LEFT   JOIN AEROSPACE_PARTS_TARGET tgt ON src.PART_NUMBER = tgt.PART_NUMBER;

    -- --------------------------------------------------------
    -- SECTION 6: SCD Type 1 MERGE into AEROSPACE_PARTS_TARGET
    -- --------------------------------------------------------
    MERGE INTO AEROSPACE_PARTS_TARGET tgt
    USING STAGE_AEROSPACE_PARTS src
       ON tgt.PART_NUMBER = src.PART_NUMBER
    WHEN MATCHED THEN
        UPDATE SET
            tgt.MANUFACTURER         = src.MANUFACTURER,
            tgt.WEIGHT_KG            = src.WEIGHT_KG,
            tgt.UNIT_PRICE_USD       = src.UNIT_PRICE_USD,
            tgt.LIFECYCLE_STATUS     = src.LIFECYCLE_STATUS,
            tgt.INSTALLATION_DATE    = src.INSTALLATION_DATE,
            tgt.UPDATED_AT           = src.UPDATED_AT,
            tgt.CERTIFICATION_STATUS = src.CERTIFICATION_STATUS,
            tgt.LEAD_TIME_DAYS       = src.LEAD_TIME_DAYS,
            tgt.RISK_SCORE           = src.RISK_SCORE,
            tgt.RECORD_STATUS        = 'Active',
            tgt.IS_ACTIVE            = TRUE,
            tgt.ETL_LOAD_TIMESTAMP   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            PART_NUMBER, MANUFACTURER, WEIGHT_KG, UNIT_PRICE_USD,
            LIFECYCLE_STATUS, INSTALLATION_DATE, UPDATED_AT,
            CERTIFICATION_STATUS, LEAD_TIME_DAYS, RISK_SCORE,
            RECORD_STATUS, IS_ACTIVE, ETL_LOAD_TIMESTAMP
        )
        VALUES (
            src.PART_NUMBER, src.MANUFACTURER, src.WEIGHT_KG, src.UNIT_PRICE_USD,
            src.LIFECYCLE_STATUS, src.INSTALLATION_DATE, src.UPDATED_AT,
            src.CERTIFICATION_STATUS, src.LEAD_TIME_DAYS, src.RISK_SCORE,
            'Active', TRUE, CURRENT_TIMESTAMP()
        );

    -- --------------------------------------------------------
    -- SECTION 7: Soft Delete - Decommission Records Missing from Source
    -- --------------------------------------------------------
    UPDATE AEROSPACE_PARTS_TARGET tgt
    SET    tgt.RECORD_STATUS      = 'Decommissioned',
           tgt.IS_ACTIVE          = FALSE,
           tgt.ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE  tgt.IS_ACTIVE = TRUE
      AND  tgt.PART_NUMBER NOT IN (SELECT PART_NUMBER FROM STAGE_AEROSPACE_PARTS);

    v_soft_deleted := SQLROWCOUNT;

    -- --------------------------------------------------------
    -- SECTION 8: Post-Load Target Count
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_target_after
    FROM   AEROSPACE_PARTS_TARGET
    WHERE  IS_ACTIVE = TRUE;

    -- --------------------------------------------------------
    -- SECTION 9: Reconciliation Logging
    -- --------------------------------------------------------
    INSERT INTO ETL_RECONCILIATION_LOG (
        RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, LOAD_TYPE,
        SOURCE_COUNT, TARGET_COUNT_BEFORE, TARGET_COUNT_AFTER,
        INSERTED_COUNT, UPDATED_COUNT, SOFT_DELETED_COUNT, EXCLUDED_COUNT,
        WATERMARK_USED, EXECUTION_STATUS, ERROR_MESSAGE
    )
    VALUES (
        :v_run_id,
        'SP_LOAD_AEROSPACE_PARTS_SCD1',
        CURRENT_TIMESTAMP(),
        :v_load_type,
        :v_source_count,
        :v_target_before,
        :v_target_after,
        :v_inserted,
        :v_updated,
        :v_soft_deleted,
        :v_excluded,
        :v_last_ts,
        :v_exec_status,
        :v_error_msg
    );

    -- --------------------------------------------------------
    -- SECTION 10: Return Execution Summary
    -- --------------------------------------------------------
    SELECT OBJECT_CONSTRUCT(
        'status',          :v_exec_status,
        'run_id',          :v_run_id,
        'load_type',       :v_load_type,
        'source_count',    :v_source_count,
        'inserted',        :v_inserted,
        'updated',         :v_updated,
        'soft_deleted',    :v_soft_deleted,
        'excluded',        :v_excluded,
        'target_before',   :v_target_before,
        'target_after',    :v_target_after
    ) INTO :v_result;

    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        v_exec_status := 'FAILED';
        v_error_msg   := SQLERRM;

        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, LOAD_TYPE,
            EXECUTION_STATUS, ERROR_MESSAGE
        )
        VALUES (
            :v_run_id,
            'SP_LOAD_AEROSPACE_PARTS_SCD1',
            CURRENT_TIMESTAMP(),
            :v_load_type,
            'FAILED',
            :v_error_msg
        );

        SELECT OBJECT_CONSTRUCT('status', 'FAILED', 'error', :v_error_msg)
        INTO   :v_result;

        RETURN :v_result;
END;
$$;