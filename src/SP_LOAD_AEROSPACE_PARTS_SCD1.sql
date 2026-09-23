-- ============================================================
-- Object Name : SP_LOAD_AEROSPACE_PARTS_SCD1
-- Purpose     : SCD Type 1 incremental/full load pipeline for aerospace parts
--               inventory. Loads AEROSPACE_PARTS_SOURCE to AEROSPACE_PARTS_TARGET
--               with deduplication, transformation rules, RISK_SCORE derivation,
--               soft delete for decommissioned records, and ETL reconciliation logging.
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_AEROSPACE_PARTS_SCD1(
    P_MODE VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_load_type         VARCHAR;
    v_last_ts           TIMESTAMP_NTZ DEFAULT '1900-01-01 00:00:00'::TIMESTAMP_NTZ;
    v_source_count      NUMBER DEFAULT 0;
    v_target_before     NUMBER DEFAULT 0;
    v_target_after      NUMBER DEFAULT 0;
    v_inserted_count    NUMBER DEFAULT 0;
    v_updated_count     NUMBER DEFAULT 0;
    v_soft_deleted      NUMBER DEFAULT 0;
    v_excluded_count    NUMBER DEFAULT 0;
    v_staged_count      NUMBER DEFAULT 0;
    v_status            VARCHAR DEFAULT 'SUCCESS';
    v_run_id            VARCHAR DEFAULT UUID_STRING();
BEGIN

    -- ==========================================================
    -- Section 1: Watermark / Incremental Load Mode Detection
    -- Determine load type: FULL or INCREMENTAL.
    -- If no prior successful run exists, default to FULL load.
    -- ==========================================================
    IF (UPPER(P_MODE) = 'FULL') THEN
        v_load_type := 'FULL';
        v_last_ts   := '1900-01-01 00:00:00'::TIMESTAMP_NTZ;
    ELSE
        SELECT COALESCE(MAX(RUN_TIMESTAMP), '1900-01-01 00:00:00'::TIMESTAMP_NTZ)
          INTO :v_last_ts
          FROM ETL_RECONCILIATION_LOG
         WHERE SP_NAME = 'SP_LOAD_AEROSPACE_PARTS_SCD1'
           AND STATUS  = 'SUCCESS';

        IF (:v_last_ts = '1900-01-01 00:00:00'::TIMESTAMP_NTZ) THEN
            v_load_type := 'FULL';
        ELSE
            v_load_type := 'INCREMENTAL';
        END IF;
    END IF;

    -- ==========================================================
    -- Section 2: Capture Target Count Before Load
    -- ==========================================================
    SELECT COUNT(1) INTO :v_target_before
      FROM AEROSPACE_PARTS_TARGET;

    -- ==========================================================
    -- Section 3: Deduplication — Retain Latest Record per PART_NUMBER
    -- Keeps only the record with MAX(UPDATED_AT) per PART_NUMBER.
    -- Applies incremental filter: UPDATED_AT > last watermark on INCREMENTAL runs.
    -- ==========================================================
    CREATE OR REPLACE TEMPORARY TABLE TEMP_DEDUP_SOURCE AS
    SELECT
        PART_NUMBER, MANUFACTURER, WEIGHT_KG, UNIT_PRICE_USD,
        LIFECYCLE_STATUS, INSTALLATION_DATE, UPDATED_AT,
        CERTIFICATION_STATUS, LEAD_TIME_DAYS
      FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY PART_NUMBER
                       ORDER BY UPDATED_AT DESC NULLS LAST
                   ) AS RN
              FROM AEROSPACE_PARTS_SOURCE
             WHERE (:v_load_type = 'FULL' OR UPDATED_AT > :v_last_ts)
           )
     WHERE RN = 1;

    SELECT COUNT(1) INTO :v_source_count
      FROM TEMP_DEDUP_SOURCE;

    -- ==========================================================
    -- Section 4: Exclusion Filter + Transformation Rules
    -- EXCL : Drop records where LIFECYCLE_STATUS = 'End of Life'
    --        AND INSTALLATION_DATE older than 3 years from today.
    -- TR-1 : MANUFACTURER → UPPER(MANUFACTURER)
    -- TR-2 : WEIGHT_KG    → NULL when value <= 0
    -- TR-3 : UNIT_PRICE_USD → ROUND(UNIT_PRICE_USD, 2)
    -- DERIVED: RISK_SCORE — CASE on CERTIFICATION_STATUS + LEAD_TIME_DAYS
    -- ==========================================================
    CREATE OR REPLACE TEMPORARY TABLE TEMP_STAGED AS
    SELECT
        PART_NUMBER,
        UPPER(MANUFACTURER)                                              AS MANUFACTURER,
        CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END           AS WEIGHT_KG,
        ROUND(UNIT_PRICE_USD, 2)                                         AS UNIT_PRICE_USD,
        LIFECYCLE_STATUS,
        INSTALLATION_DATE,
        UPDATED_AT,
        CERTIFICATION_STATUS,
        LEAD_TIME_DAYS,
        CASE
            WHEN CERTIFICATION_STATUS = 'Pending' AND LEAD_TIME_DAYS > 90
                THEN 'High Risk'
            WHEN CERTIFICATION_STATUS = 'Pending' OR LEAD_TIME_DAYS > 120
                THEN 'Medium Risk'
            WHEN CERTIFICATION_STATUS IN ('FAA', 'EASA', 'Dual')
                 AND LEAD_TIME_DAYS <= 60
                THEN 'Low Risk'
            ELSE 'Medium Risk'
        END                                                              AS RISK_SCORE
      FROM TEMP_DEDUP_SOURCE
     WHERE NOT (
               LIFECYCLE_STATUS = 'End of Life'
           AND INSTALLATION_DATE < DATEADD('year', -3, CURRENT_DATE())
           );

    SELECT COUNT(1) INTO :v_staged_count
      FROM TEMP_STAGED;

    LET v_excluded_count := :v_source_count - :v_staged_count;

    -- ==========================================================
    -- Section 5: Pre-MERGE Count Estimation (Inserts vs Updates)
    -- ==========================================================
    SELECT COUNT(1) INTO :v_updated_count
      FROM TEMP_STAGED  S
     INNER JOIN AEROSPACE_PARTS_TARGET T
             ON T.PART_NUMBER = S.PART_NUMBER;

    LET v_inserted_count := :v_staged_count - :v_updated_count;

    -- ==========================================================
    -- Section 6: SCD Type 1 MERGE — Upsert Into Target
    -- Business Key: PART_NUMBER
    -- MATCHED     → Update all columns; reset RECORD_STATUS to Active
    -- NOT MATCHED → Insert new record with RECORD_STATUS = Active
    -- ==========================================================
    MERGE INTO AEROSPACE_PARTS_TARGET AS TGT
    USING TEMP_STAGED AS SRC
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
            TGT.ETL_LOAD_TIMESTAMP   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN
        INSERT (
            PART_NUMBER, MANUFACTURER, WEIGHT_KG, UNIT_PRICE_USD,
            LIFECYCLE_STATUS, INSTALLATION_DATE, UPDATED_AT,
            CERTIFICATION_STATUS, LEAD_TIME_DAYS, RISK_SCORE,
            RECORD_STATUS, IS_ACTIVE, ETL_LOAD_TIMESTAMP
        )
        VALUES (
            SRC.PART_NUMBER, SRC.MANUFACTURER, SRC.WEIGHT_KG, SRC.UNIT_PRICE_USD,
            SRC.LIFECYCLE_STATUS, SRC.INSTALLATION_DATE, SRC.UPDATED_AT,
            SRC.CERTIFICATION_STATUS, SRC.LEAD_TIME_DAYS, SRC.RISK_SCORE,
            'Active', TRUE, CURRENT_TIMESTAMP()
        );

    -- ==========================================================
    -- Section 7: Soft Delete — Decommission Missing Source Records
    -- Any active target record absent from the current source extract
    -- is soft-deleted: RECORD_STATUS = 'Decommissioned', IS_ACTIVE = FALSE.
    -- ==========================================================
    UPDATE AEROSPACE_PARTS_TARGET
       SET RECORD_STATUS      = 'Decommissioned',
           IS_ACTIVE          = FALSE,
           ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
     WHERE PART_NUMBER NOT IN (SELECT PART_NUMBER FROM TEMP_STAGED)
       AND IS_ACTIVE = TRUE;

    LET v_soft_deleted := SQLROWCOUNT;

    -- ==========================================================
    -- Section 8: Capture Target Count After Load
    -- ==========================================================
    SELECT COUNT(1) INTO :v_target_after
      FROM AEROSPACE_PARTS_TARGET;

    -- ==========================================================
    -- Section 9: Reconciliation Logging
    -- Mandatory per run — logged to ETL_RECONCILIATION_LOG.
    -- ==========================================================
    INSERT INTO ETL_RECONCILIATION_LOG (
        RUN_ID, SP_NAME, RUN_TIMESTAMP, LOAD_TYPE,
        SOURCE_COUNT, TARGET_COUNT_BEFORE, TARGET_COUNT_AFTER,
        INSERTED_COUNT, UPDATED_COUNT, SOFT_DELETED_COUNT,
        EXCLUDED_COUNT, STATUS, ERROR_MESSAGE
    )
    VALUES (
        :v_run_id,
        'SP_LOAD_AEROSPACE_PARTS_SCD1',
        CURRENT_TIMESTAMP(),
        :v_load_type,
        :v_source_count,
        :v_target_before,
        :v_target_after,
        :v_inserted_count,
        :v_updated_count,
        :v_soft_deleted,
        :v_excluded_count,
        :v_status,
        NULL
    );

    RETURN 'SUCCESS'
        || ' | RunID: '     || :v_run_id
        || ' | Mode: '      || :v_load_type
        || ' | Source: '    || :v_source_count
        || ' | Inserted: '  || :v_inserted_count
        || ' | Updated: '   || :v_updated_count
        || ' | SoftDel: '   || :v_soft_deleted
        || ' | Excluded: '  || :v_excluded_count;

EXCEPTION
    WHEN OTHER THEN
        LET v_err_msg VARCHAR := SQLERRM;
        v_status := 'FAILED';

        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, SP_NAME, RUN_TIMESTAMP, LOAD_TYPE,
            SOURCE_COUNT, TARGET_COUNT_BEFORE, TARGET_COUNT_AFTER,
            INSERTED_COUNT, UPDATED_COUNT, SOFT_DELETED_COUNT,
            EXCLUDED_COUNT, STATUS, ERROR_MESSAGE
        )
        VALUES (
            :v_run_id,
            'SP_LOAD_AEROSPACE_PARTS_SCD1',
            CURRENT_TIMESTAMP(),
            :v_load_type,
            :v_source_count,
            :v_target_before,
            0, 0, 0, 0, 0,
            :v_status,
            :v_err_msg
        );

        RETURN 'FAILED'
            || ' | RunID: ' || :v_run_id
            || ' | Error: ' || :v_err_msg;
END;
$$;