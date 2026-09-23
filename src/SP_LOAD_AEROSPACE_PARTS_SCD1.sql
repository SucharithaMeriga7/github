-- ============================================================
-- Object Name : SP_LOAD_AEROSPACE_PARTS_SCD1
-- Purpose     : SCD Type 1 ETL pipeline — AEROSPACE_PARTS_SOURCE to AEROSPACE_PARTS_TARGET
--               Supports incremental and full load modes with deduplication,
--               transformation rules, RISK_SCORE derivation, SCD1 MERGE,
--               soft delete for decommissioned records, and mandatory ETL
--               reconciliation logging per run
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================

CREATE OR REPLACE PROCEDURE SP_LOAD_AEROSPACE_PARTS_SCD1(
    P_MODE VARCHAR DEFAULT 'INCREMENTAL'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_log_id                   NUMBER;
    v_start_ts                 TIMESTAMP_NTZ;
    v_watermark                TIMESTAMP_NTZ;
    v_source_count             NUMBER  DEFAULT 0;
    v_dedup_count              NUMBER  DEFAULT 0;
    v_excluded_count           NUMBER  DEFAULT 0;
    v_inserted_count           NUMBER  DEFAULT 0;
    v_updated_count            NUMBER  DEFAULT 0;
    v_soft_deleted_count       NUMBER  DEFAULT 0;
    v_target_count_after_load  NUMBER  DEFAULT 0;
    v_load_type                VARCHAR DEFAULT 'INCREMENTAL';
    v_duration                 NUMBER  DEFAULT 0;
    v_error_msg                VARCHAR;
BEGIN

    -- --------------------------------------------------------
    -- Section: Initialization
    -- --------------------------------------------------------
    v_start_ts := CURRENT_TIMESTAMP();

    IF (UPPER(P_MODE) = 'FULL') THEN
        v_load_type := 'FULL';
    ELSE
        v_load_type := 'INCREMENTAL';
    END IF;

    INSERT INTO ETL_RECONCILIATION_LOG (PIPELINE_NAME, LOAD_TYPE, EXECUTION_STATUS, RUN_TIMESTAMP)
    VALUES ('SP_LOAD_AEROSPACE_PARTS_SCD1', :v_load_type, 'RUNNING', :v_start_ts);

    SELECT MAX(LOG_ID) INTO :v_log_id
    FROM ETL_RECONCILIATION_LOG
    WHERE PIPELINE_NAME = 'SP_LOAD_AEROSPACE_PARTS_SCD1'
      AND EXECUTION_STATUS = 'RUNNING';

    -- --------------------------------------------------------
    -- Section: Watermark Lookup — Incremental Load
    -- --------------------------------------------------------
    IF (v_load_type = 'INCREMENTAL') THEN
        SELECT MAX(WATERMARK_USED) INTO :v_watermark
        FROM ETL_RECONCILIATION_LOG
        WHERE PIPELINE_NAME = 'SP_LOAD_AEROSPACE_PARTS_SCD1'
          AND EXECUTION_STATUS = 'SUCCESS'
          AND WATERMARK_USED IS NOT NULL;

        IF (v_watermark IS NULL) THEN
            v_load_type := 'FULL';
        END IF;
    END IF;

    -- --------------------------------------------------------
    -- Section: Source Extract Row Count
    -- --------------------------------------------------------
    IF (v_load_type = 'FULL') THEN
        SELECT COUNT(*) INTO :v_source_count
        FROM AEROSPACE_PARTS_SOURCE;
    ELSE
        SELECT COUNT(*) INTO :v_source_count
        FROM AEROSPACE_PARTS_SOURCE
        WHERE UPDATED_AT > :v_watermark;
    END IF;

    -- --------------------------------------------------------
    -- Section: Deduplication — Retain Latest Record per PART_NUMBER
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE TMP_AERO_DEDUP AS
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
        WHERE (
              (:v_load_type = 'FULL')
           OR (:v_load_type = 'INCREMENTAL' AND UPDATED_AT > :v_watermark)
        )
    ) dedup_src
    WHERE RN = 1;

    SELECT COUNT(*) INTO :v_dedup_count FROM TMP_AERO_DEDUP;

    -- --------------------------------------------------------
    -- Section: Exclusion Filter — End of Life Parts Older Than 3 Years
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE TMP_AERO_FILTERED AS
    SELECT *
    FROM TMP_AERO_DEDUP
    WHERE NOT (
        LIFECYCLE_STATUS = 'End of Life'
        AND INSTALLATION_DATE < DATEADD('year', -3, CURRENT_DATE())
    );

    v_excluded_count := :v_dedup_count - (SELECT COUNT(*) FROM TMP_AERO_FILTERED);

    -- --------------------------------------------------------
    -- Section: Transformations + RISK_SCORE Derivation
    -- --------------------------------------------------------
    CREATE OR REPLACE TEMPORARY TABLE TMP_AERO_STAGED AS
    SELECT
        PART_NUMBER,
        UPPER(MANUFACTURER)                                                                    AS MANUFACTURER,
        CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END                                 AS WEIGHT_KG,
        ROUND(UNIT_PRICE_USD, 2)                                                               AS UNIT_PRICE_USD,
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
            WHEN CERTIFICATION_STATUS IN ('FAA', 'EASA', 'Dual') AND LEAD_TIME_DAYS <= 60
                THEN 'Low Risk'
            ELSE 'Medium Risk'
        END                                                                                    AS RISK_SCORE,
        'Active'                                                                                AS RECORD_STATUS,
        TRUE                                                                                    AS IS_ACTIVE,
        CURRENT_TIMESTAMP()                                                                     AS ETL_LOAD_TIMESTAMP
    FROM TMP_AERO_FILTERED;

    -- --------------------------------------------------------
    -- Section: Pre-MERGE Insert vs Update Count
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_inserted_count
    FROM TMP_AERO_STAGED src
    WHERE NOT EXISTS (
        SELECT 1 FROM AEROSPACE_PARTS_TARGET tgt
        WHERE tgt.PART_NUMBER = src.PART_NUMBER
    );

    SELECT COUNT(*) INTO :v_updated_count
    FROM TMP_AERO_STAGED src
    WHERE EXISTS (
        SELECT 1 FROM AEROSPACE_PARTS_TARGET tgt
        WHERE tgt.PART_NUMBER = src.PART_NUMBER
    );

    -- --------------------------------------------------------
    -- Section: SCD Type 1 MERGE — Business Key: PART_NUMBER
    -- --------------------------------------------------------
    MERGE INTO AEROSPACE_PARTS_TARGET tgt
    USING TMP_AERO_STAGED src
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
            tgt.RECORD_STATUS        = src.RECORD_STATUS,
            tgt.IS_ACTIVE            = src.IS_ACTIVE,
            tgt.ETL_LOAD_TIMESTAMP   = src.ETL_LOAD_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            PART_NUMBER, MANUFACTURER, WEIGHT_KG, UNIT_PRICE_USD,
            LIFECYCLE_STATUS, INSTALLATION_DATE, UPDATED_AT,
            CERTIFICATION_STATUS, LEAD_TIME_DAYS,
            RISK_SCORE, RECORD_STATUS, IS_ACTIVE, ETL_LOAD_TIMESTAMP
        )
        VALUES (
            src.PART_NUMBER, src.MANUFACTURER, src.WEIGHT_KG, src.UNIT_PRICE_USD,
            src.LIFECYCLE_STATUS, src.INSTALLATION_DATE, src.UPDATED_AT,
            src.CERTIFICATION_STATUS, src.LEAD_TIME_DAYS,
            src.RISK_SCORE, src.RECORD_STATUS, src.IS_ACTIVE, src.ETL_LOAD_TIMESTAMP
        );

    -- --------------------------------------------------------
    -- Section: Soft Delete — Decommission Records Missing from Source
    -- --------------------------------------------------------
    UPDATE AEROSPACE_PARTS_TARGET tgt
    SET
        tgt.RECORD_STATUS      = 'Decommissioned',
        tgt.IS_ACTIVE          = FALSE,
        tgt.ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE tgt.IS_ACTIVE = TRUE
      AND NOT EXISTS (
          SELECT 1 FROM AEROSPACE_PARTS_SOURCE src
          WHERE src.PART_NUMBER = tgt.PART_NUMBER
      );

    v_soft_deleted_count := SQLROWCOUNT;

    -- --------------------------------------------------------
    -- Section: Reconciliation Logging
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_target_count_after_load
    FROM AEROSPACE_PARTS_TARGET
    WHERE IS_ACTIVE = TRUE;

    v_duration := ROUND(DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP()) / 1000.0, 2);

    UPDATE ETL_RECONCILIATION_LOG
    SET
        WATERMARK_USED             = :v_watermark,
        SOURCE_COUNT               = :v_source_count,
        AFTER_DEDUP_COUNT          = :v_dedup_count,
        EXCLUDED_COUNT             = :v_excluded_count,
        INSERTED_COUNT             = :v_inserted_count,
        UPDATED_COUNT              = :v_updated_count,
        SOFT_DELETED_COUNT         = :v_soft_deleted_count,
        TARGET_COUNT_AFTER_LOAD    = :v_target_count_after_load,
        EXECUTION_STATUS           = 'SUCCESS',
        EXECUTION_DURATION_SECONDS = :v_duration
    WHERE LOG_ID = :v_log_id;

    RETURN 'SUCCESS | LoadType=' || :v_load_type
        || ' | Source='      || :v_source_count
        || ' | Inserted='    || :v_inserted_count
        || ' | Updated='     || :v_updated_count
        || ' | SoftDeleted=' || :v_soft_deleted_count
        || ' | Excluded='    || :v_excluded_count
        || ' | Duration='    || :v_duration || 's';

EXCEPTION
    WHEN OTHER THEN
        v_error_msg := SQLERRM;

        UPDATE ETL_RECONCILIATION_LOG
        SET EXECUTION_STATUS = 'FAILED',
            ERROR_MESSAGE    = :v_error_msg
        WHERE LOG_ID = :v_log_id;

        RAISE;

END;
$$;