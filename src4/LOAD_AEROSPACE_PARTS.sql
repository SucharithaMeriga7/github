-- ============================================================
-- Object Name : LOAD_AEROSPACE_PARTS
-- Purpose     : Incremental SCD Type 1 pipeline — AEROSPACE_PARTS_SOURCE to
--               AEROSPACE_PARTS_TARGET with risk scoring, soft delete,
--               and mandatory reconciliation logging per BRD FR-010
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-24
-- [DELTA REFERENCE] Adapted from: IDEA_2_DB.PUBLIC.LOAD_AEROSPACE_PARTS
-- ============================================================

CREATE OR REPLACE PROCEDURE IDEA_2_DB.PUBLIC.LOAD_AEROSPACE_PARTS(
    P_MODE VARCHAR DEFAULT 'INCREMENTAL'
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id           VARCHAR         DEFAULT UUID_STRING();
    v_run_start        TIMESTAMP_NTZ   DEFAULT CURRENT_TIMESTAMP();
    v_last_ts          TIMESTAMP_NTZ   DEFAULT NULL;
    v_load_type        VARCHAR         DEFAULT 'INCREMENTAL';
    v_source_count     NUMBER          DEFAULT 0;
    v_target_count     NUMBER          DEFAULT 0;
    v_inserted_count   NUMBER          DEFAULT 0;
    v_updated_count    NUMBER          DEFAULT 0;
    v_deleted_count    NUMBER          DEFAULT 0;
    v_skipped_count    NUMBER          DEFAULT 0;
    v_error_msg        VARCHAR         DEFAULT NULL;
    v_status           VARCHAR         DEFAULT 'SUCCESS';

BEGIN

    -- Watermark Lookup
    IF (P_MODE = 'FULL') THEN
        v_load_type := 'FULL';
    ELSE
        SELECT MAX(LAST_LOAD_TIMESTAMP)
          INTO :v_last_ts
          FROM IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG
         WHERE PIPELINE_NAME = 'LOAD_AEROSPACE_PARTS'
           AND STATUS = 'SUCCESS';

        IF (:v_last_ts IS NULL) THEN
            v_load_type := 'FULL';
        END IF;
    END IF;

    -- Deduplication + Transformations + Exclusion Filter
    CREATE OR REPLACE TEMPORARY TABLE TMP_AEROSPACE_STAGED AS
    WITH DEDUPED AS (
        SELECT
            PART_NUMBER,
            PART_NAME,
            -- [DELTA CHANGE] UPPER(MANUFACTURER) -> INITCAP(MANUFACTURER): BRD RULE-008 requires camelcase
            INITCAP(MANUFACTURER)                                                AS MANUFACTURER,
            CATEGORY,
            -- [DELTA VERIFY] WEIGHT_KG null replacement: confirmed CASE WHEN <= 0 THEN NULL per BRD RULE-009
            CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END               AS WEIGHT_KG,
            -- [DELTA ADD] ROUND(UNIT_PRICE_USD, 2): missing from prior SP — BRD RULE-010 requires 2dp precision
            ROUND(UNIT_PRICE_USD, 2)                                            AS UNIT_PRICE_USD,
            LIFECYCLE_STATUS,
            INSTALLATION_DATE,
            CERTIFICATION_STATUS,
            LEAD_TIME_DAYS,
            UPDATED_AT,
            -- [DELTA CHANGE] RISK_SCORE High threshold: was >90, BRD RULE-007 requires >60
            -- [DELTA MODIFY] RISK_SCORE Low: FAA/EASA/Dual AND LEAD_TIME_DAYS <= 60 per BRD RULE-007
            CASE
                WHEN CERTIFICATION_STATUS = 'Pending' AND LEAD_TIME_DAYS > 60  THEN 'High Risk'
                WHEN CERTIFICATION_STATUS = 'Pending' OR  LEAD_TIME_DAYS > 120 THEN 'Medium Risk'
                WHEN CERTIFICATION_STATUS IN ('FAA','EASA','Dual')
                     AND LEAD_TIME_DAYS <= 60                                   THEN 'Low Risk'
                ELSE 'Medium Risk'
            END                                                                 AS RISK_SCORE,
            ROW_NUMBER() OVER (
                PARTITION BY PART_NUMBER ORDER BY UPDATED_AT DESC NULLS LAST
            )                                                                   AS RN
        FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
        WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
          -- [DELTA CHANGE] Exclusion window: was DATEADD(YEAR,-3,...), BRD RULE-006 requires 5 years
          AND NOT (
              LIFECYCLE_STATUS = 'End of Life'
              AND INSTALLATION_DATE < DATEADD(YEAR, -5, CURRENT_DATE())
          )
    )
    SELECT
        PART_NUMBER, PART_NAME, MANUFACTURER, CATEGORY,
        WEIGHT_KG, UNIT_PRICE_USD, LIFECYCLE_STATUS, INSTALLATION_DATE,
        CERTIFICATION_STATUS, LEAD_TIME_DAYS, UPDATED_AT, RISK_SCORE
        -- [DELTA REMOVE] ETL_LOAD_TIMESTAMP removed — not in BRD/STTM per guardrail
    FROM DEDUPED
    WHERE RN = 1;

    -- Pre-Merge Metrics
    SELECT COUNT(*) INTO :v_source_count FROM TMP_AEROSPACE_STAGED;

    SELECT COUNT(*) INTO :v_updated_count
      FROM TMP_AEROSPACE_STAGED SRC
      JOIN IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
        ON TGT.PART_NUMBER = SRC.PART_NUMBER;

    v_inserted_count := v_source_count - v_updated_count;

    SELECT COUNT(*) INTO :v_skipped_count
      FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
     WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
       AND LIFECYCLE_STATUS = 'End of Life'
       AND INSTALLATION_DATE < DATEADD(YEAR, -5, CURRENT_DATE());

    -- SCD Type 1 MERGE
    MERGE INTO IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET AS TGT
    USING TMP_AEROSPACE_STAGED AS SRC
       ON TGT.PART_NUMBER = SRC.PART_NUMBER
    WHEN MATCHED THEN UPDATE SET
        TGT.PART_NAME            = SRC.PART_NAME,
        TGT.MANUFACTURER         = SRC.MANUFACTURER,
        TGT.CATEGORY             = SRC.CATEGORY,
        TGT.WEIGHT_KG            = SRC.WEIGHT_KG,
        TGT.UNIT_PRICE_USD       = SRC.UNIT_PRICE_USD,
        TGT.LIFECYCLE_STATUS     = SRC.LIFECYCLE_STATUS,
        TGT.INSTALLATION_DATE    = SRC.INSTALLATION_DATE,
        TGT.CERTIFICATION_STATUS = SRC.CERTIFICATION_STATUS,
        TGT.LEAD_TIME_DAYS       = SRC.LEAD_TIME_DAYS,
        TGT.UPDATED_AT           = SRC.UPDATED_AT,
        TGT.RISK_SCORE           = SRC.RISK_SCORE
    WHEN NOT MATCHED THEN INSERT (
        PART_NUMBER, PART_NAME, MANUFACTURER, CATEGORY,
        WEIGHT_KG, UNIT_PRICE_USD, LIFECYCLE_STATUS, INSTALLATION_DATE,
        CERTIFICATION_STATUS, LEAD_TIME_DAYS, UPDATED_AT, RISK_SCORE
    ) VALUES (
        SRC.PART_NUMBER, SRC.PART_NAME, SRC.MANUFACTURER, SRC.CATEGORY,
        SRC.WEIGHT_KG, SRC.UNIT_PRICE_USD, SRC.LIFECYCLE_STATUS, SRC.INSTALLATION_DATE,
        SRC.CERTIFICATION_STATUS, SRC.LEAD_TIME_DAYS, SRC.UPDATED_AT, SRC.RISK_SCORE
    );

    -- Soft Delete
    UPDATE IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
       SET TGT.LIFECYCLE_STATUS = 'Decommissioned',
           TGT.UPDATED_AT       = CURRENT_TIMESTAMP()
     WHERE TGT.LIFECYCLE_STATUS <> 'Decommissioned'
       AND TGT.PART_NUMBER NOT IN (
               SELECT PART_NUMBER FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
           );

    v_deleted_count := SQLROWCOUNT;

    -- Post-Merge Target Count
    SELECT COUNT(*) INTO :v_target_count FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET;

    -- Reconciliation Logging
    INSERT INTO IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
        RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, LOAD_TYPE,
        SOURCE_COUNT, TARGET_COUNT, INSERTED_COUNT, UPDATED_COUNT,
        DELETED_COUNT, SKIPPED_COUNT, STATUS, ERROR_MESSAGE,
        LAST_LOAD_TIMESTAMP, RUN_DURATION_SECONDS
    ) VALUES (
        :v_run_id, 'LOAD_AEROSPACE_PARTS', :v_run_start, :v_load_type,
        :v_source_count, :v_target_count, :v_inserted_count, :v_updated_count,
        :v_deleted_count, :v_skipped_count, :v_status, :v_error_msg,
        CURRENT_TIMESTAMP(), DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );

    RETURN 'SUCCESS | RunID: ' || :v_run_id
        || ' | LoadType: ' || :v_load_type
        || ' | Source: '   || TO_VARCHAR(:v_source_count)
        || ' | Inserted: ' || TO_VARCHAR(:v_inserted_count)
        || ' | Updated: '  || TO_VARCHAR(:v_updated_count)
        || ' | SoftDeleted: ' || TO_VARCHAR(:v_deleted_count)
        || ' | Skipped: '  || TO_VARCHAR(:v_skipped_count);

EXCEPTION WHEN OTHER THEN
    v_status    := 'FAILED';
    v_error_msg := SQLERRM;

    INSERT INTO IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
        RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, LOAD_TYPE,
        SOURCE_COUNT, TARGET_COUNT, INSERTED_COUNT, UPDATED_COUNT,
        DELETED_COUNT, SKIPPED_COUNT, STATUS, ERROR_MESSAGE,
        LAST_LOAD_TIMESTAMP, RUN_DURATION_SECONDS
    ) VALUES (
        :v_run_id, 'LOAD_AEROSPACE_PARTS', :v_run_start, :v_load_type,
        :v_source_count, 0, 0, 0, 0, 0, :v_status, :v_error_msg,
        CURRENT_TIMESTAMP(), DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );

    RAISE;
END;
$$