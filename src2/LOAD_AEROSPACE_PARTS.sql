-- ============================================================
-- Object Name : LOAD_AEROSPACE_PARTS
-- Purpose     : Incremental SCD Type 1 pipeline for AEROSPACE_PARTS_SOURCE to AEROSPACE_PARTS_TARGET
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE OR REPLACE PROCEDURE IDEA_2_DB.PUBLIC.LOAD_AEROSPACE_PARTS(P_MODE VARCHAR DEFAULT 'INCREMENTAL')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_run_id             VARCHAR       DEFAULT UUID_STRING();
    v_run_start          TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP();
    v_last_ts            TIMESTAMP_NTZ DEFAULT NULL;
    v_load_type          VARCHAR       DEFAULT 'INCREMENTAL';
    v_source_count       NUMBER        DEFAULT 0;
    v_target_count       NUMBER        DEFAULT 0;
    v_inserted_count     NUMBER        DEFAULT 0;
    v_updated_count      NUMBER        DEFAULT 0;
    v_deleted_count      NUMBER        DEFAULT 0;
    v_excluded_count     NUMBER        DEFAULT 0;
    v_dedup_count        NUMBER        DEFAULT 0;
    v_null_weight_count  NUMBER        DEFAULT 0;
    v_invalid_cert_count NUMBER        DEFAULT 0;
    v_error_msg          VARCHAR       DEFAULT NULL;
    v_status             VARCHAR       DEFAULT 'SUCCESS';
BEGIN

    -- =========================================================
    -- Section 1: Watermark Lookup
    -- =========================================================
    IF (P_MODE = 'FULL') THEN
        v_load_type := 'FULL';
    ELSE
        SELECT MAX(LAST_LOAD_TIMESTAMP)
          INTO :v_last_ts
          FROM IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG
         WHERE PIPELINE_NAME = 'LOAD_AEROSPACE_PARTS'
           AND STATUS        = 'SUCCESS';

        IF (:v_last_ts IS NULL) THEN
            v_load_type := 'FULL';
        END IF;
    END IF;

    -- =========================================================
    -- Section 2: Deduplication, Exclusion Filter & Transformations
    -- =========================================================
    CREATE OR REPLACE TEMPORARY TABLE TMP_AEROSPACE_STAGED AS
    WITH DEDUPED AS (
        SELECT
            PART_NUMBER,
            PART_NAME,
            UPPER(MANUFACTURER)                                       AS MANUFACTURER,
            CATEGORY,
            CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END    AS WEIGHT_KG,
            ROUND(UNIT_PRICE_USD, 2)                                  AS UNIT_PRICE_USD,
            LIFECYCLE_STATUS,
            INSTALLATION_DATE,
            CERTIFICATION_STATUS,
            LEAD_TIME_DAYS,
            UPDATED_AT,
            CASE
                WHEN CERTIFICATION_STATUS = 'Pending'
                     AND LEAD_TIME_DAYS > 90                          THEN 'High'
                WHEN CERTIFICATION_STATUS = 'Pending'
                     OR  LEAD_TIME_DAYS > 120                         THEN 'Medium'
                WHEN CERTIFICATION_STATUS IN ('FAA', 'EASA', 'Dual')
                     AND LEAD_TIME_DAYS <= 60                         THEN 'Low'
                ELSE 'Medium'
            END                                                       AS RISK_SCORE,
            CURRENT_TIMESTAMP()                                       AS ETL_LOAD_TIMESTAMP,
            ROW_NUMBER() OVER (
                PARTITION BY PART_NUMBER
                ORDER BY UPDATED_AT DESC NULLS LAST
            )                                                         AS RN
        FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
        WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
          AND NOT (
                LIFECYCLE_STATUS  = 'End of Life'
                AND INSTALLATION_DATE < DATEADD(YEAR, -3, CURRENT_DATE())
              )
    )
    SELECT
        PART_NUMBER, PART_NAME, MANUFACTURER, CATEGORY,
        WEIGHT_KG, UNIT_PRICE_USD, LIFECYCLE_STATUS, INSTALLATION_DATE,
        CERTIFICATION_STATUS, LEAD_TIME_DAYS, UPDATED_AT,
        RISK_SCORE, ETL_LOAD_TIMESTAMP
    FROM DEDUPED
    WHERE RN = 1;

    -- =========================================================
    -- Section 3: Pre-Merge Metrics
    -- =========================================================
    SELECT COUNT(*)
      INTO :v_source_count
      FROM TMP_AEROSPACE_STAGED;

    SELECT COUNT(*)
      INTO :v_updated_count
      FROM TMP_AEROSPACE_STAGED  SRC
      JOIN IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
        ON TGT.PART_NUMBER = SRC.PART_NUMBER;

    v_inserted_count := v_source_count - v_updated_count;

    SELECT COUNT(*)
      INTO :v_dedup_count
      FROM (
           SELECT PART_NUMBER
             FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
            WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
            GROUP BY PART_NUMBER
           HAVING COUNT(*) > 1
           ) DUP;

    SELECT COUNT(*)
      INTO :v_null_weight_count
      FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
     WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
       AND WEIGHT_KG IS NOT NULL
       AND WEIGHT_KG <= 0;

    SELECT COUNT(*)
      INTO :v_excluded_count
      FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
     WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
       AND LIFECYCLE_STATUS   = 'End of Life'
       AND INSTALLATION_DATE  < DATEADD(YEAR, -3, CURRENT_DATE());

    SELECT COUNT(*)
      INTO :v_invalid_cert_count
      FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
     WHERE (:v_last_ts IS NULL OR UPDATED_AT > :v_last_ts)
       AND COALESCE(CERTIFICATION_STATUS, 'UNKNOWN')
           NOT IN ('Pending', 'FAA', 'EASA', 'Dual');

    -- =========================================================
    -- Section 4: SCD Type 1 MERGE
    -- =========================================================
    MERGE INTO IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET AS TGT
    USING TMP_AEROSPACE_STAGED AS SRC
       ON TGT.PART_NUMBER = SRC.PART_NUMBER
    WHEN MATCHED THEN
        UPDATE SET
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
            TGT.RISK_SCORE           = SRC.RISK_SCORE,
            TGT.ETL_LOAD_TIMESTAMP   = SRC.ETL_LOAD_TIMESTAMP
    WHEN NOT MATCHED THEN
        INSERT (
            PART_NUMBER, PART_NAME, MANUFACTURER, CATEGORY,
            WEIGHT_KG, UNIT_PRICE_USD, LIFECYCLE_STATUS, INSTALLATION_DATE,
            CERTIFICATION_STATUS, LEAD_TIME_DAYS, UPDATED_AT,
            RISK_SCORE, ETL_LOAD_TIMESTAMP
        )
        VALUES (
            SRC.PART_NUMBER, SRC.PART_NAME, SRC.MANUFACTURER, SRC.CATEGORY,
            SRC.WEIGHT_KG, SRC.UNIT_PRICE_USD, SRC.LIFECYCLE_STATUS, SRC.INSTALLATION_DATE,
            SRC.CERTIFICATION_STATUS, SRC.LEAD_TIME_DAYS, SRC.UPDATED_AT,
            SRC.RISK_SCORE, SRC.ETL_LOAD_TIMESTAMP
        );

    -- =========================================================
    -- Section 5: Soft Delete
    -- =========================================================
    UPDATE IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET TGT
    SET    TGT.LIFECYCLE_STATUS   = 'Decommissioned',
           TGT.ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE  TGT.LIFECYCLE_STATUS  <> 'Decommissioned'
      AND  TGT.PART_NUMBER NOT IN (
               SELECT PART_NUMBER
               FROM   IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_SOURCE
           );

    v_deleted_count := SQLROWCOUNT;

    -- =========================================================
    -- Section 6: Post-Merge Target Count
    -- =========================================================
    SELECT COUNT(*)
      INTO :v_target_count
      FROM IDEA_2_DB.PUBLIC.AEROSPACE_PARTS_TARGET;

    -- =========================================================
    -- Section 7: Reconciliation Log
    -- =========================================================
    INSERT INTO IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
        RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, LOAD_TYPE,
        SOURCE_COUNT, TARGET_COUNT, INSERTED_COUNT, UPDATED_COUNT,
        DELETED_COUNT, EXCLUDED_COUNT, DEDUP_COUNT,
        NULL_WEIGHT_COUNT, INVALID_CERT_COUNT,
        STATUS, ERROR_MESSAGE, LAST_LOAD_TIMESTAMP, RUN_DURATION_SECONDS
    )
    VALUES (
        :v_run_id, 'LOAD_AEROSPACE_PARTS', :v_run_start, :v_load_type,
        :v_source_count, :v_target_count, :v_inserted_count, :v_updated_count,
        :v_deleted_count, :v_excluded_count, :v_dedup_count,
        :v_null_weight_count, :v_invalid_cert_count,
        :v_status, :v_error_msg, CURRENT_TIMESTAMP(),
        DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
    );

    RETURN 'SUCCESS | RunID: ' || :v_run_id
        || ' | LoadType: '    || :v_load_type
        || ' | Source: '      || TO_VARCHAR(:v_source_count)
        || ' | Inserted: '    || TO_VARCHAR(:v_inserted_count)
        || ' | Updated: '     || TO_VARCHAR(:v_updated_count)
        || ' | SoftDeleted: ' || TO_VARCHAR(:v_deleted_count);

EXCEPTION
    WHEN OTHER THEN
        v_status    := 'FAILED';
        v_error_msg := SQLERRM;
        INSERT INTO IDEA_2_DB.PUBLIC.ETL_RECONCILIATION_LOG (
            RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, LOAD_TYPE,
            SOURCE_COUNT, TARGET_COUNT, INSERTED_COUNT, UPDATED_COUNT,
            DELETED_COUNT, EXCLUDED_COUNT, DEDUP_COUNT,
            NULL_WEIGHT_COUNT, INVALID_CERT_COUNT,
            STATUS, ERROR_MESSAGE, LAST_LOAD_TIMESTAMP, RUN_DURATION_SECONDS
        )
        VALUES (
            :v_run_id, 'LOAD_AEROSPACE_PARTS', :v_run_start, :v_load_type,
            :v_source_count, 0, 0, 0, 0, 0, 0, 0, 0,
            :v_status, :v_error_msg, CURRENT_TIMESTAMP(),
            DATEDIFF('second', :v_run_start, CURRENT_TIMESTAMP())
        );
        RAISE;
END;
$$