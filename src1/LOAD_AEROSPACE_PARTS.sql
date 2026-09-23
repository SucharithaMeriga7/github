-- ============================================================
-- Object Name : LOAD_AEROSPACE_PARTS
-- Purpose     : Incremental SCD Type 1 pipeline — AEROSPACE_PARTS_SOURCE to AEROSPACE_PARTS_TARGET
--               with deduplication, transformations, RISK_SCORE derivation,
--               soft delete, exclusion filter, and mandatory reconciliation logging
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE OR REPLACE PROCEDURE LOAD_AEROSPACE_PARTS(P_MODE VARCHAR DEFAULT 'INCREMENTAL')
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS CALLER
AS
$$
DECLARE
    v_load_type          VARCHAR          DEFAULT 'UNKNOWN';
    v_start_ts           TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP();
    v_last_ts            TIMESTAMP_NTZ    DEFAULT '1000-01-01 00:00:00'::TIMESTAMP_NTZ;
    v_watermark_to       TIMESTAMP_NTZ    DEFAULT CURRENT_TIMESTAMP();
    v_source_count       NUMBER           DEFAULT 0;
    v_target_before      NUMBER           DEFAULT 0;
    v_post_merge_count   NUMBER           DEFAULT 0;
    v_inserted           NUMBER           DEFAULT 0;
    v_updated            NUMBER           DEFAULT 0;
    v_soft_deleted       NUMBER           DEFAULT 0;
    v_excluded           NUMBER           DEFAULT 0;
    v_duration           FLOAT            DEFAULT 0;
    v_status             VARCHAR          DEFAULT 'SUCCESS';
    v_error_msg          VARCHAR          DEFAULT NULL;
    v_result             VARCHAR          DEFAULT NULL;
BEGIN
    v_start_ts := CURRENT_TIMESTAMP();

    -- --------------------------------------------------------
    -- SECTION 1: Determine Load Type and Watermark
    -- --------------------------------------------------------
    IF (UPPER(P_MODE) = 'FULL') THEN
        v_load_type := 'FULL';
        v_last_ts   := '1000-01-01 00:00:00'::TIMESTAMP_NTZ;
    ELSE
        v_load_type := 'INCREMENTAL';
        SELECT COALESCE(MAX(WATERMARK_TO), '1000-01-01 00:00:00'::TIMESTAMP_NTZ)
          INTO :v_last_ts
          FROM ETL_RECONCILIATION_LOG
         WHERE SP_NAME = 'LOAD_AEROSPACE_PARTS'
           AND STATUS  = 'SUCCESS';
        IF (v_last_ts = '1000-01-01 00:00:00'::TIMESTAMP_NTZ) THEN
            v_load_type := 'FULL';
        END IF;
    END IF;

    v_watermark_to := CURRENT_TIMESTAMP();

    -- --------------------------------------------------------
    -- SECTION 2: Capture Pre-Load Metrics
    -- --------------------------------------------------------
    SELECT COUNT(*) INTO :v_target_before FROM AEROSPACE_PARTS_TARGET;

    SELECT COUNT(*) INTO :v_source_count
      FROM AEROSPACE_PARTS_SOURCE
     WHERE COALESCE(UPDATED_AT, CURRENT_TIMESTAMP()) > :v_last_ts;

    SELECT COUNT(*) INTO :v_excluded
      FROM AEROSPACE_PARTS_SOURCE
     WHERE COALESCE(UPDATED_AT, CURRENT_TIMESTAMP()) > :v_last_ts
       AND UPPER(LIFECYCLE_STATUS) = 'END OF LIFE'
       AND INSTALLATION_DATE < DATEADD(YEAR, -3, CURRENT_DATE());

    -- --------------------------------------------------------
    -- SECTION 3: SCD Type 1 MERGE — Deduplicate, Transform, Derive RISK_SCORE
    -- --------------------------------------------------------
    MERGE INTO AEROSPACE_PARTS_TARGET AS tgt
    USING (
        SELECT
            PART_NUMBER,
            PART_NAME,
            UPPER(MANUFACTURER)                                                      AS MANUFACTURER,
            CATEGORY,
            CASE WHEN WEIGHT_KG <= 0 THEN NULL ELSE WEIGHT_KG END                   AS WEIGHT_KG,
            ROUND(UNIT_PRICE_USD, 2)                                                 AS UNIT_PRICE_USD,
            LIFECYCLE_STATUS,
            INSTALLATION_DATE,
            CERTIFICATION_STATUS,
            LEAD_TIME_DAYS,
            UPDATED_AT,
            CASE
                WHEN CERTIFICATION_STATUS = 'Pending' AND LEAD_TIME_DAYS > 90       THEN 'High'
                WHEN CERTIFICATION_STATUS = 'Pending' OR  LEAD_TIME_DAYS > 120      THEN 'Medium'
                WHEN CERTIFICATION_STATUS IN ('FAA','EASA','Dual')
                     AND LEAD_TIME_DAYS <= 60                                        THEN 'Low'
                ELSE 'Medium'
            END                                                                      AS RISK_SCORE,
            CURRENT_TIMESTAMP()                                                      AS ETL_LOAD_TIMESTAMP
        FROM (
            SELECT *,
                   ROW_NUMBER() OVER (
                       PARTITION BY PART_NUMBER
                       ORDER BY COALESCE(UPDATED_AT, '1000-01-01'::TIMESTAMP_NTZ) DESC
                   ) AS RN
              FROM AEROSPACE_PARTS_SOURCE
             WHERE COALESCE(UPDATED_AT, CURRENT_TIMESTAMP()) > :v_last_ts
               AND NOT (
                        UPPER(LIFECYCLE_STATUS) = 'END OF LIFE'
                        AND INSTALLATION_DATE < DATEADD(YEAR, -3, CURRENT_DATE())
                       )
        ) AS deduped
        WHERE deduped.RN = 1
    ) AS src
    ON tgt.PART_NUMBER = src.PART_NUMBER
    WHEN MATCHED THEN UPDATE SET
        tgt.PART_NAME            = src.PART_NAME,
        tgt.MANUFACTURER         = src.MANUFACTURER,
        tgt.CATEGORY             = src.CATEGORY,
        tgt.WEIGHT_KG            = src.WEIGHT_KG,
        tgt.UNIT_PRICE_USD       = src.UNIT_PRICE_USD,
        tgt.LIFECYCLE_STATUS     = src.LIFECYCLE_STATUS,
        tgt.INSTALLATION_DATE    = src.INSTALLATION_DATE,
        tgt.CERTIFICATION_STATUS = src.CERTIFICATION_STATUS,
        tgt.LEAD_TIME_DAYS       = src.LEAD_TIME_DAYS,
        tgt.UPDATED_AT           = src.UPDATED_AT,
        tgt.RISK_SCORE           = src.RISK_SCORE,
        tgt.ETL_LOAD_TIMESTAMP   = src.ETL_LOAD_TIMESTAMP
    WHEN NOT MATCHED THEN INSERT (
        PART_NUMBER, PART_NAME, MANUFACTURER, CATEGORY,
        WEIGHT_KG, UNIT_PRICE_USD, LIFECYCLE_STATUS, INSTALLATION_DATE,
        CERTIFICATION_STATUS, LEAD_TIME_DAYS, UPDATED_AT,
        RISK_SCORE, ETL_LOAD_TIMESTAMP
    ) VALUES (
        src.PART_NUMBER, src.PART_NAME, src.MANUFACTURER, src.CATEGORY,
        src.WEIGHT_KG, src.UNIT_PRICE_USD, src.LIFECYCLE_STATUS, src.INSTALLATION_DATE,
        src.CERTIFICATION_STATUS, src.LEAD_TIME_DAYS, src.UPDATED_AT,
        src.RISK_SCORE, src.ETL_LOAD_TIMESTAMP
    );

    -- Compute inserted vs updated from post-merge count delta
    SELECT COUNT(*) INTO :v_post_merge_count FROM AEROSPACE_PARTS_TARGET;
    v_inserted := GREATEST(v_post_merge_count - v_target_before, 0);
    v_updated  := GREATEST((v_source_count - v_excluded) - v_inserted, 0);

    -- --------------------------------------------------------
    -- SECTION 4: Soft Delete — Mark Records Absent from Source as Decommissioned
    -- --------------------------------------------------------
    UPDATE AEROSPACE_PARTS_TARGET
       SET LIFECYCLE_STATUS   = 'Decommissioned',
           ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
     WHERE LIFECYCLE_STATUS != 'Decommissioned'
       AND PART_NUMBER NOT IN (SELECT PART_NUMBER FROM AEROSPACE_PARTS_SOURCE);

    SELECT COUNT(*) INTO :v_soft_deleted
      FROM AEROSPACE_PARTS_TARGET
     WHERE LIFECYCLE_STATUS  = 'Decommissioned'
       AND ETL_LOAD_TIMESTAMP >= :v_start_ts;

    -- --------------------------------------------------------
    -- SECTION 5: Reconciliation Log
    -- --------------------------------------------------------
    SELECT DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP()) / 1000.0
      INTO :v_duration;

    INSERT INTO ETL_RECONCILIATION_LOG (
        LOG_ID, SP_NAME, RUN_TIMESTAMP, LOAD_TYPE,
        WATERMARK_FROM, WATERMARK_TO,
        SOURCE_COUNT, TARGET_COUNT_BEFORE,
        RECORDS_INSERTED, RECORDS_UPDATED,
        RECORDS_SOFT_DELETED, RECORDS_EXCLUDED,
        STATUS, ERROR_MESSAGE, RUN_DURATION_SECS
    ) VALUES (
        UUID_STRING(), 'LOAD_AEROSPACE_PARTS', :v_start_ts, :v_load_type,
        :v_last_ts, :v_watermark_to,
        :v_source_count, :v_target_before,
        :v_inserted, :v_updated,
        :v_soft_deleted, :v_excluded,
        :v_status, :v_error_msg, :v_duration
    );

    v_result := 'SUCCESS | Load: '  || v_load_type::VARCHAR     ||
                ' | Source: '       || v_source_count::VARCHAR  ||
                ' | Inserted: '     || v_inserted::VARCHAR      ||
                ' | Updated: '      || v_updated::VARCHAR       ||
                ' | SoftDel: '      || v_soft_deleted::VARCHAR  ||
                ' | Excluded: '     || v_excluded::VARCHAR      ||
                ' | Secs: '         || v_duration::VARCHAR;
    RETURN v_result;

EXCEPTION
    WHEN OTHER THEN
        v_status    := 'FAILED';
        v_error_msg := SQLERRM;
        SELECT DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP()) / 1000.0
          INTO :v_duration;
        INSERT INTO ETL_RECONCILIATION_LOG (
            LOG_ID, SP_NAME, RUN_TIMESTAMP, LOAD_TYPE,
            WATERMARK_FROM, WATERMARK_TO,
            SOURCE_COUNT, TARGET_COUNT_BEFORE,
            RECORDS_INSERTED, RECORDS_UPDATED,
            RECORDS_SOFT_DELETED, RECORDS_EXCLUDED,
            STATUS, ERROR_MESSAGE, RUN_DURATION_SECS
        ) VALUES (
            UUID_STRING(), 'LOAD_AEROSPACE_PARTS', :v_start_ts, :v_load_type,
            :v_last_ts, CURRENT_TIMESTAMP(),
            :v_source_count, :v_target_before,
            0, 0, 0, 0,
            :v_status, :v_error_msg, :v_duration
        );
        RETURN 'FAILED: ' || v_error_msg;
END;
$$;