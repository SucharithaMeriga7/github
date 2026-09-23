-- ============================================================
-- Object Name : SP_LOAD_AEROSPACE_PARTS_SCD1
-- Purpose     : SCD Type 1 incremental pipeline — AEROSPACE_PARTS_SOURCE to AEROSPACE_PARTS_TARGET
--               Watermark-based incremental load, deduplication by PART_NUMBER,
--               CamelCase MANUFACTURER, WEIGHT_KG sentinel, RISK_SCORE derivation,
--               soft delete, and reconciliation logging to ETL_RECONCILIATION_LOG
-- Author      : SUCHARITHAS
-- Generated   : 2026-09-23
-- ============================================================
CREATE OR REPLACE PROCEDURE SP_LOAD_AEROSPACE_PARTS_SCD1(P_MODE VARCHAR DEFAULT 'INCREMENTAL')
RETURNS VARIANT
LANGUAGE SQL
AS
$$
DECLARE
    v_run_id            VARCHAR           DEFAULT UUID_STRING();
    v_pipeline_name     VARCHAR           DEFAULT 'SP_LOAD_AEROSPACE_PARTS_SCD1';
    v_start_ts          TIMESTAMP_NTZ     DEFAULT CURRENT_TIMESTAMP();
    v_last_ts           TIMESTAMP_NTZ;
    v_source_count      INTEGER           DEFAULT 0;
    v_target_count      INTEGER           DEFAULT 0;
    v_insert_count      INTEGER           DEFAULT 0;
    v_update_count      INTEGER           DEFAULT 0;
    v_soft_delete_count INTEGER           DEFAULT 0;
    v_rejection_count   INTEGER           DEFAULT 0;
    v_exec_seconds      FLOAT             DEFAULT 0;
    v_status            VARCHAR           DEFAULT 'SUCCESS';
    v_result            VARIANT;
BEGIN

    -- ============================================================
    -- Section 1: Watermark Lookup
    -- FULL mode overrides watermark; INCREMENTAL uses last SUCCESS run
    -- ============================================================
    IF (P_MODE = 'FULL') THEN
        v_last_ts := '1900-01-01 00:00:00'::TIMESTAMP_NTZ;
    ELSE
        SELECT COALESCE(MAX(RUN_TIMESTAMP), '1900-01-01 00:00:00'::TIMESTAMP_NTZ)
        INTO   :v_last_ts
        FROM   ETL_RECONCILIATION_LOG
        WHERE  PIPELINE_NAME = :v_pipeline_name
          AND  STATUS        = 'SUCCESS';
    END IF;

    -- ============================================================
    -- Section 2: Deduplication
    -- Retain only the latest record per PART_NUMBER from source
    -- Filter: UPDATED_AT > last successful run timestamp
    -- ============================================================
    CREATE OR REPLACE TEMPORARY TABLE AEROSPACE_PARTS_STAGE AS
    SELECT
        PART_NUMBER,
        PART_NAME,
        CATEGORY,
        INITCAP(MANUFACTURER)                                                  AS MANUFACTURER,
        IFF(WEIGHT_KG <= 0, -1, WEIGHT_KG)                                     AS WEIGHT_KG,
        CERTIFICATION_STATUS,
        LEAD_TIME_DAYS,
        ROUND(UNIT_PRICE_USD, 2)                                               AS UNIT_PRICE_USD,
        STOCK_QUANTITY,
        SUPPLIER_ID,
        UPDATED_AT,
        -- --------------------------------------------------------
        -- Section 3: Derived Column — RISK_SCORE
        -- Priority: High Risk > Medium Risk (Pending/LT>100) > Low Risk > Medium Risk (default)
        -- --------------------------------------------------------
        CASE
            WHEN CERTIFICATION_STATUS = 'Pending' AND LEAD_TIME_DAYS > 60
                THEN 'High Risk'
            WHEN CERTIFICATION_STATUS = 'Pending' OR  LEAD_TIME_DAYS > 100
                THEN 'Medium Risk'
            WHEN CERTIFICATION_STATUS IN ('FAA','EASA','Dual') AND LEAD_TIME_DAYS <= 60
                THEN 'Low Risk'
            ELSE 'Medium Risk'
        END                                                                    AS RISK_SCORE
    FROM (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY PART_NUMBER ORDER BY UPDATED_AT DESC) AS rn
        FROM   AEROSPACE_PARTS_SOURCE
        WHERE  UPDATED_AT > :v_last_ts
    ) src
    WHERE rn = 1;

    SELECT COUNT(*) INTO :v_source_count FROM AEROSPACE_PARTS_STAGE;

    -- ============================================================
    -- Section 4: Pre-Merge Count
    -- Determine inserts vs updates before MERGE executes
    -- ============================================================
    SELECT
        COUNT(CASE WHEN tgt.PART_NUMBER IS NOT NULL THEN 1 END),
        COUNT(CASE WHEN tgt.PART_NUMBER IS NULL     THEN 1 END)
    INTO :v_update_count, :v_insert_count
    FROM      AEROSPACE_PARTS_STAGE stg
    LEFT JOIN AEROSPACE_PARTS_TARGET tgt
           ON stg.PART_NUMBER = tgt.PART_NUMBER;

    -- ============================================================
    -- Section 5: SCD Type 1 MERGE
    -- Overwrite existing records; insert new records
    -- ============================================================
    MERGE INTO AEROSPACE_PARTS_TARGET tgt
    USING AEROSPACE_PARTS_STAGE stg
       ON tgt.PART_NUMBER = stg.PART_NUMBER
    WHEN MATCHED THEN UPDATE SET
        tgt.PART_NAME            = stg.PART_NAME,
        tgt.CATEGORY             = stg.CATEGORY,
        tgt.MANUFACTURER         = stg.MANUFACTURER,
        tgt.WEIGHT_KG            = stg.WEIGHT_KG,
        tgt.CERTIFICATION_STATUS = stg.CERTIFICATION_STATUS,
        tgt.LEAD_TIME_DAYS       = stg.LEAD_TIME_DAYS,
        tgt.UNIT_PRICE_USD       = stg.UNIT_PRICE_USD,
        tgt.STOCK_QUANTITY       = stg.STOCK_QUANTITY,
        tgt.SUPPLIER_ID          = stg.SUPPLIER_ID,
        tgt.UPDATED_AT           = stg.UPDATED_AT,
        tgt.RISK_SCORE           = stg.RISK_SCORE,
        tgt.RECORD_STATUS        = 'Active',
        tgt.ETL_LOAD_TIMESTAMP   = CURRENT_TIMESTAMP()
    WHEN NOT MATCHED THEN INSERT (
        PART_NUMBER, PART_NAME, CATEGORY, MANUFACTURER, WEIGHT_KG,
        CERTIFICATION_STATUS, LEAD_TIME_DAYS, UNIT_PRICE_USD, STOCK_QUANTITY,
        SUPPLIER_ID, UPDATED_AT, RISK_SCORE, RECORD_STATUS, ETL_LOAD_TIMESTAMP
    ) VALUES (
        stg.PART_NUMBER, stg.PART_NAME, stg.CATEGORY, stg.MANUFACTURER, stg.WEIGHT_KG,
        stg.CERTIFICATION_STATUS, stg.LEAD_TIME_DAYS, stg.UNIT_PRICE_USD, stg.STOCK_QUANTITY,
        stg.SUPPLIER_ID, stg.UPDATED_AT, stg.RISK_SCORE, 'Active', CURRENT_TIMESTAMP()
    );

    SELECT COUNT(*) INTO :v_target_count FROM AEROSPACE_PARTS_TARGET;

    -- ============================================================
    -- Section 6: Soft Delete
    -- Mark records absent from source as Decommissioned
    -- ============================================================
    UPDATE AEROSPACE_PARTS_TARGET
    SET    RECORD_STATUS      = 'Decommissioned',
           ETL_LOAD_TIMESTAMP = CURRENT_TIMESTAMP()
    WHERE  RECORD_STATUS  != 'Decommissioned'
      AND  PART_NUMBER NOT IN (SELECT PART_NUMBER FROM AEROSPACE_PARTS_SOURCE);

    SELECT COUNT(*) INTO :v_soft_delete_count
    FROM   AEROSPACE_PARTS_TARGET
    WHERE  RECORD_STATUS      = 'Decommissioned'
      AND  ETL_LOAD_TIMESTAMP >= :v_start_ts;

    -- ============================================================
    -- Section 7: Reconciliation Logging — Mandatory per run
    -- ============================================================
    v_exec_seconds := DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP()) / 1000.0;

    INSERT INTO ETL_RECONCILIATION_LOG (
        RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, WATERMARK_USED,
        SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT,
        SOFT_DELETE_COUNT, REJECTION_COUNT, STATUS, EXECUTION_SECONDS
    ) VALUES (
        :v_run_id,  :v_pipeline_name, :v_start_ts,     :v_last_ts,
        :v_source_count, :v_target_count, :v_insert_count, :v_update_count,
        :v_soft_delete_count, :v_rejection_count, :v_status, :v_exec_seconds
    );

    v_result := OBJECT_CONSTRUCT(
        'run_id',            :v_run_id,
        'status',            :v_status,
        'mode',              P_MODE,
        'watermark_used',    :v_last_ts::VARCHAR,
        'source_count',      :v_source_count,
        'target_count',      :v_target_count,
        'insert_count',      :v_insert_count,
        'update_count',      :v_update_count,
        'soft_delete_count', :v_soft_delete_count,
        'rejection_count',   :v_rejection_count,
        'execution_seconds', :v_exec_seconds
    );

    RETURN :v_result;

EXCEPTION
    WHEN OTHER THEN
        -- --------------------------------------------------------
        -- Section 8: Error Handling — Log failure, re-raise
        -- --------------------------------------------------------
        v_exec_seconds := DATEDIFF('millisecond', :v_start_ts, CURRENT_TIMESTAMP()) / 1000.0;

        INSERT INTO ETL_RECONCILIATION_LOG (
            RUN_ID, PIPELINE_NAME, RUN_TIMESTAMP, WATERMARK_USED,
            SOURCE_COUNT, TARGET_COUNT, INSERT_COUNT, UPDATE_COUNT,
            SOFT_DELETE_COUNT, REJECTION_COUNT, STATUS, EXECUTION_SECONDS
        ) VALUES (
            :v_run_id,  :v_pipeline_name, :v_start_ts,     :v_last_ts,
            :v_source_count, :v_target_count, 0, 0,
            0, :v_rejection_count, 'FAILURE', :v_exec_seconds
        );

        RAISE;
END;
$$;