USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer
CREATE TABLE IF NOT EXISTS staging.cart (
    id TEXT,
    pharmacy_id TEXT,
    total TEXT,
    profit TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS staging.cart_stm
ON TABLE staging.cart;

-- Bronze Layer
CREATE TABLE IF NOT EXISTS bronze.cart (
    id TEXT,
    pharmacy_id TEXT,
    total TEXT,
    profit TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS bronze.cart_stm
ON TABLE bronze.cart;

-- Silver Layer
CREATE TABLE IF NOT EXISTS silver.cart (
    cart_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    cart_id NUMBER UNIQUE NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    profit NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS silver.cart_stm
ON TABLE silver.cart;

-- Gold Layer (SCD Type 2 Target)
CREATE TABLE IF NOT EXISTS gold.cart_dim (
    cart_hk STRING PRIMARY KEY,
    cart_id NUMBER NOT NULL, -- Removed UNIQUE constraint to allow SCD2 history tracking
    pharmacy_id NUMBER NOT NULL,
    total NUMERIC(10,2) NOT NULL,
    profit NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ NOT NULL,
    eff_start_ts TIMESTAMP_TZ NOT NULL,
    eff_end_ts TIMESTAMP_TZ,
    is_current BOOLEAN NOT NULL
);


-- ============================================================================
-- STORED PROCEDURE DEFINITION
-- ============================================================================

CREATE OR REPLACE PROCEDURE star_schema.cart_procedure()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer: Ingest raw data from external stage
    COPY INTO staging.cart (
        id, pharmacy_id, total, profit, created_at,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS pharmacy_id,
            t.$3::TEXT AS total,
            t.$4::TEXT AS profit,
            t.$5::TEXT AS created_at,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/carts AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer: Upsert raw data from Staging stream
    MERGE INTO bronze.cart AS target
    USING staging.cart_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.pharmacy_id != source.pharmacy_id OR
        target.total != source.total OR
        target.profit != source.profit OR
        target.created_at != source.created_at
    ) THEN
        UPDATE SET
            target.pharmacy_id = source.pharmacy_id,
            target.total = source.total,
            target.profit = source.profit,
            target.created_at = source.created_at,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, pharmacy_id, total, profit, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.pharmacy_id, source.total, source.profit, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer: Cleanse, cast, and upsert from Bronze stream
    MERGE INTO silver.cart AS target
    USING (
        SELECT 
            id::NUMBER AS cart_id,
            pharmacy_id::NUMBER AS pharmacy_id,
            total::NUMERIC(10,2) AS total,
            profit::NUMERIC(10,2) AS profit,
            created_at::TIMESTAMP_TZ AS created_at,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.cart_stm
    ) AS source
    ON target.cart_id = source.cart_id
    WHEN MATCHED AND (
        target.total != source.total OR
        target.profit != source.profit
    ) THEN
        UPDATE SET
            target.pharmacy_id = source.pharmacy_id,
            target.total = source.total,
            target.profit = source.profit,
            target.created_at = source.created_at,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            cart_id, pharmacy_id, total, profit, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.cart_id, source.pharmacy_id, source.total, source.profit, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer: Standard SCD Type 2 pattern from Silver stream
    MERGE INTO gold.cart_dim AS target
    USING (
        -- Pass-through stream changes
        SELECT 
            cart_id, pharmacy_id, total, profit, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.cart_stm

        UNION ALL

        -- Duplicate UPDATE stream events as new inserts to trigger row creation
        SELECT 
            cart_id, pharmacy_id, total, profit, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.cart_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.cart_id = source.cart_id
       AND target.is_current = TRUE

    -- Expire current active row on update/delete
    WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            target.eff_end_ts = CURRENT_TIMESTAMP(),
            target.is_current = FALSE

    -- Insert new current record for new rows or updated rows
    WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            cart_hk,
            cart_id, pharmacy_id, total, profit, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.total::TEXT, ''), COALESCE(source.profit::TEXT, ''))),
            source.cart_id, source.pharmacy_id, source.total, source.profit, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'cart staging, bronze, silver, and gold pipeline executed successfully in cart_procedure';
END;

-- Execute Procedure
CALL star_schema.cart_procedure();