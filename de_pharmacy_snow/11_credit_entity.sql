USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Bronze Layer
CREATE TABLE IF NOT EXISTS bronze.credit (
    id TEXT,
    client_id TEXT,
    name TEXT,
    total_amount TEXT,
    paid_amount TEXT,
    remaining_amount TEXT,
    phone TEXT,
    status TEXT,
    pharmacy_id TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS bronze.credit_stm
ON TABLE bronze.credit;

-- Silver Layer
CREATE TABLE IF NOT EXISTS silver.credit (
    credit_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    credit_id NUMBER UNIQUE NOT NULL,
    client_id STRING NOT NULL,
    name STRING NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    paid_amount NUMERIC(10,2) NOT NULL,
    remaining_amount NUMERIC(10,2) NOT NULL,
    phone STRING NOT NULL,
    status STRING NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS silver.credit_stm
ON TABLE silver.credit;

-- Gold Layer (SCD Type 2 Target)
CREATE TABLE IF NOT EXISTS gold.credit_dim (
    credit_hk STRING PRIMARY KEY,
    credit_id NUMBER NOT NULL, -- Removed UNIQUE constraint to support historical versions
    client_id STRING NOT NULL,
    name STRING NOT NULL,
    total_amount NUMERIC(10,2) NOT NULL,
    paid_amount NUMERIC(10,2) NOT NULL,
    remaining_amount NUMERIC(10,2) NOT NULL,
    phone STRING NOT NULL,
    status STRING NOT NULL,
    pharmacy_id NUMBER NOT NULL,
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

CREATE OR REPLACE PROCEDURE star_schema.credit_procedure()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

    -- 1. Bronze Layer: Load raw credit data from external stage
    COPY INTO bronze.credit (
        id, client_id, name, total_amount, paid_amount, remaining_amount, phone, status, pharmacy_id,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS client_id,
            t.$3::TEXT AS name,
            t.$4::TEXT AS total_amount,
            t.$5::TEXT AS paid_amount,
            t.$6::TEXT AS remaining_amount,
            t.$7::TEXT AS phone,
            t.$8::TEXT AS status,
            t.$9::TEXT AS pharmacy_id,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/credits AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Silver Layer: Cleanse, cast data types, and upsert from Bronze stream
    MERGE INTO silver.credit AS target
    USING (
        SELECT 
            id::NUMBER AS credit_id,
            client_id::STRING AS client_id,
            name::STRING AS name,
            total_amount::NUMERIC(10,2) AS total_amount,
            paid_amount::NUMERIC(10,2) AS paid_amount,
            remaining_amount::NUMERIC(10,2) AS remaining_amount,
            phone::STRING AS phone,
            status::STRING AS status,
            pharmacy_id::NUMBER AS pharmacy_id,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.credit_stm
    ) AS source
    ON target.credit_id = source.credit_id
    WHEN MATCHED AND (
        target.paid_amount != source.paid_amount OR 
        target.remaining_amount != source.remaining_amount OR 
        target.status != source.status
    ) THEN
        UPDATE SET
            target.client_id = source.client_id,
            target.name = source.name,
            target.total_amount = source.total_amount,
            target.paid_amount = source.paid_amount,
            target.remaining_amount = source.remaining_amount,
            target.phone = source.phone,
            target.status = source.status,
            target.pharmacy_id = source.pharmacy_id,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            credit_id, client_id, name, total_amount, paid_amount, remaining_amount, phone, status, pharmacy_id,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.credit_id, source.client_id, source.name, source.total_amount, source.paid_amount, source.remaining_amount, source.phone, source.status, source.pharmacy_id,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 3. Gold Layer: Standard SCD Type 2 pattern from Silver stream
    MERGE INTO gold.credit_dim AS target
    USING (
        -- Pass-through stream changes
        SELECT 
            credit_id, client_id, name, total_amount, paid_amount, remaining_amount, phone, status, pharmacy_id,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.credit_stm

        UNION ALL

        -- Duplicate UPDATE stream events as new inserts to trigger row creation
        SELECT 
            credit_id, client_id, name, total_amount, paid_amount, remaining_amount, phone, status, pharmacy_id,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.credit_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.credit_id = source.credit_id
       AND target.is_current = TRUE

    -- Expire current active row on update/delete
    WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            target.eff_end_ts = CURRENT_TIMESTAMP(),
            target.is_current = FALSE

    -- Insert new current record for new rows or updated rows
    WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            credit_hk,
            credit_id, client_id, name, total_amount, paid_amount, remaining_amount, phone, status, pharmacy_id,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.paid_amount::TEXT, ''), COALESCE(source.remaining_amount::TEXT, ''), COALESCE(source.status, ''))),
            source.credit_id, source.client_id, source.name, source.total_amount, source.paid_amount, source.remaining_amount, source.phone, source.status, source.pharmacy_id,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'credit bronze, silver, and gold pipeline executed successfully in credit_procedure';
END;

-- Execute Procedure
CALL star_schema.credit_procedure();