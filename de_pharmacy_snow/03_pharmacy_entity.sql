USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.pharmacies (
    id TEXT,
    name TEXT,
    opening_time TEXT,
    closing_time TEXT,
    is_open TEXT,
    user_id TEXT,
    cart_id TEXT,
    address_id TEXT,
    created_at TEXT,
    is_active TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM staging.pharmacies_stm
ON TABLE staging.pharmacies;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.pharmacies (
    id TEXT,
    name TEXT,
    opening_time TEXT,
    closing_time TEXT,
    is_open TEXT,
    user_id TEXT,
    cart_id TEXT,
    address_id TEXT,
    created_at TEXT,
    is_active TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM bronze.pharmacies_stm
ON TABLE bronze.pharmacies;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.pharmacies (
    pharmacy_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    pharmacy_id NUMBER UNIQUE NOT NULL,
    name STRING NOT NULL,
    opening_time TIME NOT NULL,
    closing_time TIME NOT NULL,
    is_open BOOLEAN NOT NULL,
    user_id NUMBER,
    cart_id NUMBER,
    address_id NUMBER,
    created_at TIMESTAMP_TZ NOT NULL,
    is_active BOOLEAN NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM silver.pharmacies_stm
ON TABLE silver.pharmacies;

-- Gold Layer (SCD Type 2 Target):
CREATE OR REPLACE TABLE gold.pharmacy_dim (
    pharmacy_hk STRING PRIMARY KEY,
    pharmacy_id NUMBER NOT NULL,
    name STRING NOT NULL,
    opening_time TIME NOT NULL,
    closing_time TIME NOT NULL,
    is_open BOOLEAN NOT NULL,
    user_id NUMBER,
    cart_id NUMBER,
    address_id NUMBER,
    created_at TIMESTAMP_TZ NOT NULL,
    is_active BOOLEAN NOT NULL,
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

CREATE OR REPLACE PROCEDURE star_schema.pharmacy_procedure()
RETURNS STRING 
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.pharmacies (
        id, name, opening_time, closing_time, is_open, user_id, cart_id, address_id, created_at, is_active,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS name,
            t.$3::TEXT AS opening_time,
            t.$4::TEXT AS closing_time,
            t.$5::TEXT AS is_open,
            t.$6::TEXT AS user_id,
            t.$7::TEXT AS cart_id,
            t.$8::TEXT AS address_id,
            t.$9::TEXT AS created_at,
            t.$10::TEXT AS is_active,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/initial/pharmacies AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.pharmacies AS target
    USING staging.pharmacies_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.opening_time != source.opening_time OR
        target.closing_time != source.closing_time OR
        target.is_open != source.is_open OR
        target.user_id != source.user_id OR
        target.cart_id != source.cart_id OR
        target.address_id != source.address_id OR
        target.created_at != source.created_at OR
        target.is_active != source.is_active
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.opening_time = source.opening_time,
            target.closing_time = source.closing_time,
            target.is_open = source.is_open,
            target.user_id = source.user_id,
            target.cart_id = source.cart_id,
            target.address_id = source.address_id,
            target.created_at = source.created_at,
            target.is_active = source.is_active,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, name, opening_time, closing_time, is_open, user_id, cart_id, address_id, created_at, is_active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.name, source.opening_time, source.closing_time, source.is_open, source.user_id, source.cart_id, source.address_id, source.created_at, source.is_active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer (Merge & Typecasting from Bronze Stream)
    MERGE INTO silver.pharmacies AS target
    USING (
        SELECT 
            id::NUMBER AS pharmacy_id,
            name::STRING AS name,
            opening_time::TIME AS opening_time,
            closing_time::TIME AS closing_time,
            is_open::BOOLEAN AS is_open,
            user_id::NUMBER AS user_id,
            cart_id::NUMBER AS cart_id,
            address_id::NUMBER AS address_id,
            created_at::TIMESTAMP_TZ AS created_at,
            is_active::BOOLEAN AS is_active,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.pharmacies_stm
    ) AS source
    ON target.pharmacy_id = source.pharmacy_id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.opening_time != source.opening_time OR
        target.closing_time != source.closing_time OR
        target.is_open != source.is_open OR
        target.user_id != source.user_id OR
        target.cart_id != source.cart_id OR
        target.address_id != source.address_id OR
        target.created_at != source.created_at OR
        target.is_active != source.is_active
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.opening_time = source.opening_time,
            target.closing_time = source.closing_time,
            target.is_open = source.is_open,
            target.user_id = source.user_id,
            target.cart_id = source.cart_id,
            target.address_id = source.address_id,
            target.created_at = source.created_at,
            target.is_active = source.is_active,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._brz_copy_data_ts = source._brz_copy_data_ts,
            target._stg_file_md5 = source._stg_file_md5
    WHEN NOT MATCHED THEN
        INSERT (
            pharmacy_id, name, opening_time, closing_time, is_open, user_id, cart_id, address_id, created_at, is_active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.pharmacy_id, source.name, source.opening_time, source.closing_time, source.is_open, source.user_id, source.cart_id, source.address_id, source.created_at, source.is_active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer (SCD Type 2 Pattern from Silver Stream)
    MERGE INTO gold.pharmacy_dim AS target
    USING (
        SELECT 
            pharmacy_id, name, opening_time, closing_time, is_open, user_id, cart_id, address_id, created_at, is_active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.pharmacies_stm
        
        UNION ALL
        
        SELECT 
            pharmacy_id, name, opening_time, closing_time, is_open, user_id, cart_id, address_id, created_at, is_active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.pharmacies_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.pharmacy_id = source.pharmacy_id
    AND target.is_current = TRUE

    WHEN MATCHED AND (
        (source.METADATA$ACTION = 'DELETE' AND source.METADATA$ISUPDATE = 'TRUE')
        OR source.METADATA$ACTION = 'DELETE'
    ) THEN
        UPDATE SET
            target.eff_end_ts = CURRENT_TIMESTAMP(),
            target.is_current = FALSE

    WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            pharmacy_hk,
            pharmacy_id, name, opening_time, closing_time, is_open, user_id, cart_id, address_id, created_at, is_active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.pharmacy_id::TEXT, ''), COALESCE(source.name, ''), COALESCE(source.opening_time::TEXT, ''), COALESCE(source.closing_time::TEXT, ''), source.is_open::TEXT, source.is_active::TEXT)),
            source.pharmacy_id, source.name, source.opening_time, source.closing_time, source.is_open, source.user_id, source.cart_id, source.address_id, source.created_at, source.is_active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'pharmacies staging, bronze, silver, and gold pipeline executed successfully in pharmacy_procedure';
END;

-- Execute Procedure
CALL star_schema.pharmacy_procedure();