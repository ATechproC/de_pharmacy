USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.category (
    id TEXT,
    name TEXT,
    pharmacy_id TEXT,
    created_at TEXT,
    active TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM staging.category_stm
ON TABLE staging.category;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.category (
    id TEXT,
    name TEXT,
    pharmacy_id TEXT,
    created_at TEXT,
    active TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM bronze.category_stm
ON TABLE bronze.category;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.category (
    category_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    category_id NUMBER UNIQUE NOT NULL,
    name STRING NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    active BOOLEAN NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM silver.category_stm
ON TABLE silver.category;

-- Gold Layer (SCD Type 2 Target):
CREATE OR REPLACE TABLE gold.category_dim (
    category_hk STRING PRIMARY KEY,
    category_id NUMBER NOT NULL,
    name STRING NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    active BOOLEAN NOT NULL,
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

CREATE OR REPLACE PROCEDURE star_schema.category_procedure()
RETURNS STRING 
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.category (
        id, name, pharmacy_id, active, created_at,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS name,
            t.$3::TEXT AS pharmacy_id,
            t.$4::TEXT AS active,
            t.$5::TEXT AS created_at,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/categories AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.category AS target
    USING staging.category_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.pharmacy_id != source.pharmacy_id OR
        target.created_at != source.created_at OR
        target.active != source.active
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.pharmacy_id = source.pharmacy_id,
            target.created_at = source.created_at,
            target.active = source.active,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.name, source.pharmacy_id, source.created_at, source.active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer (Merge & Typecasting from Bronze Stream)
    MERGE INTO silver.category AS target
    USING (
        SELECT 
            id::NUMBER AS category_id,
            name,
            pharmacy_id::NUMBER AS pharmacy_id,
            created_at::TIMESTAMP_TZ AS created_at,
            active::BOOLEAN AS active,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.category_stm
    ) AS source
    ON target.category_id = source.category_id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.pharmacy_id != source.pharmacy_id OR
        target.created_at != source.created_at OR
        target.active != source.active
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.pharmacy_id = source.pharmacy_id,
            target.created_at = source.created_at,
            target.active = source.active,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            category_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.category_id, source.name, source.pharmacy_id, source.created_at, source.active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer (SCD Type 2 Pattern from Silver Stream)
    MERGE INTO gold.category_dim AS target
    USING (
        SELECT 
            category_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.category_stm
        
        UNION ALL
        
        SELECT 
            category_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.category_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.category_id = source.category_id
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
            category_hk,
            category_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.name, ''), source.active::TEXT)),
            source.category_id, source.name, source.pharmacy_id, source.created_at, source.active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'category staging, bronze, silver, and gold pipeline executed successfully in category_procedure';
END;

-- Execute Procedure
CALL star_schema.category_procedure();