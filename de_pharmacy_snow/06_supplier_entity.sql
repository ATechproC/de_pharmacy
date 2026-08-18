USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.supplier (
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

CREATE OR REPLACE STREAM staging.supplier_stm
ON TABLE staging.supplier;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.supplier (
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

CREATE OR REPLACE STREAM bronze.supplier_stm
ON TABLE bronze.supplier;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.supplier (
    supplier_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    supplier_id NUMBER UNIQUE NOT NULL,
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

CREATE OR REPLACE STREAM silver.supplier_stm
ON TABLE silver.supplier;

-- Gold Layer (SCD Type 2 Target):
CREATE OR REPLACE TABLE gold.supplier_dim (
    supplier_hk STRING PRIMARY KEY,
    supplier_id NUMBER NOT NULL,
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

CREATE OR REPLACE PROCEDURE star_schema.supplier_procedure()
RETURNS STRING 
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.supplier (
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
        FROM @common.csv_stage/suppliers AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.supplier AS target
    USING staging.supplier_stm AS source
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
    MERGE INTO silver.supplier AS target
    USING (
        SELECT 
            id::NUMBER AS supplier_id,
            name,
            pharmacy_id::NUMBER AS pharmacy_id,
            created_at::TIMESTAMP_TZ AS created_at,
            active::BOOLEAN AS active,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.supplier_stm
    ) AS source
    ON target.supplier_id = source.supplier_id
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
            supplier_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.supplier_id, source.name, source.pharmacy_id, source.created_at, source.active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer (SCD Type 2 Pattern from Silver Stream)
    MERGE INTO gold.supplier_dim AS target
    USING (
        SELECT 
            supplier_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.supplier_stm
        
        UNION ALL
        
        SELECT 
            supplier_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.supplier_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.supplier_id = source.supplier_id
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
            supplier_hk,
            supplier_id, name, pharmacy_id, created_at, active,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.name, ''), source.active::TEXT)),
            source.supplier_id, source.name, source.pharmacy_id, source.created_at, source.active,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'supplier staging, bronze, silver, and gold pipeline executed successfully in supplier_procedure';
END;

-- Execute Procedure
CALL star_schema.supplier_procedure();