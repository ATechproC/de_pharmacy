USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.user (
    id TEXT,
    username TEXT,
    email TEXT,
    password TEXT,
    role TEXT,
    status TEXT,
    pharmacy_id TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM staging.user_stm
ON TABLE staging.user;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.user (
    id TEXT,
    username TEXT,
    email TEXT,
    password TEXT,
    role TEXT,
    status TEXT,
    pharmacy_id TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM bronze.user_stm
ON TABLE bronze.user;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.user (
    user_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    user_id NUMBER UNIQUE NOT NULL,
    username STRING NOT NULL,
    email STRING NOT NULL,
    password STRING NOT NULL,
    role STRING NOT NULL,
    status STRING NOT NULL,
    pharmacy_id NUMBER,
    created_at TIMESTAMP_TZ NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM silver.user_stm
ON TABLE silver.user;

-- Gold Layer (SCD Type 2 Target):
CREATE OR REPLACE TABLE gold.user_dim (
    user_hk STRING PRIMARY KEY,
    user_id NUMBER NOT NULL,
    username STRING NOT NULL,
    email STRING NOT NULL,
    password STRING NOT NULL,
    role STRING NOT NULL,
    status STRING NOT NULL,
    pharmacy_id NUMBER,
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

CREATE OR REPLACE PROCEDURE star_schema.user_procedure()
RETURNS STRING 
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.user (
        id, username, email, password, role, status, pharmacy_id, created_at,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS username,
            t.$3::TEXT AS email,
            t.$4::TEXT AS password,
            t.$5::TEXT AS role,
            t.$6::TEXT AS status,
            t.$7::TEXT AS pharmacy_id,
            t.$8::TEXT AS created_at,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/users AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.user AS target
    USING staging.user_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.username != source.username OR 
        target.email != source.email OR 
        target.password != source.password OR 
        target.role != source.role OR 
        target.status != source.status OR 
        target.pharmacy_id != source.pharmacy_id OR 
        target.created_at != source.created_at
    ) THEN
        UPDATE SET
            target.username = source.username,
            target.email = source.email,
            target.password = source.password,
            target.role = source.role,
            target.status = source.status,
            target.pharmacy_id = source.pharmacy_id,
            target.created_at = source.created_at,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, username, email, password, role, status, pharmacy_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.username, source.email, source.password, source.role, source.status, source.pharmacy_id, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer (Merge & Typecasting from Bronze Stream)
    MERGE INTO silver.user AS target
    USING (
        SELECT 
            id::NUMBER AS user_id,
            username,
            email,
            password,
            role,
            status,
            pharmacy_id::NUMBER AS pharmacy_id,
            created_at::TIMESTAMP_TZ AS created_at,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.user_stm
    ) AS source
    ON target.user_id = source.user_id
    WHEN MATCHED AND (
        target.username != source.username OR 
        target.email != source.email OR 
        target.password != source.password OR 
        target.role != source.role OR 
        target.status != source.status OR 
        target.pharmacy_id != source.pharmacy_id OR 
        target.created_at != source.created_at
    ) THEN
        UPDATE SET
            target.username = source.username,
            target.email = source.email,
            target.password = source.password,
            target.role = source.role,
            target.status = source.status,
            target.pharmacy_id = source.pharmacy_id,
            target.created_at = source.created_at,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            user_id, username, email, password, role, status, pharmacy_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.user_id, source.username, source.email, source.password, source.role, source.status, source.pharmacy_id, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer (SCD Type 2 Pattern from Silver Stream)
    MERGE INTO gold.user_dim AS target
    USING (
        SELECT 
            user_id, username, email, password, role, status, pharmacy_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.user_stm
        
        UNION ALL
        
        SELECT 
            user_id, username, email, password, role, status, pharmacy_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.user_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.user_id = source.user_id
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
            user_hk,
            user_id, username, email, password, role, status, pharmacy_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.username, ''), COALESCE(source.email, ''), COALESCE(source.role, ''), COALESCE(source.status, ''))),
            source.user_id, source.username, source.email, source.password, source.role, source.status, source.pharmacy_id, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'user staging, bronze, silver, and gold pipeline executed successfully in user_procedure';
END;

-- Execute Procedure
CALL star_schema.user_procedure();