USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer
CREATE TABLE IF NOT EXISTS staging.tax (
    id TEXT,
    name TEXT,
    pharmacy_id TEXT,
    description TEXT,
    total TEXT,
    year_month_value TEXT,
    year TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS staging.tax_stm
ON TABLE staging.tax;

-- Bronze Layer
CREATE TABLE IF NOT EXISTS bronze.tax (
    id TEXT,
    name TEXT,
    pharmacy_id TEXT,
    description TEXT,
    total TEXT,
    year_month_value TEXT,
    year TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS bronze.tax_stm
ON TABLE bronze.tax;

-- Silver Layer
CREATE TABLE IF NOT EXISTS silver.tax (
    tax_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    tax_id NUMBER UNIQUE NOT NULL,
    name STRING NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    description STRING,
    total NUMERIC(10,2) NOT NULL,
    year_month_value STRING NOT NULL,
    year NUMBER NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE STREAM IF NOT EXISTS silver.tax_stm
ON TABLE silver.tax;

-- Gold Layer (SCD Type 2 Target)
CREATE TABLE IF NOT EXISTS gold.tax_dim (
    tax_hk STRING PRIMARY KEY,
    tax_id NUMBER NOT NULL, -- Removed UNIQUE constraint to allow SCD2 historical tracking
    name STRING NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    description STRING,
    total NUMERIC(10,2) NOT NULL,
    year_month_value STRING NOT NULL,
    year NUMBER NOT NULL,
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

CREATE OR REPLACE PROCEDURE star_schema.tax_procedure()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer: Ingest raw tax data from external stage
    COPY INTO staging.tax (
        id, name, pharmacy_id, description, total, year_month_value, year,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS name,
            t.$3::TEXT AS pharmacy_id,
            t.$4::TEXT AS description,
            t.$5::TEXT AS total,
            t.$6::TEXT AS year_month_value,
            t.$7::TEXT AS year,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/taxes AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer: Upsert raw data from Staging stream
    MERGE INTO bronze.tax AS target
    USING staging.tax_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.pharmacy_id != source.pharmacy_id OR
        target.description != source.description OR
        target.total != source.total OR
        target.year_month_value != source.year_month_value OR
        target.year != source.year
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.pharmacy_id = source.pharmacy_id,
            target.description = source.description,
            target.total = source.total,
            target.year_month_value = source.year_month_value,
            target.year = source.year,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, name, pharmacy_id, description, total, year_month_value, year,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.name, source.pharmacy_id, source.description, source.total, source.year_month_value, source.year,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer: Cleanse, cast data types, and upsert from Bronze stream
    MERGE INTO silver.tax AS target
    USING (
        SELECT 
            id::NUMBER AS tax_id,
            name::STRING AS name,
            pharmacy_id::NUMBER AS pharmacy_id,
            description::STRING AS description,
            total::NUMERIC(10,2) AS total,
            year_month_value::STRING AS year_month_value,
            year::NUMBER AS year,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.tax_stm
    ) AS source
    ON target.tax_id = source.tax_id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.pharmacy_id != source.pharmacy_id OR
        target.description != source.description OR
        target.total != source.total OR
        target.year_month_value != source.year_month_value OR
        target.year != source.year
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.pharmacy_id = source.pharmacy_id,
            target.description = source.description,
            target.total = source.total,
            target.year_month_value = source.year_month_value,
            target.year = source.year,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            tax_id, name, pharmacy_id, description, total, year_month_value, year,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.tax_id, source.name, source.pharmacy_id, source.description, source.total, source.year_month_value, source.year,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer: Standard SCD Type 2 pattern from Silver stream
    MERGE INTO gold.tax_dim AS target
    USING (
        -- Pass-through stream changes
        SELECT 
            tax_id, name, pharmacy_id, description, total, year_month_value, year,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.tax_stm

        UNION ALL

        -- Duplicate UPDATE stream events as new inserts to trigger record creation
        SELECT 
            tax_id, name, pharmacy_id, description, total, year_month_value, year,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.tax_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.tax_id = source.tax_id
       AND target.is_current = TRUE

    -- Expire current active row on update/delete
    WHEN MATCHED AND source.METADATA$ACTION = 'DELETE' THEN
        UPDATE SET
            target.eff_end_ts = CURRENT_TIMESTAMP(),
            target.is_current = FALSE

    -- Insert new current record for new rows or updated rows
    WHEN NOT MATCHED AND source.METADATA$ACTION = 'INSERT' THEN
        INSERT (
            tax_hk,
            tax_id, name, pharmacy_id, description, total, year_month_value, year,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.name, ''), COALESCE(source.total::TEXT, ''))),
            source.tax_id, source.name, source.pharmacy_id, source.description, source.total, source.year_month_value, source.year,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'tax staging, bronze, silver, and gold pipeline executed successfully in tax_procedure';
END;

-- Execute Procedure
CALL star_schema.tax_procedure();