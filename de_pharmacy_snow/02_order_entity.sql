USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.orders (
    id TEXT,
    total TEXT,
    profit TEXT,
    pharmacy_id TEXT,
    pharmacist_id TEXT,
    day TEXT,
    date TEXT,
    year TEXT,
    year_month_value TEXT,
    week_of_month TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM staging.orders_stm
ON TABLE staging.orders;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.orders (
    id TEXT,
    total TEXT,
    profit TEXT,
    pharmacy_id TEXT,
    pharmacist_id TEXT,
    created_at TEXT,
    day TEXT,
    date TEXT,
    year TEXT,
    year_month_value TEXT,
    week_of_month TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM bronze.orders_stm
ON TABLE bronze.orders;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.orders (
    order_id NUMBER UNIQUE NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    profit DECIMAL(10,2) NOT NULL,
    pharmacy_id NUMBER NOT NULL,
    pharmacist_id NUMBER NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    day NUMBER NOT NULL,
    date DATE NOT NULL,
    year NUMBER NOT NULL,
    year_month_value STRING NOT NULL,
    week_of_month NUMBER NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM silver.orders_stm
ON TABLE silver.orders;


-- ============================================================================
-- STORED PROCEDURE DEFINITION
-- ============================================================================

CREATE OR REPLACE PROCEDURE star_schema.orders_procedure()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.orders (
        id, total, profit, pharmacy_id, pharmacist_id, 
        day, date, year, year_month_value, week_of_month, created_at,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS total,
            t.$3::TEXT AS profit,
            t.$4::TEXT AS pharmacy_id,
            t.$5::TEXT AS pharmacist_id,
            t.$6::TEXT AS day,
            t.$7::TEXT AS date,
            t.$8::TEXT AS year,
            t.$9::TEXT AS year_month_value,
            t.$10::TEXT AS week_of_month,
            t.$11::TEXT AS created_at,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/orders AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.orders AS target
    USING staging.orders_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.total != source.total OR
        target.profit != source.profit OR
        target.pharmacy_id != source.pharmacy_id OR
        target.pharmacist_id != source.pharmacist_id OR
        target.created_at != source.created_at OR
        target.day != source.day OR
        target.date != source.date OR
        target.year != source.year OR
        target.year_month_value != source.year_month_value OR
        target.week_of_month != source.week_of_month
    ) THEN
        UPDATE SET
            target.total = source.total,
            target.profit = source.profit,
            target.pharmacy_id = source.pharmacy_id,
            target.pharmacist_id = source.pharmacist_id,
            target.created_at = source.created_at,
            target.day = source.day,
            target.date = source.date,
            target.year = source.year,
            target.year_month_value = source.year_month_value,
            target.week_of_month = source.week_of_month,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, total, profit, pharmacy_id, pharmacist_id, created_at,
            day, date, year, year_month_value, week_of_month,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.total, source.profit, source.pharmacy_id, source.pharmacist_id, source.created_at,
            source.day, source.date, source.year, source.year_month_value, source.week_of_month,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer (Merge & Typecasting from Bronze Stream)
    MERGE INTO silver.orders AS target
    USING (
        SELECT 
            id::NUMBER AS order_id,
            total::DECIMAL(10,2) AS total,
            profit::DECIMAL(10,2) AS profit,
            pharmacy_id::NUMBER AS pharmacy_id,
            pharmacist_id::NUMBER AS pharmacist_id,
            created_at::TIMESTAMP_TZ AS created_at,
            day::NUMBER AS day,
            date::DATE AS date,
            year::NUMBER AS year,
            year_month_value::STRING AS year_month_value,
            week_of_month::NUMBER AS week_of_month,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.orders_stm
    ) AS source
    ON target.order_id = source.order_id
    WHEN MATCHED AND (
        target.total != source.total OR
        target.profit != source.profit OR
        target.pharmacy_id != source.pharmacy_id OR
        target.pharmacist_id != source.pharmacist_id OR
        target.created_at != source.created_at OR
        target.day != source.day OR
        target.date != source.date OR
        target.year != source.year OR
        target.year_month_value != source.year_month_value OR
        target.week_of_month != source.week_of_month
    ) THEN
        UPDATE SET
            target.total = source.total,
            target.profit = source.profit,
            target.pharmacy_id = source.pharmacy_id,
            target.pharmacist_id = source.pharmacist_id,
            target.created_at = source.created_at,
            target.day = source.day,
            target.date = source.date,
            target.year = source.year,
            target.year_month_value = source.year_month_value,
            target.week_of_month = source.week_of_month,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            order_id, total, profit, pharmacy_id, pharmacist_id, created_at,
            day, date, year, year_month_value, week_of_month,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.order_id, source.total, source.profit, source.pharmacy_id, source.pharmacist_id, source.created_at,
            source.day, source.date, source.year, source.year_month_value, source.week_of_month,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    RETURN 'orders staging, bronze, and silver pipeline executed successfully in orders_procedure';
END;

-- Execute Procedure
CALL star_schema.orders_procedure();