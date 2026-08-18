USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.order_item (
    id TEXT,
    medicine_id TEXT,
    quantity TEXT,
    total TEXT,
    profit TEXT,
    order_id TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM staging.order_item_stm
ON TABLE staging.order_item;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.order_item (
    id TEXT,
    medicine_id TEXT,
    quantity TEXT,
    total TEXT,
    profit TEXT,
    order_id TEXT,
    created_at TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM bronze.order_item_stm
ON TABLE bronze.order_item;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.order_item (
    order_item_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    order_item_id NUMBER UNIQUE NOT NULL,
    medicine_id NUMBER NOT NULL,
    quantity NUMBER NOT NULL,
    total DECIMAL(10,2) NOT NULL,
    profit DECIMAL(10,2) NOT NULL,
    order_id NUMBER NOT NULL,
    created_at TIMESTAMP_TZ NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM silver.order_item_stm
ON TABLE silver.order_item;


-- ============================================================================
-- STORED PROCEDURE DEFINITION
-- ============================================================================

CREATE OR REPLACE PROCEDURE star_schema.order_item_procedure()
RETURNS STRING
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.order_item (
        id, medicine_id, quantity, total, profit, order_id, created_at,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS medicine_id,
            t.$3::TEXT AS quantity,
            t.$4::TEXT AS total,
            t.$5::TEXT AS profit,
            t.$6::TEXT AS order_id,
            t.$7::TEXT AS created_at,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/order_items AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.order_item AS target
    USING staging.order_item_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.medicine_id != source.medicine_id OR
        target.quantity != source.quantity OR
        target.total != source.total OR
        target.profit != source.profit OR
        target.order_id != source.order_id OR
        target.created_at != source.created_at
    ) THEN
        UPDATE SET
            target.medicine_id = source.medicine_id,
            target.quantity = source.quantity,
            target.total = source.total,
            target.profit = source.profit,
            target.order_id = source.order_id,
            target.created_at = source.created_at,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, medicine_id, quantity, total, profit, order_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.medicine_id, source.quantity, source.total, source.profit, source.order_id, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer (Merge & Typecasting from Bronze Stream)
    MERGE INTO silver.order_item AS target
    USING (
        SELECT 
            id::NUMBER AS order_item_id,
            medicine_id::NUMBER AS medicine_id,
            quantity::NUMBER AS quantity,
            total::DECIMAL(10,2) AS total,
            profit::DECIMAL(10,2) AS profit,
            order_id::NUMBER AS order_id,
            created_at::TIMESTAMP_TZ AS created_at,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.order_item_stm
    ) AS source
    ON target.order_item_id = source.order_item_id
    WHEN MATCHED AND (
        target.medicine_id != source.medicine_id OR
        target.quantity != source.quantity OR
        target.total != source.total OR
        target.profit != source.profit OR
        target.order_id != source.order_id OR
        target.created_at != source.created_at
    ) THEN
        UPDATE SET
            target.medicine_id = source.medicine_id,
            target.quantity = source.quantity,
            target.total = source.total,
            target.profit = source.profit,
            target.order_id = source.order_id,
            target.created_at = source.created_at,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            order_item_id, medicine_id, quantity, total, profit, order_id, created_at,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.order_item_id, source.medicine_id, source.quantity, source.total, source.profit, source.order_id, source.created_at,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    RETURN 'order_item staging, bronze, and silver pipeline executed successfully in order_item_procedure';
END;

-- Execute Procedure
CALL star_schema.order_item_procedure();