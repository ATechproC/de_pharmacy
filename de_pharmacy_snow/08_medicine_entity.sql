USE ROLE sysadmin;
USE WAREHOUSE compute_wh;
USE DATABASE pharmacy_db;

-- ============================================================================
-- DDL SETUP LAYER (TABLES & STREAMS)
-- ============================================================================

-- Staging Layer:
CREATE OR REPLACE TABLE staging.medicine (
    id TEXT,
    name TEXT,
    expired_date TEXT,
    purchase_price TEXT,
    selling_price TEXT,
    profit TEXT,
    quantity TEXT,
    batch_number TEXT,
    type TEXT,
    status TEXT,
    supplier_id TEXT,
    category_id TEXT,
    pharmacy_id TEXT,
    low TEXT,
    sold_quantity TEXT,
    active TEXT,
    in_stock TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM staging.medicine_stm
ON TABLE staging.medicine;

-- Bronze Layer:
CREATE OR REPLACE TABLE bronze.medicine (
    id TEXT,
    name TEXT,
    expired_date TEXT,
    purchase_price TEXT,
    selling_price TEXT,
    profit TEXT,
    quantity TEXT,
    batch_number TEXT,
    type TEXT,
    status TEXT,
    supplier_id TEXT,
    category_id TEXT,
    pharmacy_id TEXT,
    low TEXT,
    sold_quantity TEXT,
    active TEXT,
    in_stock TEXT,
    _stg_file_name TEXT,
    _stg_file_load_ts TIMESTAMP_TZ,
    _stg_file_md5 TEXT,
    _stg_copy_data_ts TIMESTAMP_TZ,
    _brz_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM bronze.medicine_stm
ON TABLE bronze.medicine;

-- Silver Layer:
CREATE OR REPLACE TABLE silver.medicine (
    medicine_sk NUMBER AUTOINCREMENT PRIMARY KEY,
    medicine_id NUMBER UNIQUE NOT NULL,
    name STRING NOT NULL,
    expired_date STRING NOT NULL,
    purchase_price NUMERIC(10,2) NOT NULL,
    selling_price NUMERIC(10,2) NOT NULL,
    profit NUMERIC(10,2) NOT NULL,
    quantity NUMBER NOT NULL,
    batch_number STRING NOT NULL,
    type STRING NOT NULL,
    status STRING NOT NULL,
    supplier_id NUMBER,
    category_id NUMBER,
    pharmacy_id NUMBER NOT NULL,
    low BOOLEAN NOT NULL,
    sold_quantity NUMBER NOT NULL,
    active BOOLEAN NOT NULL,
    in_stock BOOLEAN NOT NULL,
    _stg_file_name STRING NOT NULL,
    _stg_file_load_ts TIMESTAMP_TZ NOT NULL,
    _stg_file_md5 STRING NOT NULL,
    _brz_copy_data_ts TIMESTAMP_TZ NOT NULL,
    _slv_copy_data_ts TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE OR REPLACE STREAM silver.medicine_stm
ON TABLE silver.medicine;

-- Gold Layer (SCD Type 2 Target):
CREATE OR REPLACE TABLE gold.medicine_dim (
    medicine_hk STRING PRIMARY KEY,
    medicine_id NUMBER NOT NULL,
    name STRING NOT NULL,
    expired_date STRING NOT NULL,
    purchase_price NUMERIC(10,2) NOT NULL,
    selling_price NUMERIC(10,2) NOT NULL,
    profit NUMERIC(10,2) NOT NULL,
    quantity NUMBER NOT NULL,
    batch_number STRING NOT NULL,
    type STRING NOT NULL,
    status STRING NOT NULL,
    supplier_id NUMBER,
    category_id NUMBER,
    pharmacy_id NUMBER NOT NULL,
    low BOOLEAN NOT NULL,
    sold_quantity NUMBER NOT NULL,
    active BOOLEAN NOT NULL,
    in_stock BOOLEAN NOT NULL,
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

CREATE OR REPLACE PROCEDURE star_schema.medicine_procedure()
RETURNS STRING 
LANGUAGE SQL
AS
BEGIN

    -- 1. Staging Layer (Data Load)
    COPY INTO staging.medicine (
        id, name, expired_date, purchase_price, selling_price, quantity, batch_number, 
        type, profit, status, supplier_id, category_id, pharmacy_id, low, sold_quantity, active, in_stock,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    FROM (
        SELECT 
            t.$1::TEXT AS id,
            t.$2::TEXT AS name,
            t.$3::TEXT AS expired_date,
            t.$4::TEXT AS purchase_price,
            t.$5::TEXT AS selling_price,
            t.$6::TEXT AS quantity,
            t.$7::TEXT AS batch_number,
            t.$8::TEXT AS type,
            t.$9::TEXT AS profit,
            t.$10::TEXT AS status,
            t.$11::TEXT AS supplier_id,
            t.$12::TEXT AS category_id,
            t.$13::TEXT AS pharmacy_id,
            t.$14::TEXT AS low,
            t.$15::TEXT AS sold_quantity,
            t.$16::TEXT AS active,
            t.$17::TEXT AS in_stock,
            metadata$filename AS _stg_file_name,
            metadata$file_last_modified AS _stg_file_load_ts,
            metadata$file_content_key AS _stg_file_md5
        FROM @common.csv_stage/medicine AS t
    )
    FILE_FORMAT = (FORMAT_NAME = 'common.csv_file_format');

    -- 2. Bronze Layer (Merge from Staging Stream)
    MERGE INTO bronze.medicine AS target
    USING staging.medicine_stm AS source
    ON target.id = source.id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.selling_price != source.selling_price OR
        target.quantity != source.quantity OR
        target.status != source.status
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.expired_date = source.expired_date,
            target.purchase_price = source.purchase_price,
            target.selling_price = source.selling_price,
            target.profit = source.profit,
            target.quantity = source.quantity,
            target.batch_number = source.batch_number,
            target.type = source.type,
            target.status = source.status,
            target.supplier_id = source.supplier_id,
            target.category_id = source.category_id,
            target.pharmacy_id = source.pharmacy_id,
            target.low = source.low,
            target.sold_quantity = source.sold_quantity,
            target.active = source.active,
            target.in_stock = source.in_stock,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            id, name, expired_date, purchase_price, selling_price, profit, quantity, batch_number, 
            type, status, supplier_id, category_id, pharmacy_id, low, sold_quantity, active, in_stock,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _stg_copy_data_ts
        ) VALUES (
            source.id, source.name, source.expired_date, source.purchase_price, source.selling_price, source.profit, source.quantity, source.batch_number, 
            source.type, source.status, source.supplier_id, source.category_id, source.pharmacy_id, source.low, source.sold_quantity, source.active, source.in_stock,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._stg_copy_data_ts
        );

    -- 3. Silver Layer (Merge & Typecasting from Bronze Stream)
    MERGE INTO silver.medicine AS target
    USING (
        SELECT 
            id::NUMBER AS medicine_id,
            name,
            expired_date,
            purchase_price::NUMERIC(10,2) AS purchase_price,
            selling_price::NUMERIC(10,2) AS selling_price,
            profit::NUMERIC(10,2) AS profit,
            quantity::NUMBER AS quantity,
            batch_number,
            type,
            status,
            supplier_id::NUMBER AS supplier_id,
            category_id::NUMBER AS category_id,
            pharmacy_id::NUMBER AS pharmacy_id,
            low::BOOLEAN AS low,
            sold_quantity::NUMBER AS sold_quantity,
            active::BOOLEAN AS active,
            in_stock::BOOLEAN AS in_stock,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        FROM bronze.medicine_stm
    ) AS source
    ON target.medicine_id = source.medicine_id
    WHEN MATCHED AND (
        target.name != source.name OR
        target.selling_price != source.selling_price OR
        target.quantity != source.quantity OR
        target.status != source.status
    ) THEN
        UPDATE SET
            target.name = source.name,
            target.expired_date = source.expired_date,
            target.purchase_price = source.purchase_price,
            target.selling_price = source.selling_price,
            target.profit = source.profit,
            target.quantity = source.quantity,
            target.batch_number = source.batch_number,
            target.type = source.type,
            target.status = source.status,
            target.supplier_id = source.supplier_id,
            target.category_id = source.category_id,
            target.pharmacy_id = source.pharmacy_id,
            target.low = source.low,
            target.sold_quantity = source.sold_quantity,
            target.active = source.active,
            target.in_stock = source.in_stock,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._brz_copy_data_ts = source._brz_copy_data_ts
    WHEN NOT MATCHED THEN
        INSERT (
            medicine_id, name, expired_date, purchase_price, selling_price, profit, quantity, batch_number, 
            type, status, supplier_id, category_id, pharmacy_id, low, sold_quantity, active, in_stock,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts
        ) VALUES (
            source.medicine_id, source.name, source.expired_date, source.purchase_price, source.selling_price, source.profit, source.quantity, source.batch_number, 
            source.type, source.status, source.supplier_id, source.category_id, source.pharmacy_id, source.low, source.sold_quantity, source.active, source.in_stock,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts
        );

    -- 4. Gold Layer (SCD Type 2 Pattern from Silver Stream)
    MERGE INTO gold.medicine_dim AS target
    USING (
        SELECT 
            medicine_id, name, expired_date, purchase_price, selling_price, profit, quantity, batch_number, 
            type, status, supplier_id, category_id, pharmacy_id, low, sold_quantity, active, in_stock,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            METADATA$ACTION, METADATA$ISUPDATE
        FROM silver.medicine_stm
        
        UNION ALL
        
        SELECT 
            medicine_id, name, expired_date, purchase_price, selling_price, profit, quantity, batch_number, 
            type, status, supplier_id, category_id, pharmacy_id, low, sold_quantity, active, in_stock,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            'INSERT' AS METADATA$ACTION, 'FALSE' AS METADATA$ISUPDATE
        FROM silver.medicine_stm
        WHERE METADATA$ACTION = 'INSERT' AND METADATA$ISUPDATE = 'TRUE'
    ) AS source
    ON target.medicine_id = source.medicine_id
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
            medicine_hk,
            medicine_id, name, expired_date, purchase_price, selling_price, profit, quantity, batch_number, 
            type, status, supplier_id, category_id, pharmacy_id, low, sold_quantity, active, in_stock,
            _stg_file_name, _stg_file_load_ts, _stg_file_md5, _brz_copy_data_ts, _slv_copy_data_ts,
            eff_start_ts, eff_end_ts, is_current
        ) VALUES (
            MD5(CONCAT(COALESCE(source.name, ''), COALESCE(source.batch_number, ''), source.purchase_price::TEXT, source.selling_price::TEXT, source.quantity::TEXT, COALESCE(source.status, ''))),
            source.medicine_id, source.name, source.expired_date, source.purchase_price, source.selling_price, source.profit, source.quantity, source.batch_number, 
            source.type, source.status, source.supplier_id, source.category_id, source.pharmacy_id, source.low, source.sold_quantity, source.active, source.in_stock,
            source._stg_file_name, source._stg_file_load_ts, source._stg_file_md5, source._brz_copy_data_ts, source._slv_copy_data_ts,
            CURRENT_TIMESTAMP(), NULL, TRUE
        );

    RETURN 'medicine staging, bronze, silver, and gold pipeline executed successfully in medicine_procedure';
END;

-- Execute Procedure
CALL star_schema.medicine_procedure();