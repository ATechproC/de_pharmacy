use role sysadmin;
use warehouse compute_wh;
use database pharmacy_db;

-- stage layer :
    create or replace table staging.address (
        id text,
        street text,
        city text,
        country text,
        pharmacy_id text,
        _stg_file_name text,
        _stg_file_load_ts timestamp_tz,
        _stg_file_md5 text,
        _stg_copy_data_ts timestamp_tz default current_timestamp()
    );

    create or replace stream staging.address_stm
    on table staging.address;

-- bronze layer :

    create or replace table bronze.address (
        id text,
        street text,
        city text,
        country text,
        pharmacy_id text,
        _stg_file_name text,
        _stg_file_load_ts timestamp_tz,
        _stg_file_md5 text,
        _stg_copy_data_ts timestamp_tz,
        _brz_copy_data_ts timestamp_tz default current_timestamp()
    );

    create or replace stream bronze.address_stm
    on table bronze.address;

-- silver layer :
    create or replace table silver.address (
        address_sk number autoincrement primary key,
        address_id number unique not null,
        pharmacy_id number unique not null,
        street string not null,
        city string not null,
        country string not null,
        _stg_file_name string not null,
        _stg_file_load_ts timestamp_tz not null,
        _stg_file_md5 string not null,
        _brz_copy_data_ts timestamp_tz not null,
        _slv_copy_data_ts timestamp_tz default current_timestamp()
    );

    create or replace stream silver.address_stm
    on table silver.address;

-- gold layer :
    create or replace table gold.address_dim (
        address_hk string primary key,
        address_id number unique not null,
        pharmacy_id number unique not null,
        street string not null,
        city string not null,
        country string not null,
        _stg_file_name string not null,
        _stg_file_load_ts timestamp_tz not null,
        _stg_file_md5 string not null,
        _brz_copy_data_ts timestamp_tz not null,
        _slv_copy_data_ts timestamp_tz not null,
        eff_start_ts timestamp_tz not null,
        eff_end_ts timestamp_tz,
        is_current boolean not null
    );

create or replace procedure star_schema.address_procedure()
returns string 
language sql
as
begin

-- staging layer :

    copy into staging.address ( 
        id, street, city, country, pharmacy_id,
        _stg_file_name, _stg_file_load_ts, _stg_file_md5
    )
    from (
        select 
            t.$1::text as id,
            t.$2::text as street,
            t.$3::text as city,
            t.$4::text as country,
            t.$5::text as pharmacy_id,
            metadata$filename as _stg_file_name,
            metadata$file_last_modified as _stg_file_load_ts,
            metadata$file_content_key as _stg_file_md5
        from @common.csv_stage/addresses as t
    )
    file_format = (format_name = 'common.csv_file_format');

    -- bronze layer :

    select * from staging.address_stm;

    merge into bronze.address as target 
    using staging.address_stm as source
    on 
        target.id = source.id
    when matched and (
        target.street != source.street OR
        target.city != source.city OR
        target.country != source.country OR
        target.pharmacy_id != source.pharmacy_id
    ) then 
        update set
            target.street = source.street,
            target.city = source.city,
            target.country = source.country,
            target.pharmacy_id = source.pharmacy_id,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._stg_file_md5 = source._stg_file_md5,
            target._stg_copy_data_ts = source._stg_copy_data_ts
    when not matched then
        insert (
            id,
            street,
            city,
            country,
            pharmacy_id,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _stg_copy_data_ts
        ) values (
            source.id,
            source.street,
            source.city,
            source.country,
            source.pharmacy_id,
            source._stg_file_name,
            source._stg_file_load_ts,
            source._stg_file_md5,
            source._stg_copy_data_ts
        );

    -- silver layer :

    merge into silver.address as target
    using (
        select 
            id as address_id,
            pharmacy_id,
            street,
            city,
            country,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        from bronze.address_stm
    ) as source
    on 
        target.address_id = source.address_id
    when matched and (
        target.street != source.street OR
        target.city != source.city OR
        target.country != source.country
    ) then
        update set
            target.street = source.street,
            target.city = source.city,
            target.country = source.country,
            target._stg_file_name = source._stg_file_name,
            target._stg_file_load_ts = source._stg_file_load_ts,
            target._brz_copy_data_ts = source._brz_copy_data_ts,
            target._stg_file_md5 = source._stg_file_md5
    when not matched then
        insert (
            address_id,
            pharmacy_id,
            street,
            city,
            country,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts
        ) values (
            source.address_id,
            source.pharmacy_id,
            source.street,
            source.city,
            source.country,
            source._stg_file_name,
            source._stg_file_load_ts,
            source._stg_file_md5,
            source._brz_copy_data_ts
        );

-- gold layer :

    merge into gold.address_dim as target
    using silver.address_stm as source
    on  
        target.address_id = source.address_id
        and target.is_current = true
    when not matched and (
        source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'FALSE'
    ) then
        insert (
            address_hk,
            address_id,
            pharmacy_id,
            street,
            city,
            country,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts,
            _slv_copy_data_ts,
            eff_start_ts,
            eff_end_ts,
            is_current
        ) values (
            sha1(concat(source.street, source.city, source.country)),
            source.address_id,
            source.pharmacy_id,
            source.street,
            source.city,
            source.country,
            source._stg_file_name,
            source._stg_file_load_ts,
            source._stg_file_md5,
            source._brz_copy_data_ts,
            source._slv_copy_data_ts,
            current_timestamp(),
            null,
            true
        )
    when matched and (
        source.METADATA$ACTION = 'DELETE' and source.METADATA$ISUPDATE = 'TRUE'
    ) then
        update set
            target.eff_end_ts = current_timestamp(),
            target.is_current = false
    when not matched and (
        source.METADATA$ACTION = 'INSERT' and source.METADATA$ISUPDATE = 'TRUE'
    ) then 
        insert (
            address_hk,
            address_id,
            pharmacy_id,
            street,
            city,
            country,
            _stg_file_name,
            _stg_file_load_ts,
            _stg_file_md5,
            _brz_copy_data_ts,
            _slv_copy_data_ts,
            eff_start_ts,
            eff_end_ts,
            is_current
        ) values (
            sha1(concat(source.street, source.city, source.country)),
            source.address_id,
            source.pharmacy_id,
            source.street,
            source.city,
            source.country,
            source._stg_file_name,
            source._stg_file_load_ts,
            source._stg_file_md5,
            source._brz_copy_data_ts,
            source._slv_copy_data_ts,
            current_timestamp(),
            null,
            true
        );

    return 'address bronze, silver, and gold pipeline executed successfully in address_procedure';
end;

call star_schema.address_procedure();