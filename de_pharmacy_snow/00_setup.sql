use role sysadmin;
use warehouse compute_wh;

create database if not exists pharmacy_db;

create or replace procedure create_db_and_schema()
returns string
language sql
as
begin
    create schema if not exists staging;
    create schema if not exists bronze;
    create schema if not exists silver;
    create schema if not exists gold;
    create schema if not exists common;
    create schema if not exists star_schema;
    return 'db and schemas created successfully';
end;

create or replace procedure create_stage_prcd()
returns string
language sql
as
begin
    create or replace stage common.csv_stage
        directory = ( enable = true);
    return 'stage created successfully';
end;

create or replace procedure create_file_format_prcd()
returns string
language sql
as
begin
    create or replace file format common.csv_file_format 
        type = 'csv'
        skip_header = 1
        field_delimiter = ','
        record_delimiter = '\n'
        field_optionally_enclosed_by = '\042'
        null_if = ('\\N');
    return 'csv file format created successfully';
end;

create or replace procedure create_tags_masking_policies()
returns string
language sql
as
begin
    create or replace tag 
        common.tag
        allowed_values 'PII', 'EMAIL', 'PHONE', 'PASSWORD';
    
    create or replace masking policy
        common.pii_masking_policy
        as (pii string)
        returns string -> 
        case 
            when current_role() = 'SYSADMIN'
                then pii
            else '** PII **'
        end;
    
    create or replace masking policy
        common.email_masking_policy
        as (email string)
        returns string ->
        case
            when current_role() = 'SYSADMIN'
                then email
            else '** EMAIL **'
        end;
    
    create or replace masking policy
        common.phone_masking_policy
        as (phone string)
        returns string ->
        case 
            when current_role() = 'SYSADMIN'
                then phone
            else '** PHONE **'
        end;
    
    create or replace masking policy 
        common.password_masking_policy
        as (password string)
        returns string ->
        case
            when current_role() = 'SYSADMIN'
                then password
            else '** PASSWORD **'
        end;

    return 'tags and masking policies created successfully';
end;


create or replace procedure setup_procedure()
returns string
language sql
as
begin

    call create_db_and_schema();
    call create_stage_prcd();
    call create_file_format_prcd();
    call create_tags_masking_policies();

    return 'setup done successfully';
end;

call setup_procedure();
