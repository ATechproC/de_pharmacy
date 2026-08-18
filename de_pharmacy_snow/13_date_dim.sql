use role sysadmin;
use warehouse compute_wh;
use database pharmacy_db;
create schema if not exists star_schema;

create or replace procedure star_schema.date_procedure()
returns string
language sql
as
begin
    create or replace table gold.date_dim (
        date_sk number autoincrement primary key,
        order_date date not null,
        year number not null,
        month number not null,
        quarter number not null,
        week number not null,
        day_of_year number not null,
        day_of_month number not null,
        day_of_week number not null,
        day_name string not null
    );

    insert into gold.date_dim (
        order_date, 
        year,
        month,
        quarter,
        week,
        day_of_year,
        day_of_month,
        day_of_week,
        day_name
    )
    with recursive date_cte as (
        -- anchor query :
        select 
            current_date() as today,
            year(current_date()) as year,
            month(current_date()) as month,
            quarter(current_date()) as quarter,
            week(current_date()) as week,
            dayofyear(current_date()) as day_of_year,
            dayofmonth(current_date()) as day_of_month,
            dayofweek(current_date()) as day_of_week,
            dayname(current_date()) as day_name

        union all

        -- recursive query :
        select 
            dateadd('day', -1, today) as today,
            year(dateadd('day', -1, today)) as year,
            month(dateadd('day', -1, today)) as month,
            quarter(dateadd('day', -1, today)) as quarter,
            week(dateadd('day', -1, today)) as week,
            dayofyear(dateadd('day', -1, today)) as day_of_year,
            dayofmonth(dateadd('day', -1, today)) as day_of_month,
            dayofweek(dateadd('day', -1, today)) as day_of_week,
            dayname(dateadd('day', -1, today)) as day_name
        from 
            date_cte
        where 
            today > (select min(date(created_at)) from silver.orders)       
    )
    select
        today,
        year,
        month,
        quarter,
        week,
        day_of_year,
        day_of_month,
        day_of_week,
        day_name
    from date_cte;

    return 'date gold dimension pipeline executed successfully in date_procedure';
end;

call star_schema.date_procedure();