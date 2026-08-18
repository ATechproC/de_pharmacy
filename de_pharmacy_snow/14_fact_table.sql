use role sysadmin;
use warehouse compute_wh;
use database pharmacy_db;
create schema if not exists star_schema;

    create table if not exists gold.order_items_fact (
        order_item_sk number autoincrement primary key,
        order_id number,
        order_item_id number,
        date_dim_key number,
        address_dim_key string,
        pharmacy_dim_key string,
        user_dim_key string,
        supplier_dim_key string,
        category_dim_key string,
        medicine_dim_key string,
        cart_dim_key string,
        cart_item_dim_key string,
        credit_dim_key string,
        tax_dim_key string,
        quantity number,
        total numeric(10,2),
        profit numeric(10,2),
        _copy_data_ts timestamp_tz default current_timestamp(),
        constraint order_constraint_fk foreign key (order_id) references silver.orders(order_id),
        constraint order_item_constraint_fk foreign key (order_item_id) references silver.order_item(order_item_id),
        constraint date_constraint_fk foreign key (date_dim_key) references gold.date_dim(date_sk),
        constraint pharmacy_constraint_fk foreign key (pharmacy_dim_key) references gold.pharmacy_dim(pharmacy_hk),
        constraint user_constraint_fk foreign key (user_dim_key) references gold.user_dim(user_hk),
        constraint address_constraint_fk foreign key (address_dim_key) references gold.address_dim(address_hk),
        constraint supplier_constraint_fk foreign key (supplier_dim_key) references gold.supplier_dim(supplier_hk),
        constraint category_constraint_fk foreign key (category_dim_key) references gold.category_dim(category_hk),
        constraint medicine_constraint_fk foreign key (medicine_dim_key) references gold.medicine_dim(medicine_hk),
        constraint cart_constraint_fk foreign key (cart_dim_key) references gold.cart_dim(cart_hk),
        constraint cart_item_constraint_fk foreign key (cart_item_dim_key) references gold.cart_item_dim(cart_item_hk),
        constraint credit_constraint_fk foreign key (credit_dim_key) references gold.credit_dim(credit_hk),
        constraint tax_constraint_fk foreign key (tax_dim_key) references gold.tax_dim(tax_hk)
    );


create or replace procedure star_schema.order_items_fact_procedure()
returns string
language sql
as
begin

    merge into gold.order_items_fact as target
    using (
        select
            o.order_id,
            oi.order_item_id,
            d.date_sk as date_dim_key,
            p.pharmacy_hk as pharmacy_dim_key,
            a.address_hk as address_dim_key,
            u.user_hk as user_dim_key,
            s.supplier_hk as supplier_dim_key,
            c.category_hk as category_dim_key,
            m.medicine_hk as medicine_dim_key,
            ca.cart_hk as cart_dim_key,
            ci.cart_item_hk as cart_item_dim_key,
            cr.credit_hk as credit_dim_key,
            t.tax_hk as tax_dim_key,
            oi.quantity,
            o.total,
            o.profit
        from silver.order_item_stm as oi
        
        left join silver.orders as o
            on oi.order_id = o.order_id
        
        left join gold.date_dim as d
            on date(o.created_at) = d.order_date
        
        left join gold.pharmacy_dim as p
            on o.pharmacy_id = p.pharmacy_id
           and p.is_current = true

        left join gold.address_dim as a
            on p.address_id = a.address_id
           and a.is_current = true

        left join gold.user_dim as u
            on u.pharmacy_id = p.pharmacy_id
           and u.is_current = true

        left join gold.supplier_dim as s
            on s.pharmacy_id = p.pharmacy_id
           and s.is_current = true

        left join gold.category_dim as c
            on c.pharmacy_id = p.pharmacy_id
           and c.is_current = true

        left join gold.medicine_dim as m
            on oi.medicine_id = m.medicine_id
           and m.is_current = true

        left join gold.cart_dim as ca
            on ca.pharmacy_id = p.pharmacy_id
           and ca.is_current = true

        left join gold.cart_item_dim as ci
            on ci.cart_id = ca.cart_id
           and ci.is_current = true

        left join gold.credit_dim as cr
            on cr.pharmacy_id = p.pharmacy_id
           and cr.is_current = true

        left join gold.tax_dim as t
            on t.pharmacy_id = p.pharmacy_id
           and t.is_current = true
        
    ) as source
    on 
        source.order_id = target.order_id
        and source.order_item_id = target.order_item_id
    when not matched then
        insert (
            order_id,
            order_item_id,
            date_dim_key,
            address_dim_key,
            pharmacy_dim_key,
            user_dim_key,
            supplier_dim_key,
            category_dim_key,
            medicine_dim_key,
            cart_dim_key,
            cart_item_dim_key,
            credit_dim_key,
            tax_dim_key,
            quantity,
            total,
            profit
        ) values (
            source.order_id,
            source.order_item_id,
            source.date_dim_key,
            source.address_dim_key,
            source.pharmacy_dim_key,
            source.user_dim_key,
            source.supplier_dim_key,
            source.category_dim_key,
            source.medicine_dim_key,
            source.cart_dim_key,
            source.cart_item_dim_key,
            source.credit_dim_key,
            source.tax_dim_key,
            source.quantity,
            source.total,
            source.profit
        );

    return 'order items fact gold layer pipeline executed successfully in order_items_fact_procedure';
end;

call star_schema.order_items_fact_procedure();