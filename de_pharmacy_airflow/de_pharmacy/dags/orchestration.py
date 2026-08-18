from airflow.sdk import dag
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from pendulum import datetime
from airflow.timetables.trigger import CronTriggerTimetable

from create_tables import create_tables_snow as ingest_data

db_name = "pharmacy_db"
schema_name = "star_schema"

@dag(
    dag_id="dag_orchestration_id",
    start_date=datetime(year=2026, month=8, day=8, tz='Africa/Casablanca' ),
    schedule=CronTriggerTimetable("0 0 * * *", timezone="Africa/Casablanca"),
    catchup=True
)
def orchestration():

    address_procedure = SQLExecuteQueryOperator(
        task_id='address_procedure',
        sql=f'call {db_name}.{schema_name}.address_procedure()',
        conn_id='snow_conn'
    )

    orders_procedure = SQLExecuteQueryOperator(
        task_id='orders_procedure',
        sql=f'call {db_name}.{schema_name}.orders_procedure()',
        conn_id='snow_conn'
    )

    pharmacy_procedure = SQLExecuteQueryOperator(
        task_id='pharmacy_procedure',
        sql=f'call {db_name}.{schema_name}.pharmacy_procedure()',
        conn_id='snow_conn'
    )

    order_item_procedure = SQLExecuteQueryOperator(
        task_id='order_item_procedure',
        sql=f'call {db_name}.{schema_name}.order_item_procedure()',
        conn_id='snow_conn'
    )

    user_procedure = SQLExecuteQueryOperator(
        task_id='user_procedure',
        sql=f'call {db_name}.{schema_name}.user_procedure()',
        conn_id='snow_conn'
    )

    supplier_procedure = SQLExecuteQueryOperator(
        task_id='supplier_procedure',
        sql=f'call {db_name}.{schema_name}.supplier_procedure()',
        conn_id='snow_conn'
    )

    category_procedure = SQLExecuteQueryOperator(
        task_id='category_procedure',
        sql=f'call {db_name}.{schema_name}.category_procedure()',
        conn_id='snow_conn'
    )

    medicine_procedure = SQLExecuteQueryOperator(
        task_id='medicine_procedure',
        sql=f'call {db_name}.{schema_name}.medicine_procedure()',
        conn_id='snow_conn'
    )

    cart_procedure = SQLExecuteQueryOperator(
        task_id='cart_procedure',
        sql=f'call {db_name}.{schema_name}.cart_procedure()',
        conn_id='snow_conn'
    )

    cart_item_procedure = SQLExecuteQueryOperator(
        task_id='cart_item_procedure',
        sql=f'call {db_name}.{schema_name}.cart_item_procedure()',
        conn_id='snow_conn'
    )

    credit_procedure = SQLExecuteQueryOperator(
        task_id='credit_procedure',
        sql=f'call {db_name}.{schema_name}.credit_procedure()',
        conn_id='snow_conn'
    )

    tax_procedure = SQLExecuteQueryOperator(
        task_id='tax_procedure',
        sql=f'call {db_name}.{schema_name}.tax_procedure()',
        conn_id='snow_conn'
    )

    date_procedure = SQLExecuteQueryOperator(
        task_id='date_procedure',
        sql=f'call {db_name}.{schema_name}.date_procedure()',
        conn_id='snow_conn'
    )

    order_items_fact_procedure = SQLExecuteQueryOperator(
        task_id='order_items_fact_procedure',
        sql=f'call {db_name}.{schema_name}.order_items_fact_procedure()',
        conn_id='snow_conn'
    )

    ingest_data() >> [
        address_procedure, 
        orders_procedure, 
        pharmacy_procedure, 
        order_item_procedure, 
        user_procedure, 
        supplier_procedure, 
        category_procedure, 
        medicine_procedure, 
        cart_procedure, 
        cart_item_procedure, 
        credit_procedure, 
        tax_procedure, 
        date_procedure
    ] >> order_items_fact_procedure


orchestration = orchestration()