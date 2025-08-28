from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

# Default DAG arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 8, 27),
    'depends_on_past': False,
    'retries': 1
}

# Define the DAG
with DAG(
    dag_id='load_dwh_tables',
    default_args=default_args,
    description='Load DWH dimension and fact tables',
    schedule_interval=None,  # Run on demand
    catchup=False
) as dag:

    # Start task
    start = DummyOperator(task_id='start')

    # Dimension table loads
    load_dim_users = MySqlOperator(
        task_id='load_dim_users',
        mysql_conn_id='mysql_default',
        sql='sql/load_dim_users.sql'
    )

    load_dim_sellers = MySqlOperator(
        task_id='load_dim_sellers',
        mysql_conn_id='mysql_default',
        sql='sql/load_dim_sellers.sql'
    )

    load_dim_products = MySqlOperator(
        task_id='load_dim_products',
        mysql_conn_id='mysql_default',
        sql='sql/load_dim_products.sql'
    )

    load_dim_orders = MySqlOperator(
        task_id='load_dim_orders',
        mysql_conn_id='mysql_default',
        sql='sql/load_dim_orders.sql'
    )

    load_dim_feedback = MySqlOperator(
        task_id='load_dim_feedback',
        mysql_conn_id='mysql_default',
        sql='sql/load_dim_feedback.sql'
    )

    load_dim_payments = MySqlOperator(
        task_id='load_dim_payments',
        mysql_conn_id='mysql_default',
        sql='sql/load_dim_payments.sql'
    )

    # Fact table load
    load_fact_sales = MySqlOperator(
        task_id='load_fact_sales',
        mysql_conn_id='mysql_default',
        sql='sql/load_fact_sales.sql'
    )

    # End task
    end = DummyOperator(task_id='end')

    # Define DAG dependencies
    start >> [
        load_dim_users,
        load_dim_sellers,
        load_dim_products,
        load_dim_orders,
        load_dim_feedback,
        load_dim_payments
    ] >> load_fact_sales >> end
