from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, MetaData, create_engine, text, DateTime, select
from sqlalchemy.dialects.mysql import insert
import hashlib

# --- Define a sample target table schema ---
# This section defines the structure of the table where data will be loaded.

target_metadata = MetaData()
sample_target_table = Table(
    "orders",
    target_metadata,
    Column("order_id", String(64), primary_key=True),
    Column("user_name", String(64)),
    Column("order_status", String(64)),
    Column("order_date", String(64)),
    Column("order_approved_date", String(64)),
    Column("pickup_date", String(64)),
    Column("delivered_date", String(64)),
    Column("estimated_time_delivery", String(64)),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime)
)

# --- API Connection Details ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"

# --- MySQL Database Connection Details ---
MYSQL_HOST = "192.168.29.70"
MYSQL_PORT = 3306
MYSQL_DB_NAME = "STAGELOAD"
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "Mysql$123"

final_orders_table=Table(
    "final_orders", target_metadata,
    Column("order_id", String(64), primary_key=True),
    Column("user_name", String(64)),
    Column("order_status", String(64)),
    Column("order_date", String(64)),
    Column("order_approved_date", String(64)),
    Column("pickup_date", String(64)),
    Column("delivered_date", String(64)),
    Column("estimated_time_delivery", String(64)),
    Column("md5_hash", String(64)),
)

def fetch_data_from_api_callable():
    """
    Python callable to fetch data from the API.
    """
    endpoint = f"{API_BASE_URL}/orders/"
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

def load_data_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table.
    """
    fetched_data_json = ti.xcom_pull(task_ids='fetch_data_from_api_task')

    if not fetched_data_json:
        print("No data fetched. Exiting load process.")
        return

    data_to_load = json.loads(fetched_data_json)

    if not data_to_load:
        print("API returned empty data. Nothing to load.")
        return

    db_url = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    engine = create_engine(db_url)
    target_metadata.create_all(engine, tables=[sample_target_table], checkfirst=True)

    with engine.connect() as conn:
       existing_ids = set(row[0] for row in conn.execute(select(sample_target_table.c.order_id)))
       new_data = []
       
       for record in data_to_load:
            if record["order_id"] in existing_ids:
                continue
            row_string = "|".join(str(record.get(k, "")) for k in [
                "order_id", "user_name", "order_status",
                "order_date", "order_approved_date", "pickup_date",
                "delivered_date","estimated_time_delivery"
            ])
            record["md5_hash"] = hashlib.md5(row_string.encode()).hexdigest()
            record["dv_load_timestamp"] = datetime.now()
            new_data.append(record) 
        
       if new_data:
            conn.execute(sample_target_table.insert(), new_data)
            print(f"Inserted {len(new_data)} new records.")
       else:
            print("No new records to insert.")

    engine.dispose()
    print(f"Loaded {len(data_to_load)} records into '{sample_target_table.name}'.")

def move_orders_to_final():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}")
    
    metadata=MetaData()
    duplicate_archive_table=Table(
        "order_duplicate_archive",
        metadata,
        autoload_with=engine
    )

    final_table = metadata.tables['final_orders']
    staging_table = metadata.tables['orders']
    duplicate_archive_table = metadata.tables['order_duplicate_archive']

    with engine.connect() as conn:
        result = conn.execute(select([staging_table]))
        rows = result.fetchall()

        for row in rows:
            row_dict = dict(row)
            order_id = row_dict.get('order_id')

            # Checking if orders already exists in final table
            exists_query = select([final_table]).where(final_table.c.order_id == order_id)
            final_result = conn.execute(exists_query).fetchone()

            if final_result:
                # Checking if already archived to avoid duplicates insert
                archive_check = select([duplicate_archive_table]).where(
                    duplicate_archive_table.c.order_id == order_id
                )
                archived = conn.execute(archive_check).fetchone()

                if not archived:
                    # Inserting into archive table
                    conn.execute(insert(duplicate_archive_table).values(row_dict))
            else:
                # Inserting into final table
                conn.execute(insert(final_table).values(row_dict))


    engine.dispose()
    print("Finished moving unique records to 'orders' and duplicates to 'order_duplicate_archive'.")

# Define the Airflow DAG
with DAG(
    dag_id='orders_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'mysql'],
    doc_md="""
    ### API to Database Data Pipeline: Orders
    This DAG fetches order data from an API using Basic Auth and loads it into a MySQL stage table.
    
     API Endpoint: `/orders/`  
     Target Table: `orders`
    """
) as dag:
    # Task to fetch data from the API
    fetch_data_from_api_task = PythonOperator(
        task_id='fetch_data_from_api_task',
        python_callable=fetch_data_from_api_callable,
    )

    # Task to load the fetched data into the database
    load_data_to_db_task = PythonOperator(
        task_id='load_data_to_db_task',
        python_callable=load_data_to_db,
        provide_context=True,
    )

    move_orders_to_final_task = PythonOperator(
        task_id='move_orders_to_final_task',
        python_callable=move_orders_to_final
    )
    # Define the task dependencies
    fetch_data_from_api_task >> load_data_to_db_task >> move_orders_to_final_task
