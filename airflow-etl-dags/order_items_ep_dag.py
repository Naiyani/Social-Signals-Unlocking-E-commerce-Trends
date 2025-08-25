from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, MetaData, create_engine, text, DateTime,select
from sqlalchemy.dialects.mysql import insert
import hashlib
# --- Define a sample target table schema ---
target_metadata = MetaData()
sample_target_table = Table(
    "order_items",
    target_metadata,
    Column("order_id", String(64), primary_key=True),
    Column("order_item_id", String(64), primary_key=True),
    Column("product_id", String(64)),
    Column("seller_id", String(64)),
    Column("pickup_limit_date", String(64)),
    Column("price", String(64)),
    Column("shipping_cost", String(64)),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime)
)


# --- API Connection Details  ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"

# --- MySQL Database Connection Details ---
MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = 3306
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "Mysql$123"
MYSQL_DB_NAME = "STAGELOAD"

final_order_items_table = Table(
    "final_order_items",
    target_metadata,
    Column("order_id", String(64), primary_key=True),
    Column("order_item_id", String(64), primary_key=True),
    Column("product_id", String(64)),
    Column("seller_id", String(64)),
    Column("pickup_limit_date", String(64)),
    Column("price", String(64)),
    Column("shipping_cost", String(64)),
    Column("md5_hash", String(64)),
)

def fetch_order_items_from_api_callable():
    """
    Python callable to fetch data from the API.
    """
    endpoint = f"{API_BASE_URL}/order_items/"
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

def load_order_items_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table.
    """
    fetched_data_json = ti.xcom_pull(task_ids='fetch_order_items_from_api_task')

    if not fetched_data_json:
        print("No data fetched. Exiting load process.")
        return

    data_to_load = [
    row for row in data_to_load
    if row.get("order_id") 
    and row["order_id"] != "order_id"
    and not row["order_id"].startswith("order_")  # optionally skip fake headers
]

    if not data_to_load:
        print("API returned empty data. Nothing to load.")
        return

    db_url = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    engine = create_engine("mysql+pymysql://root:Mysql$123@host.docker.internal:3306/STAGELOAD")
    target_metadata.create_all(engine, tables=[sample_target_table], checkfirst=True)

    with engine.connect() as conn:
        existing_ids = set(f"{row['order_id']}|{row['order_item_id']}" for row in conn.execute(select(sample_target_table.c.order_id, sample_target_table.c.order_item_id)))
        new_data = []

        for record in data_to_load: 
            record_key = f"{record['order_id']}|{record['order_item_id']}"
            if record_key in existing_ids:
                continue
            row_string = "|".join(str(record.get(k, "")) for k in [
                   "order_id","order_item_id","product_id","seller_id","pickup_limit_date",
                   "price","shipping_cost"
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

def move_order_items_to_final():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}")
    
    metadata=MetaData()
    duplicate_archive_table=Table(
        "order_items_duplicate_archive",
        metadata,
        autoload_with=engine
    )

    target_metadata.create_all(engine, tables=[final_order_items_table, duplicate_archive_table])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM order_items"))
        rows = result.fetchall()

        for row in rows:
            md5_val = row["md5_hash"]
            check_query = text("""
                SELECT COUNT(*) FROM final_order_items
                WHERE MD5(CONCAT_WS('|', order_id, order_item_id,
                product_id, seller_id, pickup_limit_date,price,
                shipping_cost)) = :md5
            """)
            exists = conn.execute(check_query, {"md5": md5_val}).scalar()

            if exists:
                archive_stmt = insert(duplicate_archive_table).values(row._asdict())
                conn.execute(archive_stmt)
            else:
                final_stmt = insert(final_order_items_table).values({
                    "order_id": row["order_id"],
                    "order_item_id": row["order_item_id"],
                    "product_id": row["product_id"],
                    "seller_id": row["seller_id"],
                    "pickup_limit_date": row["pickup_limit_date"],
                    "price": row["price"],
                    "shipping_cost": row["shipping_cost"],
                }).prefix_with("IGNORE")
                conn.execute(final_stmt)

    engine.dispose()
    print("Finished moving unique records to 'order_items' and duplicates to 'order_duplicate_archive'.")


# Define the Airflow DAG
with DAG(
    dag_id='order_items_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'mysql'],
    doc_md="""
    ### API to Database Data Pipeline: order_items
    This DAG fetches order data from an API using Basic Auth and loads it into a MySQL stage table.
    
     API Endpoint: `/order_items/`  
     Target Table: `order_items`
    """
) as dag:
    # Task to fetch data from the API
    fetch_order_items_from_api_task = PythonOperator(
        task_id='fetch_order_items_from_api_task',
        python_callable=fetch_order_items_from_api_callable,
    )

    # Task to load the fetched data into the database
    load_order_items_to_db_task = PythonOperator(
        task_id='load_order_items_to_db_task',
        python_callable=load_order_items_to_db,
        provide_context=True,
    )
    move_order_items_to_final_task = PythonOperator(
        task_id='move_order_items_to_final_task',
        python_callable=move_order_items_to_final
    )

    # Define the task dependencies
    fetch_order_items_from_api_task >> load_order_items_to_db_task >> move_order_items_to_final_task
