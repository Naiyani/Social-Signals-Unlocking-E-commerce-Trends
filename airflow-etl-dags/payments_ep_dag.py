from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, MetaData,text,DateTime,select, create_engine
import hashlib
from sqlalchemy.dialects.mysql import insert

# --- Define a sample target table schema ---
# This section defines the structure of the table where data will be loaded.
target_metadata = MetaData()
sample_target_table = Table(
    "payments",
    target_metadata,
    Column("order_id", String(64), primary_key=True),
    Column("payment_sequential", String(64), primary_key=True),
    Column("payment_type", String(64)),
    Column("payment_installments", String(64)),
    Column("payment_value", String(64)),
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime)
)


# --- API Connection Details ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"

# --- MySQL Database Connection Details ---
MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = 3306
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "Mysql$123"
MYSQL_DB_NAME = "STAGELOAD"

final_payments_table = Table(
    "final_payments",
    target_metadata,
    Column("order_id", String(64), primary_key=True),
    Column("payment_sequential", String(64), primary_key=True),
    Column("payment_type", String(64)),
    Column("payment_installments", String(64)),
    Column("payment_value", String(64)),
    Column("md5_hash", String(64)),
)

def fetch_payments_from_api_callable():
    """
    Python callable to fetch data from the API.
    """
    endpoint = f"{API_BASE_URL}/payments/" 
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

def load_payments_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table.
    """
    fetched_data_json = ti.xcom_pull(task_ids='fetch_payments_from_api_task')

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
            record_key = f"{record['order_id']}|{record['payment_sequential']}"
            if record_key in existing_ids:
                  continue
            row_string = "|".join(str(record.get(k, "")) for k in [
                       "order_id","payment_sequential","payment_type","payment_installments","payment_value",
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

def move_payments_to_final():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}")
    
    metadata=MetaData()
    duplicate_archive_table=Table(
        "payment_duplicate_archive",
        metadata,
        autoload_with=engine
    )

    target_metadata.create_all(engine, tables=[final_payments_table, duplicate_archive_table])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM payments"))
        rows = result.fetchall()

        for row in rows:
            md5_val = row["md5_hash"]
            check_query = text("""
                SELECT COUNT(*) FROM final_payments
                WHERE MD5(CONCAT_WS('|', order_id, payment_sequential, payment_type,
                payment_installments, payment_value)) = :md5
            """)
            exists = conn.execute(check_query, {"md5": md5_val}).scalar()

            if exists:
                archive_stmt = insert(duplicate_archive_table).values(row._asdict())
                conn.execute(archive_stmt)
            else:
                final_stmt = insert(final_payments_table).values({
                    "order_id": row["order_id"],
                    "payment_sequential": row["payment_sequential"],
                    "payment_type": row["payment_type"],
                    "payment_installments": row["payment_installments"],
                    "payment_value": row["payment_value"],
                }).prefix_with("IGNORE")
                conn.execute(final_stmt)

    engine.dispose()
    print("Finished moving unique records to 'payments' and duplicates to 'payment_duplicate_archive'.")

# Define the Airflow DAG
with DAG(
    dag_id='payments_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'mysql'],
    doc_md="""
    ### API to Database Data Pipeline: payments
    This DAG fetches payment data from an API using Basic Auth and loads it into a MySQL stage table.
    
     API Endpoint: `/payments/`  
     Target Table: `payments`
    """
) as dag:
    # Task to fetch data from the API
    fetch_payments_from_api_task = PythonOperator(
        task_id='fetch_payments_from_api_task',
        python_callable=fetch_payments_from_api_callable,
    )
    
    move_payments_to_final_task = PythonOperator(
        task_id='move_payments_to_final_task',
        python_callable=move_payments_to_final
    )

    # Task to load the fetched data into the database
    load_payments_to_db_task = PythonOperator(
        task_id='load_payments_to_db_task',
        python_callable=load_payments_to_db,
        provide_context=True,
    )

    # Define the task dependencies
    fetch_payments_from_api_task >> load_payments_to_db_task >> move_payments_to_final_task
