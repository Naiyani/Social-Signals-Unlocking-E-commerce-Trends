from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, MetaData, create_engine
from sqlalchemy.dialects.mysql import insert

# --- Define a sample target table schema ---
# This section defines the structure of the table where data will be loaded.
target_metadata = MetaData()
sample_target_table = Table(
    "feedback",
    target_metadata,
    Column("feedback_id", String(64), primary_key=True),
    Column("order_id", String(64)),
    Column("feedback_score", String(64)),
    Column("feedback_form_sent_date", String(64)),
    Column("feedback_answer_date", String(64)),
)


# --- API Connection Details ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"

# --- MySQL Database Connection Details  ---
MYSQL_HOST = "192.168.29.70"
MYSQL_PORT = 3306
MYSQL_DB_NAME = "STAGELOAD"
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "root"

def fetch_feedback_from_api_callable():
    """
    Python callable to fetch data from the API.
    """
    endpoint = f"{API_BASE_URL}/feedback/"
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text

def load_feedback_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table.
    """
    fetched_data_json = ti.xcom_pull(task_ids='fetch_feedback_from_api_task')

    if not fetched_data_json:
        print("No data fetched. Exiting load process.")
        return

    data_to_load = json.loads(fetched_data_json)

    if not data_to_load:
        print("API returned empty data. Nothing to load.")
        return

    db_url = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"
    engine = create_engine("mysql+pymysql://root:Mysql$123@host.docker.internal:3306/STAGELOAD")
    target_metadata.create_all(engine, tables=[sample_target_table], checkfirst=True)

    with engine.connect() as conn:
       for record in data_to_load:
          stmt = insert(sample_target_table).values(**record)
          stmt = stmt.prefix_with("IGNORE")
          conn.execute(stmt)

    engine.dispose()
    print(f"Loaded {len(data_to_load)} records into '{sample_target_table.name}'.")

# Define the Airflow DAG
with DAG(
    dag_id='feedback_etl',
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=['data_pipeline', 'api_integration', 'mysql'],
    doc_md="""
    ### API to Database Data Pipeline: Feedback
    This DAG fetches order data from an API using Basic Auth and loads it into a MySQL stage table.
    
     API Endpoint: `/feedback/`  
     Target Table: `feedback`
    """
) as dag:
    # Task to fetch data from the API
    fetch_feedback_from_api_task = PythonOperator(
        task_id='fetch_feedback_from_api_task',
        python_callable=fetch_feedback_from_api_callable,
    )

    # Task to load the fetched data into the database
    load_feedback_to_db_task = PythonOperator(
        task_id='load_feedback_to_db_task',
        python_callable=load_feedback_to_db,
        provide_context=True,
    )

    # Define the task dependencies
    fetch_feedback_from_api_task >> load_feedback_to_db_task
