from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import requests
from sqlalchemy import Table, Column, String, MetaData, create_engine, select, DateTime, text
from sqlalchemy.dialects.mysql import insert
from datetime import datetime
import hashlib
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
    Column("md5_hash", String(64)),
    Column("dv_load_timestamp", DateTime),
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
MYSQL_PASSWORD = "Mysql$123"

final_feedback_table = Table(
    "final_feedback", target_metadata,
    Column("feedback_id", String(64), primary_key=True),
    Column("order_id", String(64)),
    Column("feedback_score", String(64)),
    Column("feedback_form_sent_date", String(64)),
    Column("feedback_answer_date", String(64)),
)

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
    engine = create_engine(url)
    target_metadata.create_all(engine, tables=[sample_target_table], checkfirst=True)

    with engine.connect() as conn:
        existing_ids = set(row[0] for row in conn.execute(select(sample_target_table.c.feedback_id)))
        new_data = []

        for record in data_to_load:
            if record["feedback_id"] in existing_ids:
                continue
            row_string = "|".join(str(record.get(k, "")) for k in [
                "feedback_id", "order_id", "feedback_score",
                "feedback_form_sent_date", "feedback_answer_date"
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

def move_feedback_to_final():
    engine = create_engine(f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}")
    
    metadata=MetaData()
    duplicate_archive_table=Table(
        "feedback_duplicate_archive",
        metadata,
        autoload_with=engine
    )

    target_metadata.create_all(engine, tables=[final_feedback_table, duplicate_archive_table])

    with engine.connect() as conn:
        result = conn.execute(text("SELECT * FROM feedback"))
        rows = result.fetchall()

        for row in rows:
            md5_val = row["md5_hash"]
            check_query = text("""
                SELECT COUNT(*) FROM feedback
                WHERE MD5(CONCAT_WS('|', feedback_id, order_id, feedback_score, feedback_form_sent_date, feedback_answer_date)) = :md5
            """)
            exists = conn.execute(check_query, {"md5": md5_val}).scalar()

            if exists:
                archive_stmt = insert(duplicate_archive_table).values(row._asdict())
                conn.execute(archive_stmt)
            else:
                final_stmt = insert(final_feedback_table).values({
                    "feedback_id": row["feedback_id"],
                    "order_id": row["order_id"],
                    "feedback_score": row["feedback_score"],
                    "feedback_form_sent_date": row["feedback_form_sent_date"],
                    "feedback_answer_date": row["feedback_answer_date"],
                }).prefix_with("IGNORE")
                conn.execute(final_stmt)

            

    engine.dispose()
    print("Finished moving unique records to 'feedback' and duplicates to 'feedback_duplicate_archive'.")

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

     move_feedback_to_final_task = PythonOperator(
        task_id='move_feedback_to_final_task',
        python_callable=move_feedback_to_final
    )

    # Define the task dependencies
    fetch_feedback_from_api_task >> load_feedback_to_db_task >> move_feedback_to_final_task
