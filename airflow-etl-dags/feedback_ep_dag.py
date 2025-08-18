from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
from sqlalchemy import Table, Column, String, MetaData, create_engine, select, DateTime, text
from sqlalchemy.dialects.mysql import insert
import hashlib
import paramiko
import pandas as pd
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

'''
# --- API Connection Details ---
API_BASE_URL = "http://34.16.77.121:1515"
API_USERNAME = "student1"
API_PASSWORD = "pass123"
'''
SFTP_HOST = "34.16.77.121"
SFTP_PORT = 2222
SFTP_USERNAME = "BuildProject"
SFTP_PASSWORD = "student"

# --- MySQL Database Connection Details  ---
MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = 3306
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "Mysql$123"
MYSQL_DB_NAME = "STAGELOAD"

final_feedback_table = Table(
    "final_feedback", target_metadata,
    Column("feedback_id", String(64), primary_key=True),
    Column("order_id", String(64)),
    Column("feedback_score", String(64)),
    Column("feedback_form_sent_date", String(64)),
    Column("feedback_answer_date", String(64)),
)
'''
def fetch_feedback_from_api_callable():
    """
    Python callable to fetch data from the API.
    """
    endpoint = f"{API_BASE_URL}/feedback/"
    print(f"Fetching data from: {endpoint}")
    response = requests.get(endpoint, auth=(API_USERNAME, API_PASSWORD))
    response.raise_for_status()
    return response.text
'''

def fetch_feedback_from_sftp_callable(ti):
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.open(SFTP_FILE_PATH, 'r') as file_handle:
            df = pd.read_csv(file_handle, encoding='utf-8-sig')
    finally:
        sftp.close()
        transport.close()

    ti.xcom_push(key="feedback_data", value=df.to_json(orient='records'))

def load_feedback_to_db(ti):
    """
    Fetches data from XCom, connects to the target database,
    and loads the data into a table.
    """
    fetched_data_json = ti.xcom_pull(task_ids='fetch_feedback_from_sftp_task')

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
        existing_ids = set(row[0] for row in conn.execute(select(sample_target_table.c.feedback_id)).fetchall())
        new_data = []

        for record in data_to_load:
            if record["feedback_id"] in existing_ids:
                continue
            v_load_ts = datetime.now()  
            record["dv_load_timestamp"] = dv_load_ts
            row_string = "|".join(str(record.get(k, "")) for k in [
                "feedback_id", "order_id", "feedback_score",
                "feedback_form_sent_date", "feedback_answer_date", "dv_load_timestamp"
            ])
            record["md5_hash"] = hashlib.md5(row_string.encode()).hexdigest()
            new_data.append(record)

        if new_data:
            stmt = insert(sample_target_table)
            stmt = stmt.on_duplicate_key_update(
                feedback_id=stmt.inserted.feedback_id  
            )
            conn.execute(stmt, new_data)
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
         target_metadata,
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
                WHERE md5_hash = :md5
            """)
            exists = conn.execute(check_query, {"md5": md5_val}).scalar()

            if exists>1:
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
    # Task to fetch data from the SFTP
    fetch_feedback_from_sftp_task = PythonOperator(
        task_id='fetch_feedback_from_sftp_task',
        python_callable=fetch_feedback_from_sftp_callable,
    )

    # Task to load the fetched data into the database
    load_feedback_to_db_task = PythonOperator(
        task_id='load_feedback_to_db_task',
        python_callable=load_feedback_to_db,
        provide_context=True,
    )

    #Task to move the data
     move_feedback_to_final_task = PythonOperator(
        task_id='move_feedback_to_final_task',
        python_callable=move_feedback_to_final
    )

    # Define the task dependencies
    fetch_feedback_from_sftp_task >> load_feedback_to_db_task >> move_feedback_to_final_task


