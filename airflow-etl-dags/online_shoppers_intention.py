# online_shoppers_intention_sftp_to_dim_dag.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import hashlib
import os

import pandas as pd
import paramiko
from sqlalchemy import (
    Table, Column, Integer, Float, String, DateTime, BigInteger,
    MetaData, create_engine, text
)
from sqlalchemy.dialects.mysql import insert as mysql_insert

#  SFTP 
SFTP_HOST = "34.16.77.121"
SFTP_PORT = 2222
SFTP_USERNAME = "BuildProject"
SFTP_PASSWORD = "student"
SFTP_FILE_PATH = "/upload/online_shoppers_intention.csv"

#  MySQL 
MYSQL_HOST = "host.docker.internal"
MYSQL_PORT = 3306
MYSQL_USERNAME = "root"
MYSQL_PASSWORD = "Mysql$123"
MYSQL_DB_NAME = "STAGELOAD"
DB_URL = f"mysql+pymysql://{MYSQL_USERNAME}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB_NAME}"

# Tables
metadata = MetaData()

# STAGING: md5_hash is PRIMARY KEY to dedupe at ingest time
stg_online = Table(
    "stg_online_shoppers_intention",
    metadata,
    Column("Administrative", Integer, nullable=True),
    Column("Administrative_Duration", Float, nullable=True),
    Column("Informational", Integer, nullable=True),
    Column("Informational_Duration", Float, nullable=True),
    Column("ProductRelated", Integer, nullable=True),
    Column("ProductRelated_Duration", Float, nullable=True),
    Column("BounceRates", Float, nullable=True),
    Column("ExitRates", Float, nullable=True),
    Column("PageValues", Float, nullable=True),
    Column("SpecialDay", Float, nullable=True),
    Column("Month", String(16), nullable=True),
    Column("OperatingSystems", Integer, nullable=True),
    Column("Browser", Integer, nullable=True),
    Column("Region", Integer, nullable=True),
    Column("TrafficType", Integer, nullable=True),
    Column("VisitorType", String(32), nullable=True),
    Column("Weekend", Integer, nullable=True),  # 0/1
    Column("Revenue", Integer, nullable=True),  # 0/1
    Column("md5_hash", String(64), primary_key=True),
    Column("dv_load_timestamp", DateTime, nullable=True),
)

# DIM: surrogate key + UNIQUE on md5_hash to keep only the first arrival
dim_online = Table(
    "DIM_online_shoppers_intention",
    metadata,
    Column("dim_id", BigInteger, primary_key=True, autoincrement=True),
    Column("Administrative", Integer, nullable=True),
    Column("Administrative_Duration", Float, nullable=True),
    Column("Informational", Integer, nullable=True),
    Column("Informational_Duration", Float, nullable=True),
    Column("ProductRelated", Integer, nullable=True),
    Column("ProductRelated_Duration", Float, nullable=True),
    Column("BounceRates", Float, nullable=True),
    Column("ExitRates", Float, nullable=True),
    Column("PageValues", Float, nullable=True),
    Column("SpecialDay", Float, nullable=True),
    Column("Month", String(16), nullable=True),
    Column("OperatingSystems", Integer, nullable=True),
    Column("Browser", Integer, nullable=True),
    Column("Region", Integer, nullable=True),
    Column("TrafficType", Integer, nullable=True),
    Column("VisitorType", String(32), nullable=True),
    Column("Weekend", Integer, nullable=True),
    Column("Revenue", Integer, nullable=True),
    Column("md5_hash", String(64), nullable=False, unique=True, index=True),
    Column("dv_load_timestamp", DateTime, nullable=True),
)

#  Helpers
def compute_md5_row(d: dict) -> str:
    # stable md5 over all business columns + dv_load_timestamp included after set
    ordered_keys = [
        "Administrative", "Administrative_Duration", "Informational", "Informational_Duration",
        "ProductRelated", "ProductRelated_Duration", "BounceRates", "ExitRates", "PageValues",
        "SpecialDay", "Month", "OperatingSystems", "Browser", "Region", "TrafficType",
        "VisitorType", "Weekend", "Revenue", "dv_load_timestamp"
    ]
    parts = [f"{k}={'' if d.get(k) is None else str(d.get(k)).strip()}" for k in ordered_keys]
    return hashlib.md5("|".join(parts).encode()).hexdigest()

def fetch_df_from_sftp() -> pd.DataFrame:
    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.connect(username=SFTP_USERNAME, password=SFTP_PASSWORD)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.open(SFTP_FILE_PATH, "r") as fh:
            df = pd.read_csv(fh)
    finally:
        sftp.close()
        transport.close()
    return df

# Tasks
def create_tables():
    engine = create_engine(DB_URL)
    metadata.create_all(engine, tables=[stg_online, dim_online], checkfirst=True)
    engine.dispose()
    print("Ensured STG and DIM tables exist.")

def load_staging_from_sftp(ti=None):
    df = fetch_df_from_sftp()

    # normalize booleans to ints if needed
    for col in ["Weekend", "Revenue"]:
        if col in df.columns:
            df[col] = df[col].astype(int)

    now = datetime.now()
    df["dv_load_timestamp"] = now

    # ensure all needed columns exist (in case CSV lacks some)
    required_cols = [c.name for c in stg_online.columns if c.name not in ("md5_hash")]
    for c in required_cols:
        if c not in df.columns:
            df[c] = None

    # compute md5
    df["md5_hash"] = df.apply(lambda r: compute_md5_row(r.to_dict()), axis=1)

    # write to STG with INSERT IGNORE semantics via ON DUPLICATE KEY UPDATE no-op
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        rows = df[ [c.name for c in stg_online.columns] ].to_dict(orient="records")
        if rows:
            stmt = mysql_insert(stg_online).values(rows)
            # on duplicate key (md5 PK) => no-op update to avoid errors
            stmt = stmt.on_duplicate_key_update(md5_hash=stmt.inserted.md5_hash)
            conn.execute(stmt)
            print(f"STG upsert complete: attempted {len(rows)} rows (duplicates ignored).")
        else:
            print("No rows to stage.")
    engine.dispose()

def upsert_dim_from_stg():
    engine = create_engine(DB_URL)
    with engine.begin() as conn:
        # Insert only NEW md5s into DIM
        cols = [c.name for c in dim_online.columns if c.name not in ("dim_id")]
        col_list = ", ".join(cols)

        # Build SELECT of matching column order from STG
        stg_select_cols = ", ".join(
            [f"s.{c}" for c in cols]  # column names are identical between STG and DIM (except dim_id)
        )

        sql = f"""
        INSERT INTO {dim_online.name} ({col_list})
        SELECT {stg_select_cols}
        FROM {stg_online.name} s
        LEFT JOIN {dim_online.name} d
          ON d.md5_hash = s.md5_hash
        WHERE d.md5_hash IS NULL
        """
        conn.execute(text(sql))
    engine.dispose()
    print("DIM upsert complete (new md5_hash rows inserted).")

# DAG
with DAG(
    dag_id="online_shoppers_intention_sftp_to_dim",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    tags=["etl", "sftp", "dim", "mysql"],
    doc_md="""
    ### SFTP → STG → DIM (Online Shoppers Intention)
    - Reads `/upload/online_shoppers_intention.csv` from SFTP in-memory
    - Stages to `stg_online_shoppers_intention` with md5_hash + dv_load_timestamp
    - Inserts only new md5s into `DIM_online_shoppers_intention`
    """,
) as dag:
    t0 = PythonOperator(
        task_id="create_tables",
        python_callable=create_tables,
    )
    t1 = PythonOperator(
        task_id="load_staging_from_sftp",
        python_callable=load_staging_from_sftp,
        provide_context=True,
    )
    t2 = PythonOperator(
        task_id="upsert_dim",
        python_callable=upsert_dim_from_stg,
    )

    t0 >> t1 >> t2
