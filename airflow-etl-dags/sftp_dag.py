from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowException

from datetime import datetime, timedelta, timezone
import os, socket, csv, io, paramiko
from hashlib import md5
from typing import List, Dict, Tuple, Set 

from sqlalchemy import create_engine, text

#  Connectivity helpers 

def _resolve_host_or_fallback(host: str) -> str:
    if not host:
        return host
    try:
        socket.gethostbyname(host)
        return host
    except Exception:
        return os.environ.get("DOCKER_HOST_GATEWAY", "172.17.0.1")

def _get_mysql_conn():
    try:
        return BaseHook.get_connection("mysql_stage")
    except Exception:
        return BaseHook.get_connection("mysql_default")

def get_engine():
    c = _get_mysql_conn()
    host = _resolve_host_or_fallback(c.host)
    url = f"mysql+pymysql://{c.login}:{c.password}@{host}:{c.port or 3306}/{c.schema or 'stageload'}"
    return create_engine(
        url,
        pool_pre_ping=True,
        pool_recycle=300,
        connect_args={"connect_timeout": 5},
        future=True,
    )

def assert_mysql_up():
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text("SELECT 1"))

def fetch_from_sftp(remote_path: str) -> io.StringIO:
    s = BaseHook.get_connection("sftp_build")
    transport = paramiko.Transport((s.host, s.port or 22))
    transport.connect(username=s.login, password=s.password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    try:
        with sftp.open(remote_path, "rb") as f:
            return io.StringIO(f.read().decode("utf-8"))
    finally:
        sftp.close()
        transport.close()

#  Parsing / normalization 

_DT_FORMATS = (
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%d",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%Y-%m-%dT%H:%M:%S",
)

def parse_dt(v):
    if v is None:
        return None
    v = str(v).strip()
    if not v:
        return None
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(v, fmt)
        except ValueError:
            continue
    return None 
def to_int(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def to_float(v):
    if v is None:
        return None
    s = str(v).strip()
    if s == "" or s.lower() == "nan":
        return None
    try:
        return float(s)
    except Exception:
        return None

def stable_md5(row: Dict, fields: List[str]) -> str:
    parts = []
    for k in fields:
        v = row.get(k)
        parts.append("" if v is None else str(v).strip().lower())
    return md5("|".join(parts).encode("utf-8")).hexdigest()

#  Dataset configuration 

DATASETS = {
    "orders": dict(
        sftp="/upload/order_dataset.csv",
        stage="orders_stage",
        archive="orders_duplicate_archive",
        pk_cols=["order_id"],
        csv_cols=[
            "order_id","user_name","order_status",
            "order_date","order_approved_date","pickup_date",
            "delivered_date","estimated_time_delivery",
        ],
        md5_fields=[
            "order_id","user_name","order_status",
            "order_date","order_approved_date","pickup_date",
            "delivered_date","estimated_time_delivery",
        ],
        dt_cols=["order_date","order_approved_date","pickup_date","delivered_date","estimated_time_delivery"],
        int_cols=[],
        float_cols=[],
        preload_pks=False,
        chunk_size=20000,
    ),
    "order_items": dict(
        sftp="/upload/order_item_dataset.csv",
        stage="order_items_stage",
        archive="order_items_duplicate_archive",
        pk_cols=["order_id","order_item_id"],
        csv_cols=[
            "order_id","order_item_id","product_id","seller_id",
            "pickup_limit_date","price","shipping_cost",
        ],
        md5_fields=[
            "order_id","order_item_id","product_id","seller_id",
            "pickup_limit_date","price","shipping_cost",
        ],
        dt_cols=["pickup_limit_date"],
        int_cols=["order_item_id"],
        float_cols=["price","shipping_cost"],
    ),
    "payments": dict(
        sftp="/upload/payment_dataset.csv",
        stage="payments_stage",
        archive="payments_duplicate_archive",
        pk_cols=["order_id","payment_sequential"],
        csv_cols=[
            "order_id","payment_sequential","payment_type",
            "payment_installments","payment_value",
        ],
        md5_fields=[
            "order_id","payment_sequential","payment_type",
            "payment_installments","payment_value",
        ],
        dt_cols=[],
        int_cols=["payment_sequential","payment_installments"],
        float_cols=["payment_value"],
    ),
    "products": dict(
        sftp="/upload/products_dataset.csv",
        stage="products_stage",
        archive="products_duplicate_archive",
        pk_cols=["product_id"],
        csv_cols=[
            "product_id","product_category","product_name_length",
            "product_description_length","product_photos_qty",
            "product_weight_g","product_length_cm",
            "product_height_cm","product_width_cm",
        ],
        md5_fields=[
            "product_id","product_category","product_name_length",
            "product_description_length","product_photos_qty",
            "product_weight_g","product_length_cm",
            "product_height_cm","product_width_cm",
        ],
        dt_cols=[],
        int_cols=[
            "product_name_length","product_description_length","product_photos_qty",
            "product_weight_g","product_length_cm","product_height_cm","product_width_cm",
        ],
        float_cols=[],
    ),
    "sellers": dict(
        sftp="/upload/seller_dataset.csv",
        stage="sellers_stage",
        archive="sellers_duplicate_archive",
        pk_cols=["seller_id"],
        csv_cols=["seller_id","seller_zip_code","seller_city","seller_state"],
        md5_fields=["seller_id","seller_zip_code","seller_city","seller_state"],
        dt_cols=[],
        int_cols=[],
        float_cols=[],
    ),
    "users": dict(
        sftp="/upload/user_dataset.csv",
        stage="users_stage",
        archive="users_duplicate_archive",
        pk_cols=["user_name"],
        csv_cols=["user_name","customer_zip_code","customer_city","customer_state"],
        md5_fields=["user_name","customer_zip_code","customer_city","customer_state"],
        dt_cols=[],
        int_cols=[],
        float_cols=[],
    ),
    "feedback": dict(
        sftp="/upload/feedback_dataset.csv",
        stage="feedback",  
        archive="feedback_duplicate_archive",
        pk_cols=["feedback_id"],
        csv_cols=[
            "feedback_id","order_id","feedback_score",
            "feedback_form_sent_date","feedback_answer_date",
        ],
        md5_fields=[
            "feedback_id","order_id","feedback_score",
            "feedback_form_sent_date","feedback_answer_date",
        ],
        dt_cols=["feedback_form_sent_date","feedback_answer_date"],
        int_cols=["feedback_score"],
        float_cols=[],
    ),
}

#  DB helpers 

def _pk_tuple(row: Dict, pk_cols: List[str]) -> Tuple:
    return tuple(row.get(c) for c in pk_cols)

def _select_existing_pks(conn, table: str, pk_cols: List[str]) -> Set[Tuple]:
    cols = ", ".join(pk_cols)
    rs = conn.execute(text(f"SELECT {cols} FROM {table}"))
    if len(pk_cols) == 1:
        return set((r[0],) for r in rs)
    return set(tuple(r) for r in rs)

def _ensure_archive_table(conn, stage: str, archive: str):
    conn.execute(text(f"CREATE TABLE IF NOT EXISTS {archive} LIKE {stage}"))
    try:
        conn.execute(text(f"ALTER TABLE {archive} ADD UNIQUE KEY uk_{archive}_md5 (md5_hash)"))
    except Exception:
        pass

def _insert_archive_ignore(conn, table: str, rows: List[Dict], csv_cols: List[str]):
    if not rows:
        return
    cols = csv_cols + ["md5_hash","dv_load_timestamp"]
    col_list = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    sql = f"INSERT IGNORE INTO {table} ({col_list}) VALUES ({placeholders})"
    conn.execute(text(sql), rows)

def _upsert_stage(conn, table: str, rows: List[Dict], csv_cols: List[str]):
    if not rows:
        return
    cols = csv_cols + ["md5_hash","dv_load_timestamp"]
    col_list = ", ".join(cols)
    placeholders = ", ".join(f":{c}" for c in cols)
    updates = "md5_hash=VALUES(md5_hash), dv_load_timestamp=VALUES(dv_load_timestamp)"
    sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) ON DUPLICATE KEY UPDATE {updates}"
    conn.execute(text(sql), rows)

#  Core loader 

def _load_dataset(dataset: str, cfg: dict, chunk_size: int = 5000):
    if "chunk_size" in cfg and isinstance(cfg["chunk_size"], int) and cfg["chunk_size"] > 0:
        chunk_size = cfg["chunk_size"]

    fh = fetch_from_sftp(cfg["sftp"])
    reader = csv.DictReader(fh)

    dv_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    eng = get_engine()
    with eng.begin() as conn:
        _ensure_archive_table(conn, cfg["stage"], cfg["archive"])

        preload = cfg.get("preload_pks", True)
        if preload:
            existing_pks = _select_existing_pks(conn, cfg["stage"], cfg["pk_cols"])
        else:
            existing_pks = set() 
        seen_in_batch = set()
        batch, new_rows, dup_rows = [], [], []

        def pk_tuple(row: dict) -> tuple:
            return tuple(row.get(c) for c in cfg["pk_cols"])

        def normalize_row(r_raw: dict) -> dict:
            md5_val = stable_md5(r_raw, cfg["md5_fields"])
            r = dict(r_raw)
            for col in cfg["dt_cols"]:
                r[col] = parse_dt(r.get(col))
            for col in cfg["int_cols"]:
                r[col] = to_int(r.get(col))
            for col in cfg["float_cols"]:
                r[col] = to_float(r.get(col))
            r["md5_hash"] = md5_val
            r["dv_load_timestamp"] = dv_ts
            return r

        def flush():
            if new_rows:
                _upsert_stage(conn, cfg["stage"], new_rows, cfg["csv_cols"])
                new_rows.clear()
            if dup_rows:
                _insert_archive_ignore(conn, cfg["archive"], dup_rows, cfg["csv_cols"])
                dup_rows.clear()

        def detect_dups_and_enqueue(rows: list):
            nonlocal existing_pks, seen_in_batch
            if not preload:
                keys = [pk_tuple(r) for r in rows]
                if len(cfg["pk_cols"]) == 1:
                    in_vals = tuple(k[0] for k in keys)
                    if in_vals:
                        rs = conn.execute(
                            text(f"SELECT {cfg['pk_cols'][0]} FROM {cfg['stage']} WHERE {cfg['pk_cols'][0]} IN :vals"),
                            {"vals": in_vals}
                        )
                        existing = set((r[0],) for r in rs)
                    else:
                        existing = set()
                else:
                    existing = set()
                existing_pks = existing

            for r in rows:
                key = pk_tuple(r)
                only_csv = {c: r.get(c) for c in cfg["csv_cols"]}
                if (key in existing_pks) or (key in seen_in_batch):
                    dup_rows.append({**only_csv, "md5_hash": r["md5_hash"], "dv_load_timestamp": dv_ts})
                else:
                    new_rows.append({**only_csv, "md5_hash": r["md5_hash"], "dv_load_timestamp": dv_ts})
                    seen_in_batch.add(key)

        processed = 0
        for raw in reader:
            raw = {k.strip(): (None if v is None else v.strip()) for k, v in raw.items()}
            batch.append(normalize_row(raw))

            if len(batch) >= chunk_size:
                detect_dups_and_enqueue(batch)
                flush()
                processed += len(batch)
                seen_in_batch.clear()
                batch = []
                if processed and processed % 50000 == 0:
                    print(f"[{dataset}] processed ~{processed} rows…")

        if batch:
            detect_dups_and_enqueue(batch)
            flush()

    print(f"[{dataset}] load complete (chunk_size={chunk_size})")

#  DAG 
default_args = {"retries": 3, "retry_delay": timedelta(minutes=1)}

with DAG(
    dag_id="sftp_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    tags=["sftp", "stageload"],
    doc_md="""
    SFTP → stageload.* stage tables (and `feedback` table).
    • Stable MD5 (no timestamp in hash)
    • Empty strings → NULL for numeric/datetime
    • Duplicates go to <entity>_duplicate_archive with INSERT IGNORE
    • Chunked inserts to avoid memory/XCom blowups
    """,
) as dag:

    precheck = PythonOperator(
        task_id="mysql_precheck",
        python_callable=assert_mysql_up,
        retries=3,
        retry_delay=timedelta(minutes=1),
        execution_timeout=timedelta(minutes=2),
    )

    for name, cfg in DATASETS.items():
        t = PythonOperator(
            task_id=f"{name}_load_stage",
            python_callable=_load_dataset,
            op_kwargs={"dataset": name, "cfg": cfg, "chunk_size": 5000},
            retries=3,
            retry_delay=timedelta(minutes=1),
            execution_timeout=timedelta(minutes=20),
        )
        precheck >> t
