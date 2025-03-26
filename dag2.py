from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
import pandas as pd
from google.cloud import bigquery
from airflow.providers.google.common.hooks.base_google import GoogleCloudBaseHook
# Thông tin BigQuery
PROJECT_ID = "vp-dwh-dev-d963"
DATASET_ID = "TIEN_DATASET"
TABLE_ID = "vinwonder_news"
#KEY_PATH = "/home/tiennh/airflow-docker-tienth/VP_CMS/bigquery_key.json"

# Định nghĩa đường dẫn file
PICKLE_FILE = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_.pkl"

def get_bq_client():
    """Lấy BigQuery client từ Airflow Connection"""
    hook = GoogleCloudBaseHook(gcp_conn_id="tien_gcp")
    credentials = hook.get_credentials()
    return bigquery.Client(credentials=credentials, project=hook.project_id)

def upload_to_bigquery():
    """Đọc dữ liệu từ pickle và đẩy lên BigQuery"""
    client = get_bq_client()
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Đọc dữ liệu từ pickle
    df = pd.read_pickle(PICKLE_FILE)

    # Đẩy dữ liệu lên BigQuery
    job = client.load_table_from_dataframe(df, table_ref, job_config=bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND"
    ))
    job.result()  # Chờ job hoàn thành
    print(f"Upload thành công lên BigQuery: {table_ref}")

# Định nghĩa DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "crawl_up_vinwonder_GBQ_tienth_03",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

# Task thiết lập Virtual Environment và chạy script crawl trên server từ xa
crawl_task = SSHOperator(
    task_id="crawl_new_urls",
    ssh_conn_id="tienth",  # Đổi thành SSH Connection ID đã cấu hình trong Airflow
    command="""  
    source /home/tiennh/venv/bin/activate && \
    python /home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/CRAW_NEW_URL.PY
    """,
    conn_timeout=6000000,
    cmd_timeout=600000,
    dag=dag
)

# Task chạy script khác trên server từ xa qua SSH
# Task chạy script trên server từ xa qua SSH
run_script = SSHOperator(
    task_id="run_remote_script",
    ssh_conn_id="tienth",  # Đổi thành SSH Connection ID đã cấu hình trong Airflow
    command="./script.sh ",  # Đường dẫn script trên server
    conn_timeout=6000000,
    cmd_timeout=600000,
    dag=dag
)

# Task upload dữ liệu lên BigQuery
# Task upload dữ liệu lên BigQuery
upload_task = PythonOperator(
    task_id="upload_to_bigquery",
    python_callable=upload_to_bigquery,
    dag=dag,
)

# Xác định thứ tự thực hiện
run_script >> crawl_task >> upload_task