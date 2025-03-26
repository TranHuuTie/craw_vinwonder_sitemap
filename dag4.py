from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

#from airflow.providers.google.cloud.hooks.base import GoogleCloudBaseHook

import pandas as pd
from google.cloud import bigquery

# Thông tin BigQuery
PROJECT_ID = "vp-dwh-dev-d963"
DATASET_ID = "TIEN_DATASET"
TABLE_ID = "vinwonder_news"

# Định nghĩa đường dẫn file
PICKLE_FILE = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_.pkl"
def get_bq_client():
    """Lấy BigQuery client từ Airflow Connection"""
    hook = BigQueryHook(gcp_conn_id="tien_gcp")
    credentials = hook.get_credentials()
    project_id = hook._get_field("project")  # Lấy project ID từ connection
    return bigquery.Client(credentials=credentials, project=project_id), project_id

# Định nghĩa DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 3, 25),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "crawl_vinwonder_tienth_0004",
    default_args=default_args,
    schedule_interval="0 7 * * *",
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
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag
)

# Task chạy script trên server từ xa qua SSH
run_script = SSHOperator(
    task_id="run_remote_script",
    ssh_conn_id="tienth",  # Đổi thành SSH Connection ID đã cấu hình trong Airflow
#    command="./script.sh",  # Đường dẫn script trên server
    command='bash -s',
    conn_timeout=6000000,
    cmd_timeout=6000000,
    dag=dag
)

# Task 3: Upload dữ liệu lên BigQuery
upload_BQ = SSHOperator(
    task_id="up_GBQ",
    ssh_conn_id="tienth",  # Đổi thành SSH Connection ID đã cấu hình trong Airflow
    command="""  
    source /home/tiennh/venv/bin/activate && \
    python /home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/update_file_bq.py
    """,
    conn_timeout=600,
    cmd_timeout=600,
    dag=dag
)
# Xác định thứ tự thực hiện
run_script >> crawl_task >> upload_BQ