import pandas as pd
import pandas_gbq
from google.oauth2 import service_account

# Thông tin BigQuery
project_id = "vp-dwh-dev-d963"
dataset_id = "TIEN_DATASET"
table_name = "vinwonder_news"  # Tên bảng trong BigQuery

# Đường dẫn đến Service Account Key
key_path = "/home/tiennh/airflow-docker-tienth/VP_CMS/bigquery_key.json"

# Đường dẫn file .pkl cần upload
pkl_file_path = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_modified.pkl"

try:
    # Load credentials từ service account key
    cred = service_account.Credentials.from_service_account_file(key_path)

    # Đọc dữ liệu từ file .pkl
    df = pd.read_pickle(pkl_file_path)

    # Tải dữ liệu lên BigQuery (Ghi đè dữ liệu cũ nếu đã tồn tại)
    pandas_gbq.to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, credentials=cred, if_exists="replace")

    print(f"✅ Cập nhật thành công file {pkl_file_path} lên BigQuery (Bảng: {table_name})!")

except Exception as e:
    print(f"❌ Lỗi khi cập nhật dữ liệu lên BigQuery: {e}")

# import pandas as pd
# import pandas_gbq
# from google.oauth2 import service_account

# # Thông tin BigQuery
# project_id = "vp-dwh-dev-d963"
# dataset_id = "TIEN_DATASET"
# table_name = "news_sitemap_362url_"  # Tên bảng trong BigQuery

# # Đường dẫn đến Service Account Key
# key_path = "/home/tiennh/airflow-docker-tienth/VP_CMS/bigquery_key.json"

# # Đường dẫn file JSON mới
# file_path = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/362url_.json"

# try:
#     # Load credentials từ service account key
#     cred = service_account.Credentials.from_service_account_file(key_path)

#     # Đọc dữ liệu từ JSON
#     df = pd.read_json(file_path, orient="records", encoding="utf-8")

#     # Tải dữ liệu lên BigQuery (Ghi đè dữ liệu cũ)
#     pandas_gbq.to_gbq(df, f"{dataset_id}.{table_name}", project_id=project_id, credentials=cred, if_exists="replace")

#     print(f"✅ Cập nhật thành công file {file_path} lên BigQuery (Bảng: {table_name})!")

# except Exception as e:
#     print(f"❌ Lỗi khi cập nhật dữ liệu lên BigQuery: {e}")
