import os
import json
import pandas as pd

def merge_json_to_pkl(json_files, output_pkl):
    merged_data = []
    
    for file in json_files:
        if os.path.exists(file):
            with open(file, 'r', encoding='utf-8') as f:
                try:
                    data = json.load(f)
                    if isinstance(data, list):
                        merged_data.extend(data)
                    else:
                        merged_data.append(data)
                except json.JSONDecodeError as e:
                    print(f"Lỗi khi đọc file {file}: {e}")
        else:
            print(f"File không tồn tại: {file}")
    
    df = pd.DataFrame(merged_data)
    df.to_pickle(output_pkl)
    print(f"Hợp nhất hoàn tất. File lưu tại: {output_pkl}")

if __name__ == "__main__":
    json_files = [
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/362url_.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap1.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap2.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap3.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap4.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap5.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap6.json",
        "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/https:/vinwonders.com/news-sitemap7.json"
    ]
    output_pkl = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_.pkl"
    
    merge_json_to_pkl(json_files, output_pkl)
