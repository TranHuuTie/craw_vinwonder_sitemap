import re
import pandas as pd

import re
import pandas as pd
import unicodedata

import pandas as pd

# Đọc file pickle
file_path = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_modified.pkl"
df = pd.read_pickle(file_path)

df_filtered_zh = df[df['LANGUAGE'] == 'zh']

# Danh sách từ khóa và giá trị tương ứng
facility_keywords = {
    r"野生动物园": "vinpearl-safari-phu-quoc",r"畅玩富国岛大世界":"grand-world-phu-quoc", 
    r"Vinpearl Safari 富国":"vinpearl-safari-phu-quoc",r"Vinpearl Safari":"vinpearl-safari",
    r"Aquafield": "aquafield-nha-trang",r"Aquafield Nha Trang":"aquafield-nha-trang",r"VinWonders Nha Trang":"vinwonder-nha-trang",
    r"珍珠奇幻乐园芽庄":"vinpearl-wonderland-nha-trang",
    r"VinWonders": "vinwonders",
    r"Vinpearl Harbour": "vinpearl-harbour-nha-trang",r"到达文贝尔港":"vinpearl-harbour-nha-trang",r"VINPEARL 芽庄":"vinpearl-nha-trang",r"vinpearl 芽庄":"vinpeal-nha-trang",
    r"VinWonders Cua Hoi":"vinwonder-cua-hoi", r"南会安VinWonders":"vinwonder-cua-hoi",r"VinWonders Nam Hoi An":"vinWonder-cua-hoi",
}

location_keywords = {
    r"富国岛": "phu-quoc", r"富国":"phu-quoc", 
    r"\bHon Tre\b": "nha-trang",r"2天1夜探索Hon Tre —— 海洋天":"nha-trang",r"芽庄":"nha-trang",r"Vinpearl Harbour":"nha-trang", r"到达文贝尔港":"nha-trang" ,
    r"VinWonders Cua Hoi":"nam-hoi-an",r"南会安":"nam-hoi-an", 
                                                                        # @ Thêm \b để khớp chính xác cụm "Hon Tre"
}

# Hàm tìm Facility từ TITLE & ABSTRACT
def extract_facility(title, abstract):
    found_facilities = []
    text = f"{title} {abstract}"  # Gộp TITLE và ABSTRACT để kiểm tra cùng lúc
    
    for pattern, facility in facility_keywords.items():
        if re.search(pattern, text):
            found_facilities.append(facility)
    
    return list(set(found_facilities)) if found_facilities else None  # Loại bỏ trùng lặp

# Hàm tìm Location từ TITLE & ABSTRACT
def extract_location(title, abstract):
    found_locations = []
    text = f"{title} {abstract}"
    
    for pattern, location in location_keywords.items():
        if re.search(pattern, text):
            found_locations.append(location)
    
    return list(set(found_locations)) if found_locations else None  # Loại bỏ trùng lặp

# Áp dụng regex cho từng bài viết
df_filtered_zh["Facility"] = df_filtered_zh.apply(lambda row: extract_facility(row["TITLE"], row["ABSTRACT"]), axis=1)
df_filtered_zh["Location"] = df_filtered_zh.apply(lambda row: extract_location(row["TITLE"], row["ABSTRACT"]), axis=1)

# Gộp lại dữ liệu đã cập nhật vào dataframe gốc
df.update(df_filtered_zh)
# Lưu lại file pickle với dữ liệu đã được cập nhật
df.to_pickle(file_path)

print("Cập nhật dữ liệu thành công vào file pickle!")

