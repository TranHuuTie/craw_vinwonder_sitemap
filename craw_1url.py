from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
import json
import time
from datetime import datetime

# ⚙️ Cấu hình ChromeDriver
CHROME_DRIVER_PATH = "/home/tiennh/bin/chromedriver"  #  Cập nhật đường dẫn ChromeDriver của bạn!
chrome_options = Options()
chrome_options.add_argument("--headless")  # Chạy ở chế độ ẩn (không hiển thị trình duyệt)
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.264 Safari/537.36")

# 🔥 Mở trình duyệt Selenium
service = Service(CHROME_DRIVER_PATH)
driver = webdriver.Chrome(service=service, options=chrome_options)

# 📰 URL bài viết cần crawl
URL = "https://vinwonders.com/vi/wonderpedia/news/thuy-cung-lon-nhat-viet-nam-o-dau/" # thay url vào đây
driver.get(URL)
time.sleep(3)  # ⏳ Chờ trang tải

# 📌 Hàm lấy nội dung theo selector
def extract_data(selector, attr=None):
    try:
        element = driver.find_element(By.CSS_SELECTOR, selector)
        return element.get_attribute(attr) if attr else element.text.strip()
    except:
        return None
    
parts = URL.split('/')
language = parts[3] if len(parts) > 3 else "en"
        

# 📌 Các selector cần tìm
time_selectors = [
    ('[class="qb_time_view"] span', None),
    ('.qb_time_view span:first-child', None),
    ('meta[property="article:modified_time"]', 'content'), #,  # Lấy ngày từ thẻ <span> đầu tiên
    ('time', None)
]
title_selectors = ['h1', '[class="list_dea_title_box"]', '[class*="title"]']
abstract_selectors = ['[class="excerpt"]', '[class*="intro"] p', 'p:first-child']
content_selectors = ['[class="list_detailds_contents"]', 
                    '[class*="content"]',
                    '[class="list_detailds_contents edit-custom"]', 
                    '[class="listing_dea_table_tents click-show-subnv"]',
                    'article']
thumbnail_selectors = [
    ('meta[property="og:image"]', 'content'),
    ('div.featured-image img', 'src'),
    ('img[class*="thumbnail"]', 'src'),
    ('figure.bread_img img', 'src'),
    ('img', 'src'),
]
type_selectors = [('meta[property="og:type"]', 'content'), ('meta[name="type"]', 'content')]

#  Thu thập dữ liệu
data = {
    "ID": datetime.now().strftime("%Y%m%d%H%M%S"),  # ID là timestamp
    "TIME": next((extract_data(sel, attr) for sel, attr in time_selectors if extract_data(sel, attr)), None),
    "TITLE": next((extract_data(sel) for sel in title_selectors if extract_data(sel)), None),
    "ABSTRACT": next((extract_data(sel) for sel in abstract_selectors if extract_data(sel)), None),
    "CONTENT": next((extract_data(sel) for sel in content_selectors if extract_data(sel)), None),
    "LANGUAGE": language,
    "URL": URL,
    "THUMBNAIL": next((extract_data(sel, attr) for sel, attr in thumbnail_selectors if extract_data(sel, attr)), None),
    "TYPE": next((extract_data(sel, attr) for sel, attr in type_selectors if extract_data(sel, attr)), "article"),
}

#  In kết quả
print(json.dumps(data, indent=4, ensure_ascii=False))

# #  Lưu vào file JSON
# with open("scraped_data.json", "w", encoding="utf-8") as f:
#     json.dump(data, f, indent=4, ensure_ascii=False)

# Đóng trình duyệt
driver.quit()
