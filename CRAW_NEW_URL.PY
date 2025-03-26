import json
import time
import xml.etree.ElementTree as ET
import pandas as pd
from datetime import datetime
from tqdm import tqdm
import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor, as_completed

# ⚙️ Cấu hình ChromeDriver
CHROME_DRIVER_PATH = "/home/tiennh/bin/chromedriver"
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.264 Safari/537.36")
chrome_options.page_load_strategy = "eager"

# 📂 Định nghĩa đường dẫn file
PKL_FILE = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_news_.pkl"
URLS_FILE = "/home/tiennh/airflow-docker-tienth/Craw_data_vinwonder_web/Vinwonder_urls.txt"

def load_existing_urls(url_file):
    """ Đọc danh sách URL đã crawl từ file txt """
    try:
        with open(url_file, "r", encoding="utf-8") as f:
            return set(line.strip() for line in f.readlines())
    except FileNotFoundError: 
        return set()

def save_new_urls(urls, url_file):
    """ Lưu danh sách URL mới vào file txt """
    with open(url_file, "a", encoding="utf-8") as f:
        for url in urls:
            f.write(url + "\n")

def load_existing_data(pkl_file):
    """ Đọc dữ liệu đã crawl từ file PKL """
    try:
        return pd.read_pickle(pkl_file)
    except (FileNotFoundError, EOFError):
        return pd.DataFrame()

def save_new_data(data, pkl_file):
    """ Lưu dữ liệu mới vào file PKL mà không ghi đè dữ liệu cũ """
    existing_df = load_existing_data(pkl_file)
    new_df = pd.DataFrame(data)
    updated_df = pd.concat([existing_df, new_df], ignore_index=True)
    updated_df.to_pickle(pkl_file)

def extract_data(driver, selector, attr=None):
    """ Trích xuất dữ liệu từ trang web theo selector """
    try:
        element = driver.find_element(By.CSS_SELECTOR, selector)
        return element.get_attribute(attr) if attr else element.text.strip()
    except:
        return None

def scrape_data(url):
    """ Crawl dữ liệu từ URL """
    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    data = None
    try:
        driver.get(url)
        time.sleep(2)  # Có thể thay bằng WebDriverWait để tối ưu
        
        # Xác định ngôn ngữ từ URL
        parts = url.split('/')
        language = parts[3] if len(parts) > 3 else "en"
        
        time_selectors = [
            ('[class="qb_time_view"] span', None),
            ('.qb_time_view span:first-child', None),
            ('meta[property="article:modified_time"]', 'content'), #,  # Lấy ngày từ thẻ <span> đầu tiên
            ('time', None)
        ]

        title_selectors = ['h1', 
                           '[class="list_dea_title_box"]', 
                           '[class*="title"]']
        
        abstract_selectors = ['[class="excerpt"]',
                              '[class*="intro"] p', 
                              'p:first-child']
        
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
        type_selectors = [('meta[property="og:type"]', 'content'), 
                          ('meta[name="type"]', 'content')]

        data = {
            "ID": datetime.now().strftime("%Y%m%d%H%M%S"),
            "TIME": next((extract_data(driver, sel, attr) for sel, attr in time_selectors if extract_data(driver, sel, attr)), None),
            "TITLE": next((extract_data(driver, sel) for sel in title_selectors if extract_data(driver, sel)), None),
            "ABSTRACT": next((extract_data(driver, sel) for sel in abstract_selectors if extract_data(driver, sel)), None),
            "CONTENT": next((extract_data(driver, sel) for sel in content_selectors if extract_data(driver, sel)), None),
            "LANGUAGE": language,
            "URL": url,
            "THUMBNAIL": next((extract_data(driver, sel, attr) for sel, attr in thumbnail_selectors if extract_data(driver, sel, attr)), None),
            "TYPE": next((extract_data(driver, sel, attr) for sel, attr in type_selectors if extract_data(driver, sel, attr)), "article"),
        }
        
        print(f"✅ Đã crawl xong: {url}")
    except Exception as e:
        print(f"❌ Lỗi khi crawl {url}: {e}")
        
        # Ghi URL bị lỗi vào file
        with open("failed_urls.txt", "a", encoding="utf-8") as error_file:
            error_file.write(f"{url}\n")
    finally:
        driver.quit()
    return data

def read_xml_link(sitemap_url):
    """ Đọc danh sách URL từ sitemap XML """
    scraper = cloudscraper.create_scraper()
    response = scraper.get(sitemap_url)
    xml_content = response.text
    if "<html" in xml_content and "verify you are human" in xml_content.lower():
        print("🚫 Bị Cloudflare chặn khi lấy sitemap!")
        return []
    root = ET.fromstring(xml_content)
    urls = [url.text for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
    return urls

if __name__ == "__main__":
    sitemap_url = "https://vinwonders.com/news-sitemap7.xml"
    
    # 🔍 Đọc danh sách URL từ sitemap
    all_urls = set(read_xml_link(sitemap_url))
    print(f"🔎 Tìm thấy {len(all_urls)} URL trong sitemap.")

    # 📝 Đọc danh sách URL đã crawl
    crawled_urls = load_existing_urls(URLS_FILE)
    
    # 🆕 Chỉ lấy những URL chưa crawl
    new_urls = list(all_urls - crawled_urls)
    print(f"📌 Có {len(new_urls)} URL mới cần crawl.")

    # Nếu không có URL mới thì dừng
    if not new_urls:
        print("✅ Không có URL mới, thoát chương trình.")
        exit()

    # 🚀 Bắt đầu crawl dữ liệu mới
    new_data = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        future_to_url = {executor.submit(scrape_data, url): url for url in new_urls}
        for future in tqdm(as_completed(future_to_url), total=len(new_urls), desc="Scraping URLs"):
            url = future_to_url[future]
            try:
                result = future.result()
                if result:
                    new_data.append(result)
            except Exception as exc:
                print(f"❌ Lỗi khi crawl {url}: {exc}")

    # 📂 Lưu dữ liệu mới vào file PKL
    if new_data:
        save_new_data(new_data, PKL_FILE)
        save_new_urls(new_urls, URLS_FILE)
        print(f"✅ Đã lưu {len(new_data)} bài viết mới vào {PKL_FILE}")
    else:
        print("⚠️ Không có dữ liệu mới để lưu.")
