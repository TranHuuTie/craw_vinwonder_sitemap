import json
import time
import xml.etree.ElementTree as ET
import re
from datetime import datetime
from tqdm import tqdm
import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from concurrent.futures import ThreadPoolExecutor, as_completed

# ⚙️ Cấu hình ChromeDriver
CHROME_DRIVER_PATH = "/home/tiennh/bin/chromedriver"  # Cập nhật đường dẫn ChromeDriver của bạn!
chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--no-sandbox")
chrome_options.add_argument("--disable-dev-shm-usage")
chrome_options.add_argument("--window-size=1920x1080")
chrome_options.add_argument("--disable-blink-features=AutomationControlled")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.6778.264 Safari/537.36")
# Tùy chọn page load strategy có thể cải thiện tốc độ:
chrome_options.page_load_strategy = "eager"

def extract_data(driver, selector, attr=None):
    try:
        element = driver.find_element(By.CSS_SELECTOR, selector)
        return element.get_attribute(attr) if attr else element.text.strip()
    except:
        return None

def scrape_data(url):
    service = Service(CHROME_DRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    data = None
    try:
        driver.get(url)
        time.sleep(2)  # Có thể giảm thời gian chờ nếu dùng WebDriverWait
        
        # Lấy ngôn ngữ từ URL (phần thứ 3 sau khi tách dấu /)
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
        
        with open("https://vinwonders.com/news-sitemap5.json", "a", encoding="utf-8") as f:
            json.dump(data, f, indent=4, ensure_ascii=False)
            f.write(",\n")
        
        print(f"✅ Đã crawl xong: {url}")
    except Exception as e:
        print(f"❌ Lỗi khi crawl {url}: {e}")
        
        # Ghi URL bị lỗi vào file
        with open("failed_urls_5.txt", "a", encoding="utf-8") as error_file:
            error_file.write(f"{url}\n")
    finally:
        driver.quit()
    return data

def read_xml_link(sitemap_url):
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
    sitemap_url = "https://vinwonders.com/news-sitemap5.xml" #"https://vinwonders.com/offers-sitemap.xml"
    urls = read_xml_link(sitemap_url)
    print(f"🔎 Tìm thấy {len(urls)} URL trong sitemap.")
    
    # Sử dụng ThreadPoolExecutor để crawl song song (ví dụ 5 - 10 luồng cùng lúc)
    with ThreadPoolExecutor(max_workers=6) as executor:
        future_to_url = {executor.submit(scrape_data, url): url for url in urls}
        for future in tqdm(as_completed(future_to_url), total=len(urls), desc="Scraping URLs"):
            url = future_to_url[future]
            try:
                future.result()
            except Exception as exc:
                print(f"❌ Lỗi khi crawl {url}: {exc}")
