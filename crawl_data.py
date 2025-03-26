import time
import xml.etree.ElementTree as ET
import cloudscraper
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options
from datetime import datetime
import pytz
import pickle
import pandas as pd
from pandas_gbq import to_gbq
from tqdm import tqdm
import logging
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

scraped_data = []

def read_xml_link(sitemap_url):
    scraper = cloudscraper.create_scraper()
    response = scraper.get(sitemap_url)
    xml_content = response.text
    if "<html" in xml_content and "verify you are human" in xml_content.lower():
        logger.error("Blocked by Cloudflare while fetching sitemap!")
        exit()
    root = ET.fromstring(xml_content)
    urls = [url.text for url in root.findall(".//{http://www.sitemaps.org/schemas/sitemap/0.9}loc")]
    return urls

def convert_to_utc(local_time_str):
    if not local_time_str or local_time_str == 'null':
        return None
    try:
        local_time = datetime.strptime(local_time_str, "%Y-%m-%dT%H:%M:%S%z")
        utc_time = local_time.astimezone(pytz.utc)
        return utc_time
    except Exception:
        try:
            local_time = datetime.strptime(local_time_str, "%d/%m/%Y")
            utc_time = pytz.utc.localize(local_time)
            return utc_time
        except Exception as e:
            logger.warning(f"Error converting time '{local_time_str}': {e}")
            return None

def setup_driver():
    chrome_options = Options()
    chrome_options.add_argument('--headless')
    chrome_options.add_argument('--no-sandbox')
    chrome_options.add_argument('--disable-dev-shm-usage')
    chrome_options.add_argument('--disable-gpu')
    chrome_options.add_argument('--window-size=1920x1080')
    chrome_options.add_argument('--disable-extensions')
    chrome_options.add_argument('user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36')
    chrome_options.binary_location = '/opt/google/chrome/chrome'
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver

def scrape_data(url):
    driver = setup_driver()
    try:
        driver.get(url)
        time.sleep(5)
        if "verify you are human" in driver.page_source.lower():
            logger.error(f"Cloudflare block detected for URL: {url}")
            data = {key: 'null' for key in ["ID", "TIME", "TITLE", "ABSTRACT", "CONTENT", "LANGUAGE", "URL", "THUMBNAIL", "TYPE"]}
            data["URL"] = url
            data["CONTENT"] = "Blocked by Cloudflare"
            scraped_data.append(data)
            return
        data = {
            "ID": url.split('/')[-1] if url else 'null',
            "TIME": None,
            "TITLE": 'null',
            "ABSTRACT": 'null',
            "CONTENT": 'null',
            "LANGUAGE": url.split('/')[3] if len(url.split('/')) > 3 else 'null',
            "URL": url,
            "THUMBNAIL": 'null',
            "TYPE": 'null'
        }
        time_selectors = [
            ('meta[property="article:modified_time"]', 'content'),
            ('[class="qb_time_view"] span', 'text'),
            ('time', 'text')
        ]
        for selector, attr in time_selectors:
            try:
                time_tag = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                local_time = time_tag.get_attribute(attr) if attr == 'content' else time_tag.text
                data["TIME"] = convert_to_utc(local_time)
                break
            except Exception as e:
                logger.debug(f"Time extraction failed with {selector}: {e}")
        if data["TIME"] is None:
            logger.warning(f"No valid time found for {url}")
        title_selectors = ['h1', '[class="list_dea_title_box"]', '[class*="title"]']
        for selector in title_selectors:
            try:
                title_tag = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                driver.execute_script("arguments[0].scrollIntoView();", title_tag)
                data["TITLE"] = title_tag.text.strip() or 'null'
                break
            except Exception as e:
                logger.debug(f"Title extraction failed with {selector}: {e}")
        if data["TITLE"] == 'null':
            data["TITLE"] = driver.title.strip() or 'null'
        abstract_selectors = ['[class="excerpt"]', '[class*="intro"] p', 'p:first-child']
        for selector in abstract_selectors:
            try:
                sappo = driver.find_elements(By.CSS_SELECTOR, selector)
                data["ABSTRACT"] = sappo[0].text.strip() if sappo else 'null'
                break
            except Exception as e:
                logger.debug(f"Abstract extraction failed with {selector}: {e}")
        content = ''
        content_selectors = [
            '[class="list_detailds_contents"]',
            '[class="list_detailds_contents edit-custom"]',
            '[class*="content"]',
            'article'
        ]
        for selector in content_selectors:
            try:
                content_elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if content_elements:
                    for x in content_elements:
                        driver.execute_script("arguments[0].scrollIntoView();", x)
                        content += x.text + '\n'
                    break
            except Exception as e:
                logger.debug(f"Content extraction failed with {selector}: {e}")
        if not content:
            paragraphs = driver.find_elements(By.TAG_NAME, "p")
            content = '\n'.join(p.text.strip() for p in paragraphs if p.text.strip())
        data["CONTENT"] = content.strip() if content else 'null'
        thumbnail_selectors = [
            ('meta[property="og:image"]', 'content'),
            ('div.featured-image img', 'src'),
            ('img[class*="thumbnail"]', 'src')
        ]
        for selector, attr in thumbnail_selectors:
            try:
                thumbnail_tag = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                data["THUMBNAIL"] = thumbnail_tag.get_attribute(attr) or 'null'
                break
            except Exception as e:
                logger.debug(f"Thumbnail extraction failed with {selector}: {e}")
        type_selectors = [
            ('meta[property="og:type"]', 'content'),
            ('meta[name="type"]', 'content'),
        ]
        for selector, attr in type_selectors:
            try:
                type_tag = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, selector))
                )
                data["TYPE"] = type_tag.get_attribute(attr) or 'null'
                break
            except Exception as e:
                logger.debug(f"Type extraction failed with {selector}: {e}")
        read_more_selectors = [
            'div[class="read_more"] button',
            '[class*="read_more"] button',
            'button[class*="more"]',
            'a[class*="expand"]'
        ]
        for selector in read_more_selectors:
            try:
                elements = driver.find_elements(By.CSS_SELECTOR, selector)
                if elements:
                    element = elements[0]
                    driver.execute_script("arguments[0].scrollIntoView();", element)
                    ActionChains(driver).move_to_element(element).click().perform()
                    time.sleep(3)
                    content = ''
                    for sel in content_selectors:
                        content_elements = driver.find_elements(By.CSS_SELECTOR, sel)
                        if content_elements:
                            for x in content_elements:
                                content += x.text + '\n'
                            break
                    data["CONTENT"] = content.strip() if content else data["CONTENT"]
                    break
            except Exception as e:
                logger.debug(f"Read more click failed with {selector}: {e}")
        logger.info(f"Extracted data for URL: {url}")
        logger.info(data)
        scraped_data.append(data)
    except Exception as e:
        logger.error(f"Error scraping {url}: {e}")
        data = {key: 'null' for key in ["ID", "TIME", "TITLE", "ABSTRACT", "CONTENT", "LANGUAGE", "URL", "THUMBNAIL", "TYPE"]}
        data["URL"] = url
        data["CONTENT"] = f"Error: {str(e)}"
        data["TIME"] = None
        scraped_data.append(data)
    finally:
        driver.quit()

def push_to_gcp(all_infos, if_exists='replace', save_path=None):
    try:
        df = pd.DataFrame(all_infos)
        df = df.drop_duplicates(subset=['URL'], keep='last')
        if 'TIME' in df.columns:
            df['TIME'] = pd.to_datetime(df['TIME'], errors='coerce', utc=True)
            logger.info(f"NaT values in TIME column: {df['TIME'].isna().sum()}")
            df['TIME'] = df['TIME'].where(df['TIME'].notna(), None)
        if save_path:
            os.makedirs(os.path.dirname(save_path), exist_ok=True)
            df.to_pickle(save_path)
            logger.info(f"Saved DataFrame to {save_path}")
        table_id = 'CMS.CONTENT_VW_PROMOTION'
        project_id = 'vp-dwh-prod-c827'
        to_gbq(df, table_id, project_id=project_id, if_exists=if_exists)
        logger.info(f"Pushed {len(all_infos)} records to GCP")
    except Exception as e:
        logger.error(f"Error pushing to GCP: {e}")
        if "Temporary failure in name resolution" in str(e):
            time.sleep(5)
            push_to_gcp(all_infos, if_exists, save_path)
        else:
            raise

if __name__ == '__main__':
    sitemap_url = "https://vinwonders.com/offers-sitemap.xml"
    urls = read_xml_link(sitemap_url)
    for url in tqdm(urls, desc="Scraping URLs"):
        scrape_data(url)
        time.sleep(3)
    current_date = datetime.now().strftime("%d-%m-%Y")
    pickle_path = f"/home/dungdv34/snap/snapd-desktop-integration/current/Downloads/crawl_data/data/pandas_crawl_content_promotion_{current_date}.pkl"
    push_to_gcp(scraped_data, if_exists='replace')
    logger.info(f"Saved {len(scraped_data)} records to GCP and Pickle file: {pickle_path}")