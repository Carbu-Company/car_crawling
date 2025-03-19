"""
Configuration settings for the Encar crawler.
"""

import os
from datetime import datetime
import random

# URL Configuration
BASE_URL = "http://www.encar.com/dc/dc_carsearchlist.do?carType=kor#!%7B%22action%22%3A%22(And.Hidden.N._.CarType.Y.)%22%2C%22toggle%22%3A%7B%7D%2C%22layer%22%3A%22%22%2C%22sort%22%3A%22ModifiedDate%22%2C%22page%22%3A{}%2C%22limit%22%3A20%2C%22searchKey%22%3A%22%22%2C%22loginCheck%22%3Afalse%7D"

# Browser Configuration
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
WINDOW_SIZE = "1920,1080"
HEADLESS_MODE = False  # Set to True to run in headless mode

# Crawler Configuration
MAX_PAGES = 1500  # Maximum number of pages to crawl
MAX_RETRIES = 3  # Maximum number of retries for the entire crawling process
MAX_DETAIL_RETRIES = 2  # Maximum number of retries for detail page extraction
RETRY_DELAY = 5  # Retry delay between attempts

# Wait Times
def get_page_load_wait():
    """Random wait time for page loading"""
    return random.uniform(5, 7)

def get_detail_page_load_wait():
    """Random wait time for detail page loading"""
    return random.uniform(5, 7)

def get_car_processing_wait():
    """Random wait time between processing cars"""
    return random.uniform(3, 5)

def get_pagination_wait():
    """Random wait time after pagination"""
    return random.uniform(6, 10)

def get_retry_wait():
    """Random wait time before retrying the entire process"""
    return random.uniform(10, 15)

def get_browser_close_wait():
    """Random wait time before closing the browser"""
    return random.uniform(2, 5)

def get_scroll_wait():
    """Wait time after scrolling"""
    return 1

# CSS Selectors
SELECTORS = {
    "car_list": "#sr_normal",
    "car_items": "#sr_normal > tr",
    "pagination": "#pagination",
    "next_button": "span.next a",
    "detail_button": "#wrap > div > div.Layout_contents__MD95o > div.ResponsiveLayout_wrap__XLqcM.ResponsiveLayout_wide__VYk4x > div.ResponsiveLayout_content_area__yyYYv > div:nth-child(1) > div > button",
    "detail_popup": ".BottomSheet-module_bottom_sheet__LeljN",
    "detail_items": ".DetailSpec_list_default__Gx\\+ZA li",
    "detail_key": ".DetailSpec_tit__BRQb\\+",
    "detail_value": ".DetailSpec_txt__NGapF",
    "car": {
        "index": "data-index",
        "impression": "data-impression",
        "img": "td.img img.thumb",
        "badges": "td.img .service_badge_list em",
        "manufacturer": "td.inf .cls strong",
        "model": "td.inf .cls em",
        "detail_model": "td.inf .dtl strong",
        "year": "td.inf .detail .yer",
        "mileage": "td.inf .detail .km",
        "fuel": "td.inf .detail .fue",
        "location": "td.inf .detail .loc",
        "performance_record": "td.inf .detail .ins",
        "diagnosis": "td.inf .detail .ass",
        "price_hs": "td.prc_hs",
        "price": "td.prc",
        "price_value": "strong",
        "detail_url": "td.inf a",
        "ad_info": "td.img .box_advertise .desc_advertise"
    }
}

# Key Mapping for Detail Information
DETAIL_KEY_MAPPING = {
    "차량번호": "차량번호",
    "연식": "상세연식",
    "주행거리": "상세주행거리",
    "배기량": "배기량",
    "연료": "상세연료",
    "변속기": "변속기",
    "차종": "차종",
    "색상": "색상",
    "지역": "상세지역",
    "인승": "인승",
    "수입구분": "수입구분",
    "압류 · 저당": "압류저당",
    "조회수": "조회수",
    "찜": "찜수"
}

# File Paths
def get_log_filename():
    """
    Generate a log filename based on the current date and time.
    
    Returns:
        str: Log filename
    """
    # 로그 디렉토리 생성
    if not os.path.exists(LOG_DIR):
        os.makedirs(LOG_DIR)
    
    # 현재 날짜와 시간을 이용한 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(LOG_DIR, f"encar_crawler_{timestamp}.log")

def get_page_filename(page_number):
    """
    Generate a filename for saving page data.
    
    Args:
        page_number: Page number
        
    Returns:
        str: Filename for the page data
    """
    # 데이터 디렉토리 생성
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # 현재 날짜를 이용한 파일명 생성
    date_str = datetime.now().strftime("%Y%m%d")
    return os.path.join(DATA_DIR, f"encar_data_page{page_number}_{date_str}.csv")

def get_all_data_filename():
    """
    Generate a filename for saving all collected data.
    
    Returns:
        str: Filename for all data
    """
    # 데이터 디렉토리 생성
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)
    
    # 현재 날짜와 시간을 이용한 파일명 생성
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return os.path.join(DATA_DIR, f"encar_all_data_{timestamp}.csv")

def get_error_screenshot_filename():
    """Generate filename for error screenshots"""
    import time
    return f"error_screenshot_{time.strftime('%Y%m%d_%H%M%S')}.png"

# OpenSearch Configuration
OPENSEARCH_HOST = "14.6.96.11"
OPENSEARCH_PORT = 1006
OPENSEARCH_USERNAME = "admin"
OPENSEARCH_PASSWORD = "Myopensearch!1"
OPENSEARCH_INDEX_NAME = "encar_vehicles"
OPENSEARCH_USE_SSL = False
OPENSEARCH_VERIFY_CERTS = False

# Data Storage Configuration
DATA_DIR = "data"  # Directory to save data
SCREENSHOTS_DIR = "screenshots"  # Directory to save screenshots

# Logging Configuration
LOG_DIR = "logs"
LOG_LEVEL = "INFO"
LOG_FORMAT = "%(asctime)s - %(levelname)s - %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S" 