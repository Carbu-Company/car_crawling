"""
Module for setting up the Selenium WebDriver with anti-detection measures.
"""

import os
import time
import logging
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import config

def setup_driver():
    """
    Set up and configure a Chrome WebDriver with anti-detection measures.
    
    Returns:
        WebDriver: Configured Chrome WebDriver instance
    """
    try:
        # 크롬 옵션 설정
        chrome_options = Options()
        
        # 차단 방지를 위한 설정들
        if config.HEADLESS_MODE:
            chrome_options.add_argument("--headless")  # 웹 브라우저를 열지 않도록 설정
        
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        
        # User-Agent 설정 (실제 브라우저처럼 보이게)
        chrome_options.add_argument(f'user-agent={config.USER_AGENT}')
        
        # 브라우저 창 크기 설정 (headless 모드에서는 무시됨)
        chrome_options.add_argument(f"--window-size={config.WINDOW_SIZE}")
        
        # 자동화 감지 플래그 제거
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option('useAutomationExtension', False)
        
        # 드라이버 설정
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        
        # 자동화 감지 회피를 위한 추가 설정
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # 페이지 로드 타임아웃 설정
        driver.set_page_load_timeout(30)
        
        logging.info("WebDriver가 성공적으로 설정되었습니다.")
        return driver
    
    except Exception as e:
        logging.error(f"WebDriver 설정 중 오류 발생: {e}")
        raise

def navigate_to_url(driver, url):
    """
    Navigate to the specified URL.
    
    Args:
        driver: Selenium WebDriver instance
        url: URL to navigate to
        
    Returns:
        bool: True if navigation was successful, False otherwise
    """
    try:
        logging.info(f"URL로 이동 중: {url}")
        driver.get(url)
        time.sleep(2)  # 페이지 로딩 대기
        logging.info("페이지 로딩 완료")
        return True
    
    except Exception as e:
        logging.error(f"URL 이동 중 오류 발생: {e}")
        return False

def handle_popups(driver):
    """
    Handle any popups that might appear on the page.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        bool: True if popups were handled successfully, False otherwise
    """
    try:
        # 팝업 창 처리 (예: 쿠키 동의, 광고 등)
        # 여기에 특정 팝업 처리 로직 추가
        
        # 예시: 쿠키 동의 팝업 닫기
        try:
            cookie_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".cookie-consent-button"))
            )
            cookie_button.click()
            logging.info("쿠키 동의 팝업을 닫았습니다.")
        except TimeoutException:
            # 팝업이 없으면 무시
            pass
        
        # 예시: 다른 팝업 닫기
        try:
            close_buttons = driver.find_elements(By.CSS_SELECTOR, ".popup-close, .modal-close, .close-button")
            for button in close_buttons:
                if button.is_displayed():
                    button.click()
                    time.sleep(0.5)
                    logging.info("팝업을 닫았습니다.")
        except NoSuchElementException:
            # 팝업이 없으면 무시
            pass
        
        return True
    
    except Exception as e:
        logging.error(f"팝업 처리 중 오류 발생: {e}")
        return False

def cleanup_driver(driver):
    """
    Clean up and close the WebDriver.
    
    Args:
        driver: Selenium WebDriver instance
    """
    try:
        if driver:
            driver.quit()
            logging.info("WebDriver가 성공적으로 종료되었습니다.")
    except Exception as e:
        logging.error(f"WebDriver 종료 중 오류 발생: {e}")

def take_screenshot(driver, filename=None):
    """
    Take a screenshot of the current page.
    
    Args:
        driver: Selenium WebDriver instance
        filename: Optional filename for the screenshot
        
    Returns:
        str: Path to the saved screenshot
    """
    try:
        if not filename:
            # 스크린샷 저장 디렉토리 확인 및 생성
            screenshots_dir = "screenshots"
            if not os.path.exists(screenshots_dir):
                os.makedirs(screenshots_dir)
            
            # 타임스탬프를 이용한 파일명 생성
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            filename = f"{screenshots_dir}/screenshot_{timestamp}.png"
        
        # 스크린샷 저장
        driver.save_screenshot(filename)
        logging.info(f"스크린샷이 {filename}에 저장되었습니다.")
        return filename
    
    except Exception as e:
        logging.error(f"스크린샷 저장 중 오류 발생: {e}")
        return None 