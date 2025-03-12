"""
Module for setting up the WebDriver for the Encar crawler.
"""

import os
import time
import logging
import platform
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
    Set up and configure the Chrome WebDriver.
    
    Returns:
        WebDriver: Configured Chrome WebDriver instance
    """
    try:
        # 크롬 옵션 설정
        chrome_options = Options()
        
        # 헤드리스 모드 설정 (UI 없음)
        if config.HEADLESS_MODE:
            chrome_options.add_argument("--headless=new")
        
        # 기타 옵션 설정
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-notifications")
        
        # User-Agent 설정
        chrome_options.add_argument(f"user-agent={config.USER_AGENT}")
        
        # 언어 설정
        chrome_options.add_argument("--lang=ko-KR")
        
        # 로깅 레벨 설정
        chrome_options.add_argument("--log-level=3")
        
        # ChromeDriver 설정
        system_platform = platform.system()
        is_arm_mac = system_platform == "Darwin" and platform.machine() == "arm64"
        
        if is_arm_mac:
            logging.info("Apple Silicon Mac detected, using specific ChromeDriver setup")
            # For Apple Silicon Macs, we need to be more specific
            service = Service(ChromeDriverManager().install())
            
            # Verify the driver exists and is executable
            if not os.path.isfile(service.path):
                logging.error(f"ChromeDriver not found at {service.path}")
                raise FileNotFoundError(f"ChromeDriver not found at {service.path}")
            
            # Make sure the file is executable
            if not os.access(service.path, os.X_OK):
                logging.info(f"Making ChromeDriver executable: {service.path}")
                os.chmod(service.path, 0o755)
        else:
            # For other platforms, use the standard approach
            service = Service(ChromeDriverManager().install())
        
        # WebDriver 생성
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 페이지 로드 타임아웃 설정
        driver.set_page_load_timeout(30)
        
        # 자동화 감지 방지
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        logging.info("WebDriver가 성공적으로 설정되었습니다.")
        return driver
    
    except Exception as e:
        logging.error(f"WebDriver 설정 중 오류 발생: {e}")
        # Print more detailed error information
        import traceback
        logging.error(traceback.format_exc())
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