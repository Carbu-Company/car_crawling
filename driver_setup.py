"""
Module for setting up the WebDriver for the Encar crawler.
"""

import os
import time
import logging
import platform
import subprocess
import shutil
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
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
        
        # Detect system platform
        system_platform = platform.system()
        is_arm_mac = system_platform == "Darwin" and platform.machine() == "arm64"
        
        # Clear WebDriver Manager cache
        wdm_cache_dir = os.path.expanduser("~/.wdm/drivers/chromedriver")
        if os.path.exists(wdm_cache_dir):
            logging.info(f"Clearing WebDriver Manager cache: {wdm_cache_dir}")
            shutil.rmtree(wdm_cache_dir, ignore_errors=True)
        
        # For Apple Silicon Macs, download ChromeDriver directly
        if is_arm_mac:
            logging.info("Apple Silicon Mac detected, using direct ChromeDriver download")
            
            # Create a directory for ChromeDriver if it doesn't exist
            driver_dir = os.path.join(os.getcwd(), "chromedriver")
            os.makedirs(driver_dir, exist_ok=True)
            
            # Path to the ChromeDriver executable
            driver_path = os.path.join(driver_dir, "chromedriver")
            
            # Check if we need to download ChromeDriver
            if not os.path.exists(driver_path) or not os.access(driver_path, os.X_OK):
                # Get Chrome version
                try:
                    chrome_version_cmd = [
                        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
                        "--version"
                    ]
                    chrome_version_output = subprocess.check_output(chrome_version_cmd).decode('utf-8')
                    chrome_version = chrome_version_output.strip().split(' ')[2].split('.')[0]
                    logging.info(f"Detected Chrome version: {chrome_version}")
                except:
                    # Default to a recent version if detection fails
                    chrome_version = "114"
                    logging.warning(f"Could not detect Chrome version, using default: {chrome_version}")
                
                # Download URL for ChromeDriver
                download_url = f"https://chromedriver.storage.googleapis.com/LATEST_RELEASE_{chrome_version}"
                
                try:
                    # Get the latest ChromeDriver version for this Chrome version
                    import urllib.request
                    chromedriver_version = urllib.request.urlopen(download_url).read().decode('utf-8')
                    logging.info(f"Latest ChromeDriver version for Chrome {chrome_version}: {chromedriver_version}")
                    
                    # Download ChromeDriver
                    chromedriver_url = f"https://chromedriver.storage.googleapis.com/{chromedriver_version}/chromedriver_mac64_m1.zip"
                    zip_path = os.path.join(driver_dir, "chromedriver.zip")
                    
                    logging.info(f"Downloading ChromeDriver from: {chromedriver_url}")
                    urllib.request.urlretrieve(chromedriver_url, zip_path)
                    
                    # Extract the zip file
                    import zipfile
                    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                        zip_ref.extractall(driver_dir)
                    
                    # Make the ChromeDriver executable
                    os.chmod(driver_path, 0o755)
                    logging.info(f"ChromeDriver downloaded and extracted to: {driver_path}")
                    
                    # Clean up the zip file
                    os.remove(zip_path)
                except Exception as e:
                    logging.error(f"Error downloading ChromeDriver: {e}")
                    # Try alternative method using Chrome for Testing
                    try:
                        logging.info("Trying alternative download method using Chrome for Testing")
                        alt_url = "https://storage.googleapis.com/chrome-for-testing-public/LATEST_RELEASE_STABLE"
                        chromedriver_version = urllib.request.urlopen(alt_url).read().decode('utf-8')
                        
                        alt_chromedriver_url = f"https://storage.googleapis.com/chrome-for-testing-public/{chromedriver_version}/mac-arm64/chromedriver-mac-arm64.zip"
                        logging.info(f"Downloading from alternative URL: {alt_chromedriver_url}")
                        
                        urllib.request.urlretrieve(alt_chromedriver_url, zip_path)
                        
                        # Extract the zip file
                        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                            zip_ref.extractall(driver_dir)
                        
                        # The extracted path might be different
                        extracted_driver = os.path.join(driver_dir, "chromedriver-mac-arm64", "chromedriver")
                        if os.path.exists(extracted_driver):
                            shutil.copy(extracted_driver, driver_path)
                            os.chmod(driver_path, 0o755)
                            logging.info(f"ChromeDriver copied to: {driver_path}")
                        
                        # Clean up
                        os.remove(zip_path)
                    except Exception as alt_e:
                        logging.error(f"Alternative download method failed: {alt_e}")
                        raise
            
            # Create a Service object with the driver path
            service = Service(executable_path=driver_path)
        else:
            # For other platforms, use the standard approach with a direct path
            from webdriver_manager.chrome import ChromeDriverManager
            driver_path = ChromeDriverManager().install()
            
            # Verify the driver exists and is executable
            if not os.path.isfile(driver_path):
                logging.error(f"ChromeDriver not found at {driver_path}")
                raise FileNotFoundError(f"ChromeDriver not found at {driver_path}")
            
            # Make sure the file is executable
            if not os.access(driver_path, os.X_OK):
                logging.info(f"Making ChromeDriver executable: {driver_path}")
                os.chmod(driver_path, 0o755)
            
            service = Service(executable_path=driver_path)
        
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