"""
Module for setting up the Selenium WebDriver with anti-detection measures.
"""

import os
import time
import logging
import platform
import subprocess
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from webdriver_manager.chrome import ChromeDriverManager
import config
import tempfile
import uuid
import random
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

def setup_driver():
    """
    Set up and configure a Chrome WebDriver with anti-detection measures.

    Returns:
        WebDriver: Configured Chrome WebDriver instance
    """
    try:
        # 크롬 옵션 설정
        chrome_options = Options()
        if config.HEADLESS_MODE:
            chrome_options.add_argument("--headless=new")  # 최신 headless 모드 사용
            # headless 모드에서는 window-size 옵션이 필요할 수 있음
            chrome_options.add_argument(f"--window-size={config.WINDOW_SIZE}")
        
        # 차단 방지를 위한 설정들
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument(f'user-agent={config.USER_AGENT}')
        
        # 사용자 데이터 디렉토리 관련 오류 방지를 위해 유니크한 사용자 데이터 디렉토리 설정
        temp_dir = os.path.join(tempfile.gettempdir(), f"chrome_data_{uuid.uuid4().hex}")
        chrome_options.add_argument(f"--user-data-dir={temp_dir}")
        
        # 자동화 감지 플래그 제거
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        
        # WebDriver 설정 및 다운로드
        driver_path = ChromeDriverManager().install()
        service = Service(driver_path)
        
        # 서비스 시작 타임아웃 증가
        service.start_error_message = "Chrome 드라이버 서비스를 시작하지 못했습니다."
        service.service_url = None
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # 타임아웃 설정
        if hasattr(driver, 'command_executor'):
            driver.command_executor._conn.timeout = 300.0  # 120초에서 300초로 증가
        
        # 자동화 감지 회피를 위한 추가 설정
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        
        # 페이지 로드 타임아웃 설정
        driver.set_page_load_timeout(60)  # 30초에서 60초로 증가
        
        # 요청 타임아웃 설정
        capabilities = DesiredCapabilities.CHROME
        capabilities["pageLoadStrategy"] = "eager"  # 페이지가 일부만 로드되어도 진행
        capabilities["timeouts"] = {
            "implicit": 30000,  # 암시적 대기 시간 (밀리초)
            "pageLoad": 60000,  # 페이지 로드 타임아웃 (밀리초)
            "script": 30000     # 스크립트 실행 타임아웃 (밀리초)
        }
        
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
        # 페이지 로딩을 WebDriverWait로 대체하여 안정성 향상
        WebDriverWait(driver, 10).until(lambda d: d.execute_script("return document.readyState") == "complete")
        logging.info("페이지 로딩 완료")
        
        # 로봇 감지 확인 및 처리
        if not handle_robot_check(driver):
            logging.warning("로봇 감지 처리에 실패했습니다.")
            return False
            
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
        # 예시: 쿠키 동의 팝업 닫기
        try:
            cookie_button = WebDriverWait(driver, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, ".cookie-consent-button"))
            )
            cookie_button.click()
            logging.info("쿠키 동의 팝업을 닫았습니다.")
        except TimeoutException:
            logging.debug("쿠키 동의 팝업이 나타나지 않음.")

        # 예시: 기타 팝업 닫기
        close_buttons = driver.find_elements(By.CSS_SELECTOR, ".popup-close, .modal-close, .close-button")
        for button in close_buttons:
            try:
                if button.is_displayed() and button.is_enabled():
                    button.click()
                    # 클릭 후 약간의 대기
                    WebDriverWait(driver, 2).until(EC.invisibility_of_element(button))
                    logging.info("팝업을 닫았습니다.")
            except Exception as e:
                logging.debug(f"팝업 닫기 시도 중 오류 발생: {e}")

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
            logging.info("WebDriver 종료 중...")
            
            # 열려있는 모든 창 닫기 시도
            try:
                for handle in driver.window_handles:
                    driver.switch_to.window(handle)
                    driver.close()
                logging.info("모든 브라우저 창을 닫았습니다.")
            except Exception as e:
                logging.warning(f"브라우저 창 닫기 중 오류 발생: {e}")
            
            # 드라이버 종료
            try:
                driver.quit()
                logging.info("WebDriver가 정상적으로 종료되었습니다.")
            except Exception as e:
                logging.warning(f"WebDriver 종료 중 오류 발생: {e}")
            
            # Chrome 및 ChromeDriver 프로세스 강제 종료
            kill_chrome_processes()
            
    except Exception as e:
        logging.error(f"WebDriver 정리 중 오류 발생: {e}")
        # 오류가 발생해도 프로세스 정리 시도
        kill_chrome_processes()

def kill_chrome_processes():
    """
    Kill any remaining Chrome and ChromeDriver processes.
    """
    try:
        system = platform.system()
        
        if system == "Darwin" or system == "Linux":  # macOS 또는 Linux
            logging.info("Chrome 관련 프로세스 정리 중...")
            
            # Chrome 프로세스 종료
            subprocess.run(["pkill", "-f", "chrome"], check=False)
            
            # ChromeDriver 프로세스 종료
            subprocess.run(["pkill", "-f", "chromedriver"], check=False)
            
            logging.info("Chrome 관련 프로세스 정리 완료")
            
        elif system == "Windows":  # Windows
            logging.info("Chrome 관련 프로세스 정리 중...")
            
            # Windows에서는 taskkill 명령어 사용
            subprocess.run(["taskkill", "/F", "/IM", "chrome.exe"], check=False, shell=True)
            subprocess.run(["taskkill", "/F", "/IM", "chromedriver.exe"], check=False, shell=True)
            
            logging.info("Chrome 관련 프로세스 정리 완료")
            
    except Exception as e:
        logging.error(f"프로세스 정리 중 오류 발생: {e}")

def take_screenshot(driver, filename=None):
    """
    Take a screenshot of the current page.

    Args:
        driver: Selenium WebDriver instance
        filename: Optional filename for the screenshot

    Returns:
        str: Path to the saved screenshot or None if error occurred
    """
    try:
        screenshots_dir = "screenshots"
        if not os.path.exists(screenshots_dir):
            os.makedirs(screenshots_dir)
        
        if not filename:
            # 타임스탬프를 이용한 파일명 생성
            timestamp = time.strftime("%Y%m%d-%H%M%S")
            filename = os.path.join(screenshots_dir, f"screenshot_{timestamp}.png")
        
        # 스크린샷 저장
        if driver.save_screenshot(filename):
            logging.info(f"스크린샷이 {filename}에 저장되었습니다.")
            return filename
        else:
            logging.error("스크린샷 저장에 실패했습니다.")
            return None

    except Exception as e:
        logging.error(f"스크린샷 저장 중 오류 발생: {e}")
        return None

def handle_robot_check(driver):
    """
    브라우저가 로봇인지 확인하는 페이지를 감지하고 자동으로 처리를 시도합니다.
    실패하더라도 계속 진행할 수 있도록 항상 True를 반환합니다.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        bool: Always True to continue the process
    """
    try:
        # 현재 페이지가 로봇 검사 페이지인지 확인
        # URL이나 특정 텍스트를 기반으로 확인
        if "recaptcha" in driver.page_source.lower() or "robot" in driver.page_source.lower() or "보안문자" in driver.page_source.lower():
            logging.warning("로봇 감지 페이지가 발견되었습니다. 자동 처리를 시도합니다.")
            
            # 현재 URL 저장
            current_url = driver.current_url
            logging.info(f"현재 URL 저장: {current_url}")
            
            # 유저 에이전트 무작위화
            try:
                new_user_agent = random.choice(config.USER_AGENTS)
                driver.execute_cdp_cmd('Network.setUserAgentOverride', {"userAgent": new_user_agent})
                logging.info(f"새로운 User-Agent 설정: {new_user_agent}")
            except:
                logging.warning("User-Agent 변경에 실패했습니다.")
            
            # IP 우회를 위한 브라우저 재실행 시도
            logging.info("새로운 세션으로 재시도합니다...")
            
            # 스크린샷 저장 (디버깅 목적)
            try:
                take_screenshot(driver, "robot_detection.png")
            except:
                pass
            
            # 랜덤 대기 시간 추가 (더 긴 시간으로 설정)
            wait_time = random.uniform(8, 15)
            logging.info(f"{wait_time:.2f}초 동안 대기합니다...")
            time.sleep(wait_time)
            
            # 메인 페이지로 이동하여 쿠키 초기화
            try:
                driver.get("https://www.encar.com")
                time.sleep(3)
                logging.info("메인 페이지로 이동했습니다.")
                
                # 메인 페이지에서 일부 링크를 클릭하는 등의 사용자 활동 시뮬레이션
                try:
                    links = driver.find_elements(By.TAG_NAME, "a")
                    if links:
                        random_link = random.choice(links[:5])  # 상단의 링크 중 하나를 선택
                        if random_link.is_displayed() and random_link.is_enabled():
                            logging.info("사용자 행동 시뮬레이션: 랜덤 링크 클릭")
                            random_link.click()
                            time.sleep(3)
                            driver.back()
                            time.sleep(2)
                except:
                    pass
            except Exception as e:
                logging.error(f"메인 페이지로 이동 실패: {e}")
            
            # 세션 초기화를 위해 브라우저 쿠키 삭제
            try:
                driver.delete_all_cookies()
                logging.info("모든 쿠키를 삭제했습니다.")
                time.sleep(2)
            except Exception as e:
                logging.error(f"쿠키 삭제 중 오류 발생: {e}")
            
            # 다시 원래 페이지로 이동
            try:
                logging.info(f"원래 페이지로 다시 이동합니다: {current_url}")
                driver.get(current_url)
                time.sleep(5)  # 페이지 로딩 대기
                
                # 로봇 감지가 여전히 있는지 확인
                if "recaptcha" in driver.page_source.lower() or "robot" in driver.page_source.lower() or "보안문자" in driver.page_source.lower():
                    logging.warning("로봇 감지가 계속됩니다. 하지만 계속 진행합니다.")
                    # 마지막 로봇 감지 시간 기록
                    config.LAST_ROBOT_DETECTION = time.time()
                    return True  # 여전히 로봇 감지가 있어도 계속 진행
            except Exception as e:
                logging.error(f"원래 페이지로 돌아가는 중 오류 발생: {e}")
                return True  # 오류가 발생해도 계속 진행
                
        # 로봇 확인 페이지가 아니면 그냥 진행
        return True
        
    except Exception as e:
        logging.error(f"로봇 감지 처리 중 오류 발생: {e}")
        return True  # 오류가 발생해도 계속 진행 (항상 True 반환)
