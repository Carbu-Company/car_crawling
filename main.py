"""
Main module for the Encar crawler.
"""

import time
import logging
import sys
import platform
import subprocess
import random
from selenium.webdriver.common.by import By
from selenium.common.exceptions import TimeoutException, WebDriverException, UnexpectedAlertPresentException, NoAlertPresentException
import config
import driver_setup
import car_detail_extractor
import pagination_handler
import data_processor
import opensearch_handler

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def cleanup_existing_processes():
    """
    Clean up any existing Chrome and ChromeDriver processes before starting the crawler.
    """
    logging.info("기존 Chrome 및 ChromeDriver 프로세스 정리 중...")
    driver_setup.kill_chrome_processes()
    logging.info("기존 프로세스 정리 완료")

def crawl_page(driver, page_number, all_car_data, opensearch_client=None):
    """
    Crawl a single page of car listings.
    
    Args:
        driver: Selenium WebDriver instance
        page_number: Page number to crawl
        all_car_data: List of all car data collected so far
        opensearch_client: OpenSearch client for indexing (optional)
        
    Returns:
        list: List of car data dictionaries from this page
        bool: Flag indicating if driver needs to be reset
    """
    logging.info(f"\n===== {page_number}페이지 크롤링 시작 =====\n")
    
    # 로봇 감지 후 충분한 시간이 지났는지 확인
    current_time = time.time()
    if hasattr(config, 'LAST_ROBOT_DETECTION') and config.LAST_ROBOT_DETECTION > 0:
        time_since_detection = current_time - config.LAST_ROBOT_DETECTION
        if time_since_detection < config.ROBOT_DETECTION_COOLDOWN:
            wait_time = min(config.ROBOT_DETECTION_COOLDOWN - time_since_detection, 10)  # 최대 60초 대기
            logging.info(f"로봇 감지 후 {wait_time:.0f}초 동안 대기합니다...")
            time.sleep(wait_time)
    
    # 세션 유효성 확인
    if not car_detail_extractor.is_session_valid(driver):
        logging.error("WebDriver 세션이 유효하지 않습니다. 드라이버를 재설정해야 합니다.")
        return [], True
    
    # 페이지로 이동
    try:
        if not pagination_handler.navigate_to_page(driver, page_number):
            return [], False
    except UnexpectedAlertPresentException as e:
        # 알림창 처리
        try:
            alert = driver.switch_to.alert
            alert_text = alert.text
            logging.warning(f"알림창 감지: {alert_text}")
            alert.accept()
            
            # 로봇 감지로 간주
            config.LAST_ROBOT_DETECTION = time.time()
            logging.error("로봇 감지 알림이 표시되었습니다. 잠시 후 다시 시도합니다.")
            
            # 쿨다운 시간 설정 (점진적으로 증가)
            if not hasattr(config, 'ROBOT_DETECTION_COUNT'):
                config.ROBOT_DETECTION_COUNT = 0
            config.ROBOT_DETECTION_COUNT += 1
            
            # 지수 백오프 적용 (최대 30분)
            backoff_time = min(300 * (2 ** config.ROBOT_DETECTION_COUNT), 1800)
            config.ROBOT_DETECTION_COOLDOWN = backoff_time
            
            logging.info(f"로봇 감지 후 {backoff_time}초 동안 대기합니다...")
            
            # 드라이버 재설정 필요
            return [], True
        except NoAlertPresentException:
            logging.error("알림창을 감지했으나 처리할 수 없습니다.")
            return [], True
    
    # 차량 목록 가져오기
    try:
        # 인간 행동 시뮬레이션 - 페이지 로딩 후 약간의 지연
        random_delay = random.uniform(2, 5)
        time.sleep(random_delay)
        
        car_items = driver.find_elements(By.CSS_SELECTOR, config.SELECTORS["car_items"])
        logging.info(f"총 {len(car_items)}개의 차량을 발견했습니다.")
    except UnexpectedAlertPresentException as e:
        # 알림창 처리
        try:
            alert = driver.switch_to.alert
            alert_text = alert.text
            logging.warning(f"알림창 감지: {alert_text}")
            alert.accept()
            
            # 로봇 감지로 간주
            config.LAST_ROBOT_DETECTION = time.time()
            
            # 드라이버 재설정 필요
            return [], True
        except NoAlertPresentException:
            logging.error("알림창을 감지했으나 처리할 수 없습니다.")
            return [], True
    except Exception as e:
        logging.error(f"차량 목록을 가져오는 중 오류 발생: {e}")
        return [], True  # 드라이버 재설정 필요
    
    # 현재 페이지에 차량이 없으면 빈 리스트 반환
    if len(car_items) == 0:
        logging.info("더 이상 차량이 없습니다.")
        return [], False
    
    # 데이터 저장용 리스트 (현재 페이지)
    page_car_data = []
    
    # 진행 상황 표시
    total_cars = len(car_items)
    indexed_count = 0
    reset_needed = False
    
    for idx, car in enumerate(car_items):
        try:
            logging.info(f"차량 {idx+1}/{total_cars} 처리 중...")
            
            # 세션 유효성 재확인
            if not car_detail_extractor.is_session_valid(driver):
                logging.error("WebDriver 세션이 유효하지 않습니다. 나머지 차량 처리를 중단합니다.")
                reset_needed = True
                break
            
            # 기본 차량 정보 추출
            car_info = car_detail_extractor.extract_car_info(car, all_car_data)
            
            # 중복 차량이거나 추출 실패 시 건너뛰기
            if car_info is None:
                continue
            
            # 페이지 번호 추가
            car_info["페이지번호"] = page_number
            
            # 인간 행동 시뮬레이션 - 가변적인 대기 시간
            random_delay = random.uniform(0.5, 2.5)
            time.sleep(random_delay)
            
            # 상세 페이지 정보 가져오기
            logging.info(f"차량 ID {car_info['차량ID']}의 상세 정보를 가져오는 중...")
            try:
                detail_info = car_detail_extractor.get_car_detail_info(driver, car_info["상세페이지URL"])
            except UnexpectedAlertPresentException as e:
                # 알림창 처리
                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text
                    logging.warning(f"알림창 감지: {alert_text}")
                    alert.accept()
                    
                    # 로봇 감지로 간주
                    config.LAST_ROBOT_DETECTION = time.time()
                    
                    # 드라이버 재설정 필요
                    reset_needed = True
                    break
                except NoAlertPresentException:
                    logging.error("알림창을 감지했으나 처리할 수 없습니다.")
                    reset_needed = True
                    break
            
            # 세션 오류 확인
            if "세션오류" in detail_info:
                logging.error("세션 오류가 발생했습니다. 드라이버를 재설정해야 합니다.")
                reset_needed = True
                break
            
            # 기본 정보와 상세 정보 병합
            car_info.update(detail_info)
            
            # 데이터 저장 (현재 페이지 및 전체)
            page_car_data.append(car_info)
            all_car_data.append(car_info)
            
            # OpenSearch에 인덱싱 (클라이언트가 제공된 경우)
            if opensearch_client:
                if opensearch_handler.index_car_to_opensearch(opensearch_client, car_info, idx+1):
                    indexed_count += 1
            
            # 중간 저장
            # data_processor.save_checkpoint(page_car_data, idx, page_number)
            
            # 인간처럼 행동하기 위한 가변적인 대기 시간
            wait_time = config.get_car_processing_wait() * random.uniform(0.8, 1.2)
            time.sleep(wait_time)
            
        except UnexpectedAlertPresentException as e:
            # 알림창 처리
            try:
                alert = driver.switch_to.alert
                alert_text = alert.text
                logging.warning(f"알림창 감지: {alert_text}")
                alert.accept()
                
                # 로봇 감지로 간주
                config.LAST_ROBOT_DETECTION = time.time()
                
                # 드라이버 재설정 필요
                reset_needed = True
                break
            except NoAlertPresentException:
                logging.error("알림창을 감지했으나 처리할 수 없습니다.")
                reset_needed = True
                break
        except Exception as e:
            logging.error(f"차량 정보 추출 중 오류 발생: {e}")
            # 세션 오류인지 확인
            if "invalid session id" in str(e) or "no such session" in str(e):
                logging.error("세션 오류가 감지되었습니다. 드라이버를 재설정해야 합니다.")
                reset_needed = True
                break
            continue
    
    # 현재 페이지 데이터 저장
    # if page_car_data:
    #     data_processor.save_page_data(page_car_data, page_number)
    
    if opensearch_client and page_car_data:
        logging.info(f"페이지 {page_number}에서 총 {indexed_count}/{len(page_car_data)}개의 차량 데이터 인덱싱 완료")
    
    return page_car_data, reset_needed

def crawl_encar(start_page=62, max_pages=None, save_all=True, use_opensearch=True):
    """
    Main function to crawl the Encar website.
    
    Args:
        start_page: Page number to start crawling from
        max_pages: Maximum number of pages to crawl
        save_all: Whether to save all data to a single file
        use_opensearch: Whether to use OpenSearch for indexing
    """
    driver = None
    opensearch_client = None
    
    try:
        # WebDriver 설정
        driver = driver_setup.setup_driver()
        
        # WebDriver 커맨드 타임아웃 설정 (기본 120초에서 600초로 증가)
        if hasattr(driver, 'command_executor'):
            driver.command_executor._conn.timeout = 600.0
            logging.info(f"WebDriver 커맨드 타임아웃을 {driver.command_executor._conn.timeout}초로 설정했습니다.")
            
            # Set page load timeout and script timeout
            driver.set_page_load_timeout(300)
            driver.set_script_timeout(300)
            logging.info("페이지 로드 및 스크립트 타임아웃을 300초로 설정했습니다.")
        
        # 사용자 에이전트 랜덤화 시도 (알림이 뜨는 것을 방지하기 위함)
        try:
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": config.get_random_user_agent()
            })
            logging.info("사용자 에이전트를 랜덤화하였습니다.")
        except Exception as e:
            logging.error(f"사용자 에이전트 설정 실패: {e}")
            
        # 쿠키 수락 및 홈페이지 접속
        try:
            driver.get("http://www.encar.com")
            time.sleep(random.uniform(1, 3))
            
            # 쿠키 동의 버튼이 있으면 클릭
            try:
                cookie_accept = driver.find_element(By.CSS_SELECTOR, ".btn_accept")
                if cookie_accept:
                    cookie_accept.click()
                    logging.info("쿠키 동의 버튼을 클릭했습니다.")
                    time.sleep(random.uniform(1, 2))
            except:
                pass
                
        except Exception as e:
            logging.error(f"홈페이지 접속 중 오류 발생: {e}")
        
        # OpenSearch 클라이언트 설정 (사용하는 경우)
        if use_opensearch:
            try:
                logging.info("OpenSearch 클라이언트 생성 및 인덱스 설정 중...")
                opensearch_client = opensearch_handler.create_opensearch_client()
                opensearch_handler.create_encar_index(opensearch_client)
                logging.info("OpenSearch 설정 완료")
            except Exception as e:
                logging.error(f"OpenSearch 설정 중 오류 발생: {e}")
                logging.warning("OpenSearch 인덱싱 없이 계속 진행합니다.")
                opensearch_client = None
        
        # 데이터 저장용 리스트 (모든 페이지의 데이터를 저장)
        all_car_data = []
        
        # 현재 페이지 번호
        current_page = start_page
        
        # 최대 페이지 수 설정
        if max_pages is None:
            max_pages = config.MAX_PAGES
        
        # 페이지 크롤링 계속 진행 여부
        continue_crawling = True
        pages_crawled = 0
        
        # 로봇 감지 카운터 초기화
        if not hasattr(config, 'ROBOT_DETECTION_COUNT'):
            config.ROBOT_DETECTION_COUNT = 0
            
        # 로봇 감지 쿨다운 초기화
        if not hasattr(config, 'ROBOT_DETECTION_COOLDOWN'):
            config.ROBOT_DETECTION_COOLDOWN = 300  # 기본 5분
            
        # 마지막 로봇 감지 시간 초기화
        if not hasattr(config, 'LAST_ROBOT_DETECTION'):
            config.LAST_ROBOT_DETECTION = 0
        
        while continue_crawling and (pages_crawled < max_pages):
            # 현재 페이지 크롤링
            try:
                # 알림 확인 및 처리
                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text
                    logging.warning(f"페이지 크롤링 전 알림창 감지: {alert_text}")
                    alert.accept()
                    
                    # 로봇 감지로 간주
                    config.LAST_ROBOT_DETECTION = time.time()
                    config.ROBOT_DETECTION_COUNT += 1
                    
                    # 지수 백오프 적용 (최대 30분)
                    backoff_time = min(300 * (2 ** config.ROBOT_DETECTION_COUNT), 1800)
                    config.ROBOT_DETECTION_COOLDOWN = backoff_time
                    
                    logging.info(f"로봇 감지 후 {backoff_time}초 동안 대기합니다...")
                    time.sleep(backoff_time)
                    
                    # 드라이버 재설정
                    if driver:
                        driver_setup.cleanup_driver(driver)
                    driver = driver_setup.setup_driver()
                    if hasattr(driver, 'command_executor'):
                        driver.command_executor._conn.timeout = 600.0
                        driver.set_page_load_timeout(300)
                        driver.set_script_timeout(300)
                        
                    # 사용자 에이전트 랜덤화
                    try:
                        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                            "userAgent": config.get_random_user_agent()
                        })
                    except:
                        pass
                        
                    continue
                except NoAlertPresentException:
                    pass  # 알림이 없으면 계속 진행
                
                page_car_data, reset_needed = crawl_page(driver, current_page, all_car_data, opensearch_client)
                
                # 드라이버 재설정이 필요한 경우
                if reset_needed:
                    logging.warning("WebDriver 세션 오류로 인해 드라이버를 재설정합니다.")
                    
                    # 기존 드라이버 정리
                    if driver:
                        driver_setup.cleanup_driver(driver)
                    
                    # 새 드라이버 설정
                    driver = driver_setup.setup_driver()
                    
                    # WebDriver 타임아웃 재설정
                    if hasattr(driver, 'command_executor'):
                        driver.command_executor._conn.timeout = 600.0
                        driver.set_page_load_timeout(300)
                        driver.set_script_timeout(300)
                    
                    # 사용자 에이전트 랜덤화
                    try:
                        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                            "userAgent": config.get_random_user_agent()
                        })
                    except:
                        pass
                    
                    # 대기 시간 추가 (로봇 감지 완화)
                    wait_time = random.uniform(30, 60)
                    logging.info(f"드라이버 재설정 후 {wait_time:.0f}초 동안 대기합니다...")
                    time.sleep(wait_time)
                    
                    # 현재 페이지 다시 시도
                    logging.info(f"페이지 {current_page}를 다시 시도합니다.")
                    continue
                
                # 페이지에 차량이 없으면 크롤링 종료
                if not page_car_data:
                    logging.info("더 이상 차량이 없습니다. 크롤링을 종료합니다.")
                    break
                
                # 페이지 카운트 증가
                pages_crawled += 1
                
                # 최대 페이지 수 확인
                if pages_crawled >= max_pages:
                    logging.info(f"최대 페이지 수({max_pages})에 도달했습니다. 크롤링을 종료합니다.")
                    break
                
                # 인간 행동 시뮬레이션 - 가변적인 대기 시간
                random_delay = random.uniform(2, 5)
                logging.info(f"다음 페이지로 이동 전 {random_delay:.1f}초 대기...")
                time.sleep(random_delay)
                
                # 다음 페이지로 이동
                try:
                    next_page = pagination_handler.go_to_next_page(driver, current_page)
                    
                    if next_page is None:
                        logging.info("마지막 페이지에 도달했거나 페이지 이동에 실패했습니다.")
                        continue_crawling = False
                    else:
                        current_page = next_page
                except UnexpectedAlertPresentException as e:
                    # 알림창 처리
                    try:
                        alert = driver.switch_to.alert
                        alert_text = alert.text
                        logging.warning(f"다음 페이지 이동 중 알림창 감지: {alert_text}")
                        alert.accept()
                        
                        # 로봇 감지로 간주
                        config.LAST_ROBOT_DETECTION = time.time()
                        config.ROBOT_DETECTION_COUNT += 1
                        
                        # 지수 백오프 적용 (최대 30분)
                        backoff_time = min(300 * (2 ** config.ROBOT_DETECTION_COUNT), 1800)
                        config.ROBOT_DETECTION_COOLDOWN = backoff_time
                        
                        logging.info(f"로봇 감지 후 {backoff_time}초 동안 대기합니다...")
                        time.sleep(backoff_time)
                        
                        # 드라이버 재설정
                        if driver:
                            driver_setup.cleanup_driver(driver)
                        driver = driver_setup.setup_driver()
                        if hasattr(driver, 'command_executor'):
                            driver.command_executor._conn.timeout = 600.0
                            driver.set_page_load_timeout(300)
                            driver.set_script_timeout(300)
                            
                        # 사용자 에이전트 랜덤화
                        try:
                            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                                "userAgent": config.get_random_user_agent()
                            })
                        except:
                            pass
                            
                        # 현재 페이지 유지하고 다시 시도
                        continue
                    except NoAlertPresentException:
                        logging.error("알림창을 감지했으나 처리할 수 없습니다.")
                        # 드라이버 재설정
                        if driver:
                            driver_setup.cleanup_driver(driver)
                        driver = driver_setup.setup_driver()
                        if hasattr(driver, 'command_executor'):
                            driver.command_executor._conn.timeout = 600.0
                            driver.set_page_load_timeout(300)
                            driver.set_script_timeout(300)
                        continue
                except TimeoutException as e:
                    logging.error(f"다음 페이지로 이동 중 타임아웃 발생: {e}")
                    # 드라이버 재설정
                    logging.warning("타임아웃으로 인해 드라이버를 재설정합니다.")
                    if driver:
                        driver_setup.cleanup_driver(driver)
                    driver = driver_setup.setup_driver()
                    # WebDriver 타임아웃 재설정
                    if hasattr(driver, 'command_executor'):
                        driver.command_executor._conn.timeout = 600.0
                        driver.set_page_load_timeout(300)
                        driver.set_script_timeout(300)
                    # 다음 페이지로 이동
                    current_page += 1
                    continue
                
            except UnexpectedAlertPresentException as e:
                # 알림창 처리
                try:
                    alert = driver.switch_to.alert
                    alert_text = alert.text
                    logging.warning(f"크롤링 중 알림창 감지: {alert_text}")
                    alert.accept()
                    
                    # 로봇 감지로 간주
                    config.LAST_ROBOT_DETECTION = time.time()
                    config.ROBOT_DETECTION_COUNT += 1
                    
                    # 지수 백오프 적용 (최대 30분)
                    backoff_time = min(300 * (2 ** config.ROBOT_DETECTION_COUNT), 1800)
                    config.ROBOT_DETECTION_COOLDOWN = backoff_time
                    
                    logging.info(f"로봇 감지 후 {backoff_time}초 동안 대기합니다...")
                    time.sleep(backoff_time)
                    
                    # 드라이버 재설정
                    if driver:
                        driver_setup.cleanup_driver(driver)
                    driver = driver_setup.setup_driver()
                    if hasattr(driver, 'command_executor'):
                        driver.command_executor._conn.timeout = 600.0
                        driver.set_page_load_timeout(300)
                        driver.set_script_timeout(300)
                        
                    # 사용자 에이전트 랜덤화
                    try:
                        driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                            "userAgent": config.get_random_user_agent()
                        })
                    except:
                        pass
                        
                    continue
                except NoAlertPresentException:
                    logging.error("알림창을 감지했으나 처리할 수 없습니다.")
                    # 드라이버 재설정
                    if driver:
                        driver_setup.cleanup_driver(driver)
                    driver = driver_setup.setup_driver()
                    continue
            except TimeoutException as e:
                logging.error(f"페이지 {current_page} 처리 중 타임아웃 발생: {e}")
                # 드라이버 재설정
                if driver:
                    driver_setup.cleanup_driver(driver)
                driver = driver_setup.setup_driver()
                # WebDriver 타임아웃 재설정
                if hasattr(driver, 'command_executor'):
                    driver.command_executor._conn.timeout = 600.0
                    driver.set_page_load_timeout(300)
                    driver.set_script_timeout(300)
                # 다음 페이지로 이동
                current_page += 1
                continue
            except WebDriverException as e:
                if "timeout" in str(e).lower():
                    logging.error(f"WebDriver 타임아웃 발생: {e}")
                    # 드라이버 재설정
                    if driver:
                        driver_setup.cleanup_driver(driver)
                    driver = driver_setup.setup_driver()
                    # WebDriver 타임아웃 재설정
                    if hasattr(driver, 'command_executor'):
                        driver.command_executor._conn.timeout = 600.0
                        driver.set_page_load_timeout(300)
                        driver.set_script_timeout(300)
                    # 다음 페이지로 이동
                    current_page += 1
                    continue
                else:
                    raise
        
        # 모든 페이지의 데이터를 하나의 CSV 파일로 저장
        if all_car_data and save_all:
            data_processor.save_all_data(all_car_data)
            data_processor.print_data_summary(all_car_data)
            
            logging.info(f"\n===== 크롤링 완료 =====")
            logging.info(f"총 {len(all_car_data)}개의 차량 정보를 수집했습니다.")
            
            # OpenSearch 인덱스 통계 출력
            if opensearch_client:
                opensearch_handler.get_index_stats(opensearch_client)
        
    except Exception as e:
        logging.error(f"크롤링 중 오류 발생: {e}")
        import traceback
        logging.error(traceback.format_exc())
    
    finally:
        # 브라우저 종료 전 랜덤 대기
        time.sleep(config.get_browser_close_wait())
        
        # WebDriver 정리
        try:
            if driver:
                # 타임아웃 방지를 위해 안전하게 종료
                try:
                    driver.set_page_load_timeout(30)
                    driver.set_script_timeout(30)
                except Exception:
                    pass
                    
                driver_setup.cleanup_driver(driver)
        except Exception as e:
            logging.error(f"WebDriver 정리 중 오류 발생: {e}")
            # 강제로 프로세스 종료
            driver_setup.kill_chrome_processes()

def main():
    """
    Entry point of the program with retry mechanism.
    """
    logging.info("엔카 차량 데이터 수집 및 OpenSearch 인덱싱 시작...")
    
    # 기존 Chrome 프로세스 정리
    cleanup_existing_processes()
    
    # 최대 재시도 횟수
    retry_count = 0
    
    while retry_count < config.MAX_RETRIES:
        try:
            # OpenSearch 사용 여부 설정 (기본값: True)
            use_opensearch = True
            
            crawl_encar(use_opensearch=use_opensearch)
            break  # 성공적으로 완료되면 루프 종료
        except Exception as e:
            retry_count += 1
            logging.error(f"크롤링 실패 ({retry_count}/{config.MAX_RETRIES}): {e}")
            
            # 실패 시 남아있는 프로세스 정리
            driver_setup.kill_chrome_processes()
            
            if retry_count < config.MAX_RETRIES:
                wait_time = config.get_retry_wait()
                logging.info(f"{wait_time:.0f}초 후 재시도합니다...")
                time.sleep(wait_time)
            else:
                logging.error("최대 재시도 횟수를 초과했습니다. 프로그램을 종료합니다.")
    
    # 프로그램 종료 전 최종 프로세스 정리
    driver_setup.kill_chrome_processes()

if __name__ == "__main__":
    main() 