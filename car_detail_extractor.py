"""
Module for extracting detailed car information from car detail pages.
"""

import time
import logging
import os
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException, 
    NoSuchElementException, 
    WebDriverException,
    InvalidSessionIdException
)
import config
import driver_setup

def is_session_valid(driver):
    """
    Check if the WebDriver session is still valid.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        bool: True if session is valid, False otherwise
    """
    try:
        # 간단한 명령을 실행해서 세션이 유효한지 확인
        driver.current_url
        return True
    except (WebDriverException, InvalidSessionIdException):
        logging.error("WebDriver 세션이 유효하지 않습니다.")
        return False

def get_car_detail_info(driver, detail_url, max_retries=2):
    """
    Get detailed car information from the car detail page.
    
    Args:
        driver: Selenium WebDriver instance
        detail_url: URL of the car detail page
        max_retries: Maximum number of retries
        
    Returns:
        dict: Dictionary containing detailed car information
    """
    detail_info = {}
    retry_count = 0
    original_window = driver.current_window_handle
    
    while retry_count < max_retries:
        try:
            # 세션 유효성 확인
            if not is_session_valid(driver):
                logging.warning("세션이 유효하지 않아 드라이버를 재설정합니다.")
                # 기존 드라이버 정리
                try:
                    driver_setup.cleanup_driver(driver)
                except:
                    pass
                
                # 새 드라이버 설정
                driver = driver_setup.setup_driver()
                driver_setup.navigate_to_url(driver, config.BASE_URL)
                time.sleep(config.get_page_load_wait())
                
                # 세션 재설정 후 상세 정보 가져오기 중단
                return {"세션오류": "세션이 재설정되었습니다"}
            
            # 현재 열려있는 창 수 확인
            window_count_before = len(driver.window_handles)
            
            # 새 탭에서 상세 페이지 열기
            driver.execute_script(f"window.open('{detail_url}', '_blank');")
            
            # 새 탭으로 전환
            WebDriverWait(driver, 10).until(
                lambda d: len(d.window_handles) > window_count_before
            )
            driver.switch_to.window(driver.window_handles[-1])
            
            # 페이지 로드 대기
            time.sleep(config.get_detail_page_load_wait())
            
            # 세부정보 버튼 클릭
            try:
                # 스크롤을 버튼 위치로 이동
                detail_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config.SELECTORS["detail_button"]))
                )
                
                # 버튼이 보이도록 스크롤
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
                time.sleep(1)  # 스크롤 후 잠시 대기
                
                # 버튼 클릭
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, config.SELECTORS["detail_button"]))
                ).click()
                
                # 팝업이 나타날 때까지 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config.SELECTORS["detail_popup"]))
                )
                
                # 세부 정보 추출
                detail_items = driver.find_elements(By.CSS_SELECTOR, config.SELECTORS["detail_items"])
                
                for item in detail_items:
                    try:
                        key_element = item.find_element(By.CSS_SELECTOR, config.SELECTORS["detail_key"])
                        key = key_element.text
                        
                        # 툴팁 버튼이 있는 경우 제거
                        if "조회수" in key:
                            key = "조회수"
                        
                        value = item.find_element(By.CSS_SELECTOR, config.SELECTORS["detail_value"]).text
                        
                        # 키 이름 정리
                        key_mapping = {
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
                        
                        mapped_key = key_mapping.get(key, key)
                        detail_info[mapped_key] = value
                        
                    except Exception as e:
                        logging.warning(f"세부 정보 항목 추출 중 오류: {e}")
                        continue
                
                # 성공적으로 정보를 가져왔으면 루프 종료
                break
                    
            except Exception as e:
                logging.error(f"세부정보 버튼 클릭 또는 팝업 처리 중 오류: {e}")
                retry_count += 1
                
                # 오류 스크린샷 저장
                try:
                    screenshot_file = config.get_error_screenshot_filename()
                    driver.save_screenshot(screenshot_file)
                    logging.info(f"오류 스크린샷이 {screenshot_file}에 저장되었습니다.")
                except:
                    pass
            
            finally:
                # 탭 닫기
                try:
                    driver.close()
                except (WebDriverException, InvalidSessionIdException):
                    logging.warning("탭을 닫는 중 오류가 발생했습니다. 세션이 유효하지 않을 수 있습니다.")
                
                # 원래 탭으로 돌아가기
                try:
                    driver.switch_to.window(original_window)
                except (WebDriverException, InvalidSessionIdException):
                    logging.error("원래 탭으로 돌아가는 중 오류가 발생했습니다. 세션이 유효하지 않습니다.")
                    return {"세션오류": "세션이 유효하지 않습니다"}
                
        except Exception as e:
            logging.error(f"상세 페이지 처리 중 오류 발생: {e}")
            retry_count += 1
            
            # 오류 발생 시 원래 탭으로 돌아가기
            try:
                if len(driver.window_handles) > 1:
                    driver.close()
                    driver.switch_to.window(original_window)
            except (WebDriverException, InvalidSessionIdException):
                logging.error("창 처리 중 오류가 발생했습니다. 세션이 유효하지 않습니다.")
                return {"세션오류": "세션이 유효하지 않습니다"}
    
    return detail_info

def extract_car_info(car, all_car_data):
    """
    Extract basic information from a car listing element.
    
    Args:
        car: Selenium WebElement representing a car listing
        all_car_data: List of all car data collected so far (for duplicate checking)
        
    Returns:
        dict or None: Dictionary with car information or None if it's a duplicate
    """
    try:
        # 차량 ID 및 인덱스 추출
        car_index = car.get_attribute(config.SELECTORS["car"]["index"])
        impression_data = car.get_attribute(config.SELECTORS["car"]["impression"])
        car_id = impression_data.split("|")[0] if impression_data else None
        
        # 이미 처리한 차량인지 확인 (중복 방지)
        if any(item.get("차량ID") == car_id for item in all_car_data):
            logging.info(f"차량 ID {car_id}는 이미 처리되었습니다. 건너뜁니다.")
            return None
        
        # 이미지 URL 추출
        img_url = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["img"]).get_attribute("src")
        
        # 서비스 배지 추출 (진단, 믿고 등)
        badges = []
        badge_elements = car.find_elements(By.CSS_SELECTOR, config.SELECTORS["car"]["badges"])
        for badge in badge_elements:
            badges.append(badge.text)
        
        # 제조사와 모델 정보
        manufacturer = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["manufacturer"]).text
        model = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["model"]).text
        
        # 세부 모델명
        detail_model = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["detail_model"]).text.strip()
        
        # 연식, 주행거리, 연료, 지역 정보
        year = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["year"]).text
        mileage = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["mileage"]).text
        fuel_type = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["fuel"]).text
        location = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["location"]).text
        
        # 성능기록, 엔카진단 여부 확인
        has_performance_record = len(car.find_elements(By.CSS_SELECTOR, config.SELECTORS["car"]["performance_record"])) > 0
        has_encar_diagnosis = len(car.find_elements(By.CSS_SELECTOR, config.SELECTORS["car"]["diagnosis"])) > 0
        
        # 가격 정보 (prc_hs 클래스가 있는 td 요소)
        try:
            price_element = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["price_hs"])
            price_value = price_element.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["price_value"]).text
            price_unit = price_element.text.replace(price_value, "").strip()
            full_price = price_value + price_unit
        except:
            # 다른 가격 클래스 시도
            try:
                price_element = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["price"])
                price_value = price_element.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["price_value"]).text
                price_unit = price_element.text.replace(price_value, "").strip()
                full_price = price_value + price_unit
            except:
                price_value = "정보없음"
                price_unit = ""
                full_price = "정보없음"
        
        # 차량 상세 페이지 URL
        detail_url = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["detail_url"]).get_attribute("href")
        
        # 광고 정보 추출 시도
        ad_info = ""
        try:
            ad_info = car.find_element(By.CSS_SELECTOR, config.SELECTORS["car"]["ad_info"]).text
        except:
            pass
        
        # 기본 데이터 저장
        return {
            "차량ID": car_id,
            "인덱스": car_index,
            "제조사": manufacturer,
            "모델": model,
            "세부모델": detail_model,
            "연식": year,
            "주행거리": mileage,
            "연료": fuel_type,
            "지역": location,
            "가격": full_price,
            "가격값": price_value,
            "가격단위": price_unit,
            "이미지URL": img_url,
            "배지": ", ".join(badges),
            "성능기록여부": has_performance_record,
            "엔카진단여부": has_encar_diagnosis,
            "광고정보": ad_info,
            "상세페이지URL": detail_url,
            "크롤링시간": time.strftime("%Y-%m-%d %H:%M:%S")
        }
        
    except Exception as e:
        logging.error(f"차량 정보 추출 중 오류 발생: {e}")
        return None 