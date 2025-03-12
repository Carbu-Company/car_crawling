"""
Module for extracting detailed car information from car detail pages.
"""

import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import config

def get_car_detail_info(driver, detail_url):
    """
    Extract detailed information from a car's detail page.
    
    Args:
        driver: Selenium WebDriver instance
        detail_url: URL of the car detail page
        
    Returns:
        dict: Dictionary containing detailed car information
    """
    detail_info = {}
    retry_count = 0
    
    while retry_count < config.MAX_DETAIL_RETRIES:
        try:
            # 새 탭에서 상세 페이지 열기
            driver.execute_script(f"window.open('{detail_url}', '_blank');")
            
            # 새 탭으로 전환
            driver.switch_to.window(driver.window_handles[-1])
            
            # 페이지 로드 대기
            time.sleep(config.get_detail_page_wait())
            
            # 세부정보 버튼 클릭
            try:
                # 스크롤을 버튼 위치로 이동
                detail_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, config.SELECTORS["detail_button"]))
                )
                
                # 버튼이 보이도록 스크롤
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
                time.sleep(config.get_scroll_wait())  # 스크롤 후 잠시 대기
                
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
                        mapped_key = config.DETAIL_KEY_MAPPING.get(key, key)
                        detail_info[mapped_key] = value
                        
                    except Exception as e:
                        logging.warning(f"세부 정보 항목 추출 중 오류: {e}")
                        continue
                
                # 성공적으로 정보를 가져왔으면 루프 종료
                break
                    
            except Exception as e:
                logging.error(f"세부정보 버튼 클릭 또는 팝업 처리 중 오류: {e}")
                retry_count += 1
                
                # 스크린샷 저장 (디버깅용)
                try:
                    screenshot_file = config.get_error_screenshot_filename()
                    driver.save_screenshot(screenshot_file)
                    logging.info(f"오류 스크린샷이 {screenshot_file}에 저장되었습니다.")
                except:
                    pass
            
            finally:
                # 탭 닫기
                driver.close()
                
                # 원래 탭으로 돌아가기
                driver.switch_to.window(driver.window_handles[0])
                
        except Exception as e:
            logging.error(f"상세 페이지 처리 중 오류 발생: {e}")
            retry_count += 1
            
            # 오류 발생 시 원래 탭으로 돌아가기
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
    
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