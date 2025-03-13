from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import time
import random
import pandas as pd

def setup_driver():
    # 크롬 옵션 설정
    chrome_options = Options()
    
    # 차단 방지를 위한 설정들
    # chrome_options.add_argument("--headless")  # 웹 브라우저를 열지 않도록 설정
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-gpu")
    
    # User-Agent 설정 (실제 브라우저처럼 보이게)
    user_agent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    chrome_options.add_argument(f'user-agent={user_agent}')
    
    # 브라우저 창 크기 설정 (headless 모드에서는 무시됨)
    chrome_options.add_argument("--window-size=1920,1080")
    
    # 자동화 감지 플래그 제거
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # 드라이버 설정
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
    # 자동화 감지 회피를 위한 추가 설정
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    
    return driver

def crawl_encar():
    driver = setup_driver()
    
    try:
        # 엔카 벤츠 검색 URL (첫 페이지)
        base_url = "http://www.encar.com/fc/fc_carsearchlist.do?carType=for#!%7B%22action%22%3A%22(And.Hidden.N._.(C.CarType.N._.Manufacturer.%EB%B2%A4%EC%B8%A0.))%22%2C%22toggle%22%3A%7B%7D%2C%22layer%22%3A%22%22%2C%22sort%22%3A%22ModifiedDate%22%2C%22page%22%3A{page}%2C%22limit%22%3A%2250%22%2C%22searchKey%22%3A%22%22%2C%22loginCheck%22%3Afalse%7D"
        
        # 데이터 저장용 리스트 (모든 페이지의 데이터를 저장)
        all_car_data = []
        
        # 현재 페이지 번호
        current_page = 1
        
        # 페이지 크롤링 계속 진행 여부
        continue_crawling = True
        
        while continue_crawling:
            print(f"\n===== {current_page}페이지 크롤링 시작 =====\n")
            
            # 현재 페이지 URL 설정
            url = base_url.format(page=current_page)
            
            # 페이지 로드
            driver.get(url)
            
            # 페이지가 완전히 로드될 때까지 대기
            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "#sr_normal"))
            )
            
            # 인간처럼 행동하기 위한 랜덤 대기
            time.sleep(random.uniform(2, 5))
            
            # 차량 목록 가져오기 (tr 요소들)
            car_items = driver.find_elements(By.CSS_SELECTOR, "#sr_normal > tr")
            print(f"총 {len(car_items)}개의 차량을 발견했습니다.")
            
            # 현재 페이지에 차량이 없으면 크롤링 종료
            if len(car_items) == 0:
                print("더 이상 차량이 없습니다. 크롤링을 종료합니다.")
                break
            
            # 데이터 저장용 리스트 (현재 페이지)
            car_data = []
            
            # 진행 상황 표시
            total_cars = len(car_items)
            
            for idx, car in enumerate(car_items):
                try:
                    print(f"차량 {idx+1}/{total_cars} 처리 중...")
                    
                    # 차량 ID 및 인덱스 추출
                    car_index = car.get_attribute("data-index")
                    impression_data = car.get_attribute("data-impression")
                    car_id = impression_data.split("|")[0] if impression_data else None
                    
                    # 이미 처리한 차량인지 확인 (중복 방지)
                    if any(item.get("차량ID") == car_id for item in all_car_data):
                        print(f"차량 ID {car_id}는 이미 처리되었습니다. 건너뜁니다.")
                        continue
                    
                    # 이미지 URL 추출
                    img_url = car.find_element(By.CSS_SELECTOR, "td.img img.thumb").get_attribute("src")
                    
                    # 서비스 배지 추출 (진단, 믿고 등)
                    badges = []
                    badge_elements = car.find_elements(By.CSS_SELECTOR, "td.img .service_badge_list em")
                    for badge in badge_elements:
                        badges.append(badge.text)
                    
                    # 제조사와 모델 정보
                    manufacturer = car.find_element(By.CSS_SELECTOR, "td.inf .cls strong").text
                    model = car.find_element(By.CSS_SELECTOR, "td.inf .cls em").text
                    
                    # 세부 모델명
                    detail_model = car.find_element(By.CSS_SELECTOR, "td.inf .dtl strong").text.strip()
                    
                    # 연식, 주행거리, 연료, 지역 정보
                    year = car.find_element(By.CSS_SELECTOR, "td.inf .detail .yer").text
                    mileage = car.find_element(By.CSS_SELECTOR, "td.inf .detail .km").text
                    fuel_type = car.find_element(By.CSS_SELECTOR, "td.inf .detail .fue").text
                    location = car.find_element(By.CSS_SELECTOR, "td.inf .detail .loc").text
                    
                    # 성능기록, 엔카진단 여부 확인
                    has_performance_record = len(car.find_elements(By.CSS_SELECTOR, "td.inf .detail .ins")) > 0
                    has_encar_diagnosis = len(car.find_elements(By.CSS_SELECTOR, "td.inf .detail .ass")) > 0
                    
                    # 가격 정보 (prc_hs 클래스가 있는 td 요소)
                    try:
                        price_element = car.find_element(By.CSS_SELECTOR, "td.prc_hs")
                        price_value = price_element.find_element(By.CSS_SELECTOR, "strong").text
                        price_unit = price_element.text.replace(price_value, "").strip()
                        full_price = price_value + price_unit
                    except:
                        # 다른 가격 클래스 시도
                        try:
                            price_element = car.find_element(By.CSS_SELECTOR, "td.prc")
                            price_value = price_element.find_element(By.CSS_SELECTOR, "strong").text
                            price_unit = price_element.text.replace(price_value, "").strip()
                            full_price = price_value + price_unit
                        except:
                            price_value = "정보없음"
                            price_unit = ""
                            full_price = "정보없음"
                    
                    # 차량 상세 페이지 URL
                    detail_url = car.find_element(By.CSS_SELECTOR, "td.inf a").get_attribute("href")
                    
                    # 광고 정보 추출 시도
                    ad_info = ""
                    try:
                        ad_info = car.find_element(By.CSS_SELECTOR, "td.img .box_advertise .desc_advertise").text
                    except:
                        pass
                    
                    # 기본 데이터 저장
                    car_info = {
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
                        "크롤링시간": time.strftime("%Y-%m-%d %H:%M:%S"),
                        "페이지번호": current_page
                    }
                    
                    # 상세 페이지 정보 가져오기
                    print(f"차량 ID {car_id}의 상세 정보를 가져오는 중...")
                    detail_info = get_car_detail_info(driver, detail_url)
                    
                    # 기본 정보와 상세 정보 병합
                    car_info.update(detail_info)
                    
                    # 데이터 저장 (현재 페이지 및 전체)
                    car_data.append(car_info)
                    all_car_data.append(car_info)
                    
                    # 인간처럼 행동하기 위한 짧은 대기
                    time.sleep(random.uniform(1.5, 3.0))
                    
                except Exception as e:
                    print(f"차량 정보 추출 중 오류 발생: {e}")
                    continue
            
            # 현재 페이지 데이터 저장
            # if car_data:
            #     page_df = pd.DataFrame(car_data)
            #     page_filename = f"encar_benz_data_page{current_page}_{time.strftime('%Y%m%d_%H%M%S')}.csv"
            #     page_df.to_csv(page_filename, index=False, encoding="utf-8-sig")
            #     print(f"페이지 {current_page}의 데이터가 {page_filename}에 저장되었습니다.")
            
            # 다음 페이지로 이동
            try:
                # 페이지네이션 요소 찾기
                pagination = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#pagination"))
                )
                
                # 다음 페이지 버튼 찾기 (현재 페이지가 10의 배수이면 다음 10페이지 버튼, 아니면 다음 페이지 번호)
                if current_page % 10 == 0:
                    # 다음 10페이지 버튼 클릭
                    next_button = pagination.find_element(By.CSS_SELECTOR, "span.next a")
                    next_page = int(next_button.get_attribute("data-page"))
                    next_button.click()
                else:
                    # 다음 페이지 번호 클릭
                    next_page = current_page + 1
                    next_page_link = pagination.find_element(By.CSS_SELECTOR, f"a[data-page='{next_page}']")
                    next_page_link.click()
                
                # 페이지 번호 업데이트
                current_page = next_page
                
                # 페이지 로드 대기
                time.sleep(random.uniform(3, 5))
                
                # 최대 페이지 제한 (선택적)
                max_pages = 300  # 최대 300페이지까지만 크롤링
                if current_page > max_pages:
                    print(f"최대 페이지 수({max_pages})에 도달했습니다. 크롤링을 종료합니다.")
                    break
                
            except Exception as e:
                print(f"다음 페이지로 이동 중 오류 발생: {e}")
                print("마지막 페이지에 도달했거나 페이지 이동에 실패했습니다.")
                continue_crawling = False
        
        # 모든 페이지의 데이터를 하나의 CSV 파일로 저장
        if all_car_data:          
            print(f"\n===== 크롤링 완료 =====")
            print(f"총 {len(all_car_data)}개의 차량 정보를 수집했습니다.")
            
        
    except Exception as e:
        print(f"크롤링 중 오류 발생: {e}")
    
    finally:
        # 브라우저 종료 전 랜덤 대기
        time.sleep(random.uniform(2, 5))
        driver.quit()

def get_car_detail_info(driver, detail_url):
    """차량 상세 페이지에서 세부 정보를 추출하는 함수"""
    detail_info = {}
    max_retries = 2
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 새 탭에서 상세 페이지 열기
            driver.execute_script(f"window.open('{detail_url}', '_blank');")
            
            # 새 탭으로 전환
            driver.switch_to.window(driver.window_handles[-1])
            
            # 페이지 로드 대기
            time.sleep(random.uniform(4, 6))
            
            # 세부정보 버튼 클릭
            try:
                # 스크롤을 버튼 위치로 이동
                detail_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "#wrap > div > div.Layout_contents__MD95o > div.ResponsiveLayout_wrap__XLqcM.ResponsiveLayout_wide__VYk4x > div.ResponsiveLayout_content_area__yyYYv > div:nth-child(1) > div > button"))
                )
                
                # 버튼이 보이도록 스크롤
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", detail_button)
                time.sleep(1)  # 스크롤 후 잠시 대기
                
                # 버튼 클릭
                WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.CSS_SELECTOR, "#wrap > div > div.Layout_contents__MD95o > div.ResponsiveLayout_wrap__XLqcM.ResponsiveLayout_wide__VYk4x > div.ResponsiveLayout_content_area__yyYYv > div:nth-child(1) > div > button"))
                ).click()
                
                # 팝업이 나타날 때까지 대기
                WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, ".BottomSheet-module_bottom_sheet__LeljN"))
                )
                
                # 세부 정보 추출
                detail_items = driver.find_elements(By.CSS_SELECTOR, ".DetailSpec_list_default__Gx\\+ZA li")
                
                for item in detail_items:
                    try:
                        key_element = item.find_element(By.CSS_SELECTOR, ".DetailSpec_tit__BRQb\\+")
                        key = key_element.text
                        
                        # 툴팁 버튼이 있는 경우 제거
                        if "조회수" in key:
                            key = "조회수"
                        
                        value = item.find_element(By.CSS_SELECTOR, ".DetailSpec_txt__NGapF").text
                        
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
                        print(f"세부 정보 항목 추출 중 오류: {e}")
                        continue
                
                # 성공적으로 정보를 가져왔으면 루프 종료
                break
                    
            except Exception as e:
                print(f"세부정보 버튼 클릭 또는 팝업 처리 중 오류: {e}")
                retry_count += 1
            
            finally:
                # 탭 닫기
                driver.close()
                
                # 원래 탭으로 돌아가기
                driver.switch_to.window(driver.window_handles[0])
                
        except Exception as e:
            print(f"상세 페이지 처리 중 오류 발생: {e}")
            retry_count += 1
            
            # 오류 발생 시 원래 탭으로 돌아가기
            if len(driver.window_handles) > 1:
                driver.close()
                driver.switch_to.window(driver.window_handles[0])
    
    return detail_info

def main():
    # 최대 재시도 횟수
    max_retries = 3
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            crawl_encar()
            break  # 성공적으로 완료되면 루프 종료
        except Exception as e:
            retry_count += 1
            print(f"크롤링 실패 ({retry_count}/{max_retries}): {e}")
            
            if retry_count < max_retries:
                wait_time = random.uniform(30, 60)
                print(f"{wait_time:.0f}초 후 재시도합니다...")
                time.sleep(wait_time)
            else:
                print("최대 재시도 횟수를 초과했습니다. 프로그램을 종료합니다.")

if __name__ == "__main__":
    main()