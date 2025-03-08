import requests
from bs4 import BeautifulSoup
import time
from opensearchpy import OpenSearch
from datetime import datetime
import logging
import sys
import random
from fake_useragent import UserAgent
import socket
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

def create_opensearch_client():
    opensearch = OpenSearch(
        "http://14.6.96.11:1006",
        http_auth=("admin", "Myopensearch!1"),
        timeout=180,
        max_retries=30,
        retry_on_timeout=True
    )
    return opensearch

def create_carku_index(client):
    index_name = 'carku_goods_detail'
    
    try:
        if not client.indices.exists(index=index_name):
            index_body = {
    'settings': {
        'index': {
            'number_of_shards': 2,
            'number_of_replicas': 1
        },
        'similarity': {
                'scripted_no_idf': {
                  'type': 'scripted',
                  'script': {
                    'source': "double tf = Math.sqrt(doc.freq); double norm = 1/Math.sqrt(doc.length); return query.boost * tf * norm;",
                  },
                },
              },
    },
    'mappings': {
        'properties': {
            'image_url': {'type': 'keyword'},
            'detail_page': {'type': 'keyword'},
            'car_info': {
                'type': 'text',
                'norms': 'false',
                'similarity': 'scripted_no_idf'
            },
            'transmission': {'type': 'keyword'},
            'year': {'type': 'keyword'},
            'fuel': {'type': 'keyword'},
            'mileage': {'type': 'keyword'},
            'price': {'type': 'keyword'},
            'contact_info': {'type': 'keyword'},
            # 상세 페이지에서 추출한 데이터 필드 추가
            'all_images': {'type': 'keyword'},
            'sale_price': {'type': 'keyword'},
            'car_number': {'type': 'keyword'},
            'year_model': {'type': 'keyword'},
            'registration_date': {'type': 'keyword'},
            'fuel_type': {'type': 'keyword'},
            'transmission_type': {'type': 'keyword'},
            'color': {'type': 'keyword'},
            'detailed_mileage': {'type': 'keyword'},
            'vin': {'type': 'keyword'},
            'performance_number': {'type': 'keyword'},
            'seizure_info': {'type': 'keyword'},
            'accident_info': {'type': 'keyword'},
            'tax_unpaid': {'type': 'keyword'},
            'reference_number': {'type': 'keyword'},
            'association_info': {'type': 'text'},
            'seller_name': {'type': 'keyword'},
            'seller_contact': {'type': 'keyword'},
            'seller_company': {'type': 'keyword'},
            'seller_license': {'type': 'keyword'},
            'seller_address': {'type': 'keyword'},
            'seller_img_url': {'type': 'keyword'},
            'timestamp': {'type': 'date'}
        }
    }
}
            client.indices.create(index=index_name, body=index_body)
            logging.info(f"Created index: {index_name}")
    except Exception as e:
        logging.error(f"Error creating index: {str(e)}")
        raise

def scrape_car_data_from_page(soup):
    table = soup.find('table', class_='one_list')
    if not table:
        logging.warning("테이블을 찾을 수 없습니다.")
        return []
        
    car_data = []
    rows = table.find_all('tr')[1:]
    logging.info(f"총 {len(rows)}개의 차량 행을 찾았습니다.")
    
    for row_index, row in enumerate(rows):
        cells = row.find_all('td')
        if len(cells) >= 8:
            # 이미지 및 상세 페이지 URL 추출
            img_tag = cells[0].find('img')
            img_url = img_tag['src'] if img_tag else ''
            
            # 상세 페이지 URL 추출
            detail_link = cells[0].find('a')
            # URL 도메인 수정 (www 포함)
            detail_page = f"https://www.carku.kr{detail_link['href']}" if detail_link else ''
            logging.info(f"[차량 {row_index+1}] 상세 페이지 URL: {detail_page}")
            
            # 나머지 데이터 추출
            car_info = cells[1].find('span').text.strip() if cells[1].find('span') else ''
            transmission = cells[2].text.strip()
            year = cells[3].text.strip()
            fuel = cells[4].text.strip()
            mileage = cells[5].text.strip()
            price = cells[6].text.strip()
            contact_info = cells[7].get_text(separator=' ').strip()
            
            logging.info(f"[차량 {row_index+1}] 기본 정보: {car_info}, 가격: {price}, 연식: {year}")
            
            # 상세 페이지 데이터 초기화
            all_images = []
            sale_price = ''
            car_number = ''
            year_model = ''
            registration_date = ''
            fuel_type = ''
            transmission_type = ''
            color = ''
            detailed_mileage = ''
            vin = ''
            performance_number = ''
            seizure_info = ''
            accident_info = ''
            tax_unpaid = ''
            reference_number = ''
            association_info = ''
            seller_name = ''
            seller_contact = ''
            seller_company = ''
            seller_license = ''
            seller_address = ''
            seller_img_url = ''
            
            # 상세 페이지에서 추가 정보 가져오기
            if detail_page:
                try:
                    logging.info(f"[차량 {row_index+1}] 상세 페이지 크롤링 시작: {detail_page}")
                    
                    # 재시도 로직 추가
                    retry_count = 0
                    max_retries = 3
                    detail_response = None
                    
                    while retry_count < max_retries:
                        try:
                            # 랜덤 User-Agent 사용
                            headers = {
                                'User-Agent': get_random_user_agent(),
                                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                                'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
                                'Referer': 'https://www.carku.kr/',
                                'Connection': 'keep-alive',
                            }
                            
                            detail_response = requests.get(detail_page, headers=headers, timeout=30)
                            if detail_response.status_code == 200:
                                break
                            
                            logging.warning(f"[차량 {row_index+1}] 상세 페이지 응답 코드: {detail_response.status_code}, 재시도 {retry_count+1}/{max_retries}")
                            retry_count += 1
                            time.sleep(3)
                        except Exception as e:
                            logging.warning(f"[차량 {row_index+1}] 상세 페이지 요청 중 오류, 재시도 {retry_count+1}/{max_retries}: {str(e)}")
                            retry_count += 1
                            time.sleep(3)
                    
                    if not detail_response or detail_response.status_code != 200:
                        logging.error(f"[차량 {row_index+1}] 상세 페이지 요청 실패, 기본 정보만 저장합니다.")
                    else:
                        # 응답 내용 로깅 (디버깅용)
                        logging.debug(f"[차량 {row_index+1}] 상세 페이지 응답 길이: {len(detail_response.text)} 바이트")
                        
                        detail_soup = BeautifulSoup(detail_response.text, 'html.parser')
                        
                        # 상세 페이지 상단 정보 추출 (선택자 수정)
                        detail_top = detail_soup.select_one('div.detail-top')
                        
                        if detail_top:
                            logging.info(f"[차량 {row_index+1}] 상세 페이지 상단 정보 추출 성공")
                            
                            # 모든 이미지 URL 추출 - 수정된 방식
                            image_elements = detail_top.select('div.s_img li img')
                            logging.info(f"[차량 {row_index+1}] 이미지 요소 {len(image_elements)}개 발견")
                            
                            for img_index, img in enumerate(image_elements):
                                if 'onclick' in img.attrs:
                                    onclick_value = img['onclick']
                                    # 큰 이미지 URL 추출 방식 수정
                                    try:
                                        # 'imageShowLarge('URL')' 형식에서 URL 추출
                                        large_img_url = onclick_value.split("'")[1] if "'" in onclick_value else ''
                                        if large_img_url:
                                            all_images.append(large_img_url)
                                            logging.debug(f"[차량 {row_index+1}] 이미지 {img_index+1}: {large_img_url}")
                                    except Exception as e:
                                        logging.warning(f"[차량 {row_index+1}] 이미지 URL 추출 중 오류: {str(e)}")
                            
                            logging.info(f"[차량 {row_index+1}] 총 {len(all_images)}개의 이미지 URL 추출 완료")
                            
                            # 판매가 및 차량번호 추출 - 수정된 선택자
                            try:
                                detail_text = detail_top.select_one('div.detail-text')
                                if detail_text:
                                    # 판매가 추출
                                    price_element = detail_text.select_one('table.detail1 th:nth-of-type(1) span.red')
                                    if price_element:
                                        sale_price = price_element.text.strip()
                                    
                                    # 차량번호 추출
                                    car_number_element = detail_text.select_one('table.detail1 th:nth-of-type(2) span.red')
                                    if car_number_element:
                                        car_number = car_number_element.text.strip()
                                    
                                    logging.info(f"[차량 {row_index+1}] 판매가: {sale_price}, 차량번호: {car_number}")
                                else:
                                    logging.warning(f"[차량 {row_index+1}] detail-text 요소를 찾을 수 없습니다.")
                            except Exception as e:
                                logging.error(f"[차량 {row_index+1}] 판매가/차량번호 추출 중 오류: {str(e)}")
                            
                            # 차량 상세 정보 추출 (.detail2 테이블) - 수정된 방식
                            try:
                                detail_rows = detail_top.select('table.detail2 tr')
                                logging.info(f"[차량 {row_index+1}] 상세 정보 행 {len(detail_rows)}개 발견")
                                
                                for detail_row_index, row in enumerate(detail_rows):
                                    ths = row.find_all('th')
                                    tds = row.find_all('td')
                                    
                                    # 각 행의 첫 번째 th와 td 쌍 처리
                                    if len(ths) > 0 and len(tds) > 0:
                                        key = ths[0].text.strip()
                                        value = tds[0].text.strip()
                                        logging.debug(f"[차량 {row_index+1}] 상세 정보: {key} = {value}")
                                        
                                        # 각 항목별로 변수에 할당
                                        if '년 형 | 등록' in key:
                                            parts = value.split('|')
                                            if len(parts) >= 1:
                                                year_model = parts[0].strip()
                                            if len(parts) >= 2:
                                                registration_date = parts[1].strip()
                                        elif '연료' in key:
                                            fuel_type = value
                                        elif '색상' in key:
                                            color = value
                                        elif '차대번호' in key:
                                            vin = value
                                        elif '압류 | 저당' in key:
                                            seizure_info = value
                                        elif '세금미납' in key:
                                            tax_unpaid = value
                                        elif '제시번호' in key:
                                            reference_number = value
                                        elif '조합정보' in key:
                                            association_info = value
                                    
                                    # 같은 행의 두 번째 th와 td 쌍 처리 (있는 경우)
                                    if len(ths) > 1 and len(tds) > 1:
                                        key2 = ths[1].text.strip()
                                        value2 = tds[1].text.strip()
                                        logging.debug(f"[차량 {row_index+1}] 상세 정보: {key2} = {value2}")
                                        
                                        if '변속기' in key2:
                                            transmission_type = value2
                                        elif '주행거리' in key2:
                                            detailed_mileage = value2
                                        elif '성능번호' in key2:
                                            performance_number = value2
                                        elif '사고정보' in key2:
                                            accident_info = value2
                                
                                logging.info(f"[차량 {row_index+1}] 차량 상세 정보 추출 완료: 연식: {year_model}, 등록일: {registration_date}, 연료: {fuel_type}")
                            except Exception as e:
                                logging.error(f"[차량 {row_index+1}] 상세 정보 추출 중 오류: {str(e)}")
                            
                            # 판매자 정보 추출 (.detail3 테이블) - 수정된 방식
                            try:
                                seller_table = detail_top.select_one('table.detail3')
                                if seller_table:
                                    # 판매자 이미지 추출
                                    seller_img = seller_table.select_one('img')
                                    if seller_img and 'src' in seller_img.attrs:
                                        seller_img_url = seller_img['src']
                                        logging.debug(f"[차량 {row_index+1}] 판매자 이미지 URL: {seller_img_url}")
                                    
                                    # 판매자 정보 추출
                                    seller_rows = seller_table.select('tr')
                                    logging.info(f"[차량 {row_index+1}] 판매자 정보 행 {len(seller_rows)}개 발견")
                                    
                                    for seller_row_index, row in enumerate(seller_rows):
                                        # 첫 번째 행: 판매자 이름
                                        if seller_row_index == 0:
                                            name_cell = row.select_one('td')
                                            if name_cell:
                                                seller_name = name_cell.text.strip()
                                                logging.debug(f"[차량 {row_index+1}] 판매자 이름: {seller_name}")
                                        
                                        # 두 번째 행: 연락처
                                        elif seller_row_index == 1:
                                            contact_cell = row.select_one('td')
                                            if contact_cell:
                                                seller_contact = contact_cell.text.strip()
                                                logging.debug(f"[차량 {row_index+1}] 판매자 연락처: {seller_contact}")
                                        
                                        # 세 번째 행: 상사
                                        elif seller_row_index == 2:
                                            company_cell = row.select_one('td')
                                            if company_cell:
                                                seller_company = company_cell.text.strip()
                                                logging.debug(f"[차량 {row_index+1}] 판매자 회사: {seller_company}")
                                        
                                        # 네 번째 행: 사원증번호
                                        elif seller_row_index == 3:
                                            license_cell = row.select_one('td')
                                            if license_cell:
                                                seller_license = license_cell.text.strip()
                                                logging.debug(f"[차량 {row_index+1}] 판매자 사원증번호: {seller_license}")
                                        
                                        # 다섯 번째 행: 주소
                                        elif seller_row_index == 4:
                                            address_cell = row.select_one('th[colspan="3"]')
                                            if address_cell:
                                                seller_address = address_cell.text.strip()
                                                logging.debug(f"[차량 {row_index+1}] 판매자 주소: {seller_address}")
                                    
                                    logging.info(f"[차량 {row_index+1}] 판매자 정보 추출 완료: 이름: {seller_name}, 연락처: {seller_contact}")
                                else:
                                    logging.warning(f"[차량 {row_index+1}] 판매자 정보 테이블을 찾을 수 없습니다.")
                            except Exception as e:
                                logging.error(f"[차량 {row_index+1}] 판매자 정보 추출 중 오류: {str(e)}")
                        else:
                            logging.warning(f"[차량 {row_index+1}] 상세 페이지 상단 정보를 찾을 수 없습니다.")
                            # HTML 구조 디버깅을 위한 로깅
                            body_content = detail_soup.select('body > *')
                            logging.debug(f"[차량 {row_index+1}] 페이지 최상위 요소: {[tag.name for tag in body_content[:5]]}")
                        
                except Exception as e:
                    logging.error(f"[차량 {row_index+1}] 상세 페이지 크롤링 중 오류 발생: {str(e)}")
                    # 스택 트레이스 로깅
                    import traceback
                    logging.error(traceback.format_exc())
            
            car_dict = {
                'image_url': img_url,
                'detail_page': detail_page,
                'car_info': car_info,
                'transmission': transmission,
                'year': year,
                'fuel': fuel,
                'mileage': mileage,
                'price': price,
                'contact_info': contact_info,
                # 상세 페이지에서 추출한 데이터를 개별 필드로 추가
                'all_images': all_images,
                'sale_price': sale_price,
                'car_number': car_number,
                'year_model': year_model,
                'registration_date': registration_date,
                'fuel_type': fuel_type,
                'transmission_type': transmission_type,
                'color': color,
                'detailed_mileage': detailed_mileage,
                'vin': vin,
                'performance_number': performance_number,
                'seizure_info': seizure_info,
                'accident_info': accident_info,
                'tax_unpaid': tax_unpaid,
                'reference_number': reference_number,
                'association_info': association_info,
                'seller_name': seller_name,
                'seller_contact': seller_contact,
                'seller_company': seller_company,
                'seller_license': seller_license,
                'seller_address': seller_address,
                'seller_img_url': seller_img_url,
                'timestamp': datetime.now().isoformat()
            }
            
            # 데이터 검증
            empty_fields = [field for field, value in car_dict.items() 
                           if value == '' and field not in ['all_images', 'timestamp', 'performance_number', 'accident_info'] 
                           and not isinstance(value, list)]
            if empty_fields:
                logging.warning(f"[차량 {row_index+1}] 다음 필드가 비어 있습니다: {', '.join(empty_fields)}")
            
            logging.info(f"[차량 {row_index+1}] 데이터 추출 완료")
            car_data.append(car_dict)
        else:
            logging.warning(f"행 {row_index+1}에 충분한 셀이 없습니다. 셀 수: {len(cells)}")
    
    logging.info(f"총 {len(car_data)}개의 차량 데이터 추출 완료")
    return car_data

def get_random_user_agent():
    try:
        ua = UserAgent()
        return ua.random
    except:
        # 기본 User-Agent 목록
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36'
        ]
        return random.choice(user_agents)

def create_session_with_retry():
    session = requests.Session()
    
    # 재시도 전략 설정
    retry_strategy = Retry(
        total=5,  # 최대 재시도 횟수
        backoff_factor=1,  # 재시도 간격 (1, 2, 4, 8, 16초...)
        status_forcelist=[429, 500, 502, 503, 504],  # 재시도할 HTTP 상태 코드
        allowed_methods=["GET"]  # GET 요청만 재시도
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_random_delay():
    # 5~15초 사이의 랜덤한 지연 시간
    return random.uniform(5, 15)

def scrape_page(session, url, client):
    try:
        # 랜덤 User-Agent 사용
        headers = {
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Referer': 'https://www.carku.kr/',
            'DNT': '1',  # Do Not Track
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
        }
        
        # 요청 전 랜덤 지연
        delay = get_random_delay()
        logging.info(f"요청 전 {delay:.2f}초 대기 중...")
        time.sleep(delay)
        
        logging.info(f"URL 요청 시작: {url}")
        response = session.get(url, headers=headers, timeout=30)
        
        # 응답 상태 코드 확인
        if response.status_code != 200:
            logging.warning(f"응답 상태 코드 {response.status_code} 받음. URL: {url}")
            if response.status_code == 429:  # Too Many Requests
                logging.warning("속도 제한 감지. 5분 대기 중...")
                time.sleep(300)  # 5분 대기
                return None, 0
            return None, 0
            
        html = response.text
        logging.info(f"HTML 응답 수신 완료. 길이: {len(html)} 바이트")
        
        soup = BeautifulSoup(html, 'html.parser')
        
        if "데이터가 없습니다" in soup.text:
            logging.warning("페이지에 '데이터가 없습니다' 메시지가 포함되어 있습니다.")
            return None, 0
        elif "존재하지 않는 페이지" in soup.text:
            logging.warning("페이지에 '존재하지 않는 페이지' 메시지가 포함되어 있습니다.")
            return None, 0
        
        logging.info("차량 데이터 추출 시작...")
        car_data = scrape_car_data_from_page(soup)
        
        if not car_data:
            logging.warning("추출된 차량 데이터가 없습니다.")
            return None, 0
        
        # Index the data to OpenSearch
        logging.info(f"OpenSearch에 {len(car_data)}개의 차량 데이터 인덱싱 시작...")
        indexed_count = 0
        
        for car_index, car in enumerate(car_data):
            try:
                logging.info(f"차량 {car_index+1}/{len(car_data)} 인덱싱 중... 차량 정보: {car['car_info']}")
                
                # 인덱싱 전 데이터 검증
                if not car['car_info']:
                    logging.warning(f"차량 {car_index+1} 데이터 누락: car_info 필드가 비어 있습니다.")
                
                # 차대번호 검증 (중요 필드)
                if not car['vin']:
                    logging.warning(f"차량 {car_index+1} 데이터 누락: vin(차대번호) 필드가 비어 있습니다.")
                
                # OpenSearch에 인덱싱
                response = client.index(
                    index='carku_goods_detail',  # 인덱스 이름 수정
                    body=car,
                    refresh=True
                )
                
                if response['result'] == 'created':
                    indexed_count += 1
                    logging.info(f"차량 {car_index+1} 인덱싱 성공. ID: {response['_id']}")
                else:
                    logging.warning(f"차량 {car_index+1} 인덱싱 결과: {response['result']}")
                
                # 각 인덱싱 사이에 랜덤 지연 (0.5~1.0초)
                time.sleep(random.uniform(0.5, 1.0))
            except Exception as e:
                logging.error(f"차량 {car_index+1} 인덱싱 중 오류 발생: {str(e)}")
                continue
        
        logging.info(f"총 {indexed_count}/{len(car_data)}개의 차량 데이터 인덱싱 완료")
        return car_data, indexed_count
    except requests.exceptions.RequestException as e:
        logging.error(f"요청 오류: {str(e)}")
        # 네트워크 오류 시 더 오래 대기
        logging.info("네트워크 오류로 인해 60초 대기 중...")
        time.sleep(60)
        return None, 0
    except Exception as e:
        logging.error(f"페이지 크롤링 중 오류 발생: {str(e)}")
        return None, 0

def scrape_and_index_data(client):
    base_url = 'https://www.carku.kr/search/search.html'
    
    total_indexed = 0
    page = 1
    
    # 세션 생성
    session = create_session_with_retry()
    
    # 크롤링 시작 시간 기록
    start_time = time.time()
    
    try:
        while True:
            url = f"{base_url}?wCurPage={page}&wKmS=&wKmE=&wPageSize="
            logging.info(f"Scraping page {page}: {url}")
            
            car_data, indexed_count = scrape_page(session, url, client)
            
            if car_data is None:
                # 페이지가 없거나 오류 발생 시
                if page > 1:  # 첫 페이지가 아니면 크롤링 종료
                    break
                else:  # 첫 페이지에서 오류 발생 시 재시도
                    logging.warning("Error on first page. Retrying after 2 minutes...")
                    time.sleep(20)
                    continue
            
            total_indexed += indexed_count
            logging.info(f"Indexed {indexed_count} cars from page {page}")
            
            page += 1
            
            delay = random.uniform(60, 120)
            logging.info(f"Waiting {delay:.2f} seconds before next page...")
            time.sleep(delay)
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
    finally:
        session.close()
    
    return total_indexed

def main():
    logging.info("Starting data collection and indexing to OpenSearch...")
    
    try:
        client = create_opensearch_client()
        create_carku_index(client)
        
        total_indexed = scrape_and_index_data(client)
        
        logging.info(f"Total cars indexed: {total_indexed}")
        
        index_stats = client.indices.stats(index='carku_goods_detail')
        logging.info(f"Total documents in index: {index_stats['indices']['carku_goods_detail']['total']['docs']['count']}")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
