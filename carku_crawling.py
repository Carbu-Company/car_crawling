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
import cloudscraper  # 캡챠/방화벽 우회용
import os

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
    """OpenSearch 클라이언트 생성"""
    opensearch = OpenSearch(
        "http://14.6.96.11:1006",
        http_auth=("admin", "Myopensearch!1"),
        timeout=180,
        max_retries=30,
        retry_on_timeout=True
    )
    return opensearch

def create_carku_index(client):
    """카크 인덱스 생성"""
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

def get_random_user_agent():
    """랜덤 User-Agent 생성"""
    try:
        ua = UserAgent()
        return ua.random
    except:
        # 기본 User-Agent 목록
        user_agents = [
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.4472.124 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:89.0) Gecko/20100101 Firefox/89.0',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.1 Safari/605.1.15',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36 Edg/91.0.864.59',
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
            'Mozilla/5.0 (iPhone; CPU iPhone OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (iPad; CPU OS 14_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.0 Mobile/15E148 Safari/604.1',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.127 Safari/537.36'
        ]
        return random.choice(user_agents)

def create_scraper_with_retry():
    """CloudScraper 세션 생성 (방화벽 우회)"""
    try:
        scraper = cloudscraper.create_scraper(
            browser={
                'browser': 'chrome',
                'platform': 'windows',
                'desktop': True
            },
            delay=10
        )
        
        # 재시도 전략 설정
        retry_strategy = Retry(
            total=5,  # 최대 재시도 횟수
            backoff_factor=1,  # 재시도 간격 (1, 2, 4, 8, 16초...)
            status_forcelist=[429, 500, 502, 503, 504],  # 재시도할 HTTP 상태 코드
            allowed_methods=["GET"]  # GET 요청만 재시도
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        scraper.mount("http://", adapter)
        scraper.mount("https://", adapter)
        
        # 쿠키 및 헤더 초기화
        scraper.headers.update({
            'User-Agent': get_random_user_agent(),
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://www.carku.kr/',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0',
            'sec-ch-ua': '"Chromium";v="110", "Not A(Brand";v="24", "Google Chrome";v="110"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"'
        })
        
        # 초기 메인 페이지 방문하여 쿠키 획득
        try:
            logging.info("메인 페이지 방문하여 쿠키 획득 중...")
            scraper.get('https://www.carku.kr/', timeout=30)
            time.sleep(2)  # 일부러 지연
        except Exception as e:
            logging.warning(f"초기 쿠키 획득 중 오류: {str(e)}")
        
        return scraper
    except Exception as e:
        logging.error(f"Scraper 생성 중 오류: {str(e)}")
        # 기본 세션으로 대체
        session = requests.Session()
        session.headers.update({'User-Agent': get_random_user_agent()})
        return session

def get_random_delay(min_sec=5, max_sec=15):
    """랜덤 지연 시간 생성"""
    return random.uniform(min_sec, max_sec)

def validate_html_content(html):
    """HTML 콘텐츠 유효성 검사"""
    if not html or len(html) < 500:  # 너무 짧은 응답은 의심스러움
        return False
    
    # 봇 차단 감지
    suspicious_phrases = [
        "captcha", "robot", "blocked", "access denied", "too many requests",
        "rate limit", "비정상적인", "차단", "캡챠", "로봇"
    ]
    
    lower_html = html.lower()
    for phrase in suspicious_phrases:
        if phrase in lower_html:
            logging.warning(f"의심스러운 표현 감지: '{phrase}'")
            return False
    
    return True

def save_error_response(html, page_num):
    """디버깅을 위해 오류 응답 저장"""
    error_dir = "error_responses"
    if not os.path.exists(error_dir):
        os.makedirs(error_dir)
    
    filename = f"{error_dir}/error_page_{page_num}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
    with open(filename, 'w', encoding='utf-8') as f:
        f.write(html)
    
    logging.info(f"오류 응답을 파일에 저장했습니다: {filename}")

def scrape_car_data_from_page(soup):
    """페이지에서 차량 데이터 추출"""
    table = soup.find('table', class_='one_list')
    if not table:
        logging.warning("테이블을 찾을 수 없습니다.")
        # 디버깅을 위한 HTML 구조 검사
        body_content = soup.select('body > *')
        table_elements = soup.find_all('table')
        logging.debug(f"페이지 최상위 요소: {[tag.name for tag in body_content[:5]]}")
        logging.debug(f"발견된 테이블 수: {len(table_elements)}")
        logging.debug(f"테이블 클래스들: {[t.get('class', []) for t in table_elements]}")
        return []
        
    car_data = []
    rows = table.find_all('tr')[1:]  # 헤더 행 제외
    logging.info(f"총 {len(rows)}개의 차량 행을 찾았습니다.")
    
    for row_index, row in enumerate(rows):
        try:
            cells = row.find_all('td')
            if len(cells) < 8:
                logging.warning(f"행 {row_index+1}에 충분한 셀이 없습니다. 셀 수: {len(cells)}")
                continue
            
            # 이미지 및 상세 페이지 URL 추출
            img_tag = cells[0].find('img')
            img_url = img_tag['src'] if img_tag and 'src' in img_tag.attrs else ''
            
            # 상세 페이지 URL 추출
            detail_link = cells[0].find('a')
            # URL 도메인 수정 (www 포함)
            detail_page = f"https://www.carku.kr{detail_link['href']}" if detail_link and 'href' in detail_link.attrs else ''
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
            
            # 기초 데이터 사전 생성
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
                # 상세 페이지 필드는 추후 업데이트됨
                'all_images': [],
                'sale_price': '',
                'car_number': '',
                'year_model': '',
                'registration_date': '',
                'fuel_type': '',
                'transmission_type': '',
                'color': '',
                'detailed_mileage': '',
                'vin': '',
                'performance_number': '',
                'seizure_info': '',
                'accident_info': '',
                'tax_unpaid': '',
                'reference_number': '',
                'association_info': '',
                'seller_name': '',
                'seller_contact': '',
                'seller_company': '',
                'seller_license': '',
                'seller_address': '',
                'seller_img_url': '',
                'timestamp': datetime.now().isoformat()
            }
            
            car_data.append(car_dict)
        except Exception as e:
            logging.error(f"행 {row_index+1} 처리 중 오류: {str(e)}")
            continue
    
    logging.info(f"총 {len(car_data)}개의 차량 기본 데이터 추출 완료")
    return car_data

def fetch_detail_page(scraper, detail_page, car_index, max_retries=3):
    """상세 페이지 가져오기"""
    retry_count = 0
    
    while retry_count < max_retries:
        try:
            # 새로운 User-Agent 사용
            scraper.headers.update({'User-Agent': get_random_user_agent()})
            
            # 상세 페이지 요청 시간 측정
            start_time = time.time()
            logging.info(f"[차량 {car_index}] 상세 페이지 요청 시작: {detail_page}")
            
            response = scraper.get(detail_page, timeout=30)
            
            elapsed = time.time() - start_time
            logging.info(f"[차량 {car_index}] 상세 페이지 응답 수신 완료. 소요 시간: {elapsed:.2f}초, 상태 코드: {response.status_code}")
            
            if response.status_code != 200:
                logging.warning(f"[차량 {car_index}] 상세 페이지 응답 코드: {response.status_code}")
                retry_count += 1
                time.sleep(5)
                continue
            
            html = response.text
            
            # 응답 내용 유효성 검사
            if not validate_html_content(html):
                logging.warning(f"[차량 {car_index}] 상세 페이지 응답이 유효하지 않습니다. 길이: {len(html)} 바이트")
                # 오류 응답 저장
                save_error_response(html, f"detail_{car_index}")
                retry_count += 1
                time.sleep(10)
                continue
            
            logging.info(f"[차량 {car_index}] 상세 페이지 내용 획득 성공. 길이: {len(html)} 바이트")
            return html
            
        except Exception as e:
            logging.error(f"[차량 {car_index}] 상세 페이지 요청 중 오류: {str(e)}")
            retry_count += 1
            time.sleep(5)
    
    logging.error(f"[차량 {car_index}] 상세 페이지 가져오기 최대 재시도 횟수 초과")
    return None

def extract_detail_page_data(car_dict, html, car_index):
    """상세 페이지에서 추가 데이터 추출"""
    if not html:
        return car_dict
    
    try:
        detail_soup = BeautifulSoup(html, 'html.parser')
        
        # 상세 페이지 상단 정보 추출
        detail_top = detail_soup.select_one('div.detail-top')
        
        if not detail_top:
            logging.warning(f"[차량 {car_index}] 상세 페이지 상단 정보를 찾을 수 없습니다.")
            # HTML 구조 디버깅
            body_content = detail_soup.select('body > *')
            logging.debug(f"[차량 {car_index}] 페이지 최상위 요소: {[tag.name for tag in body_content[:5]]}")
            return car_dict
        
        logging.info(f"[차량 {car_index}] 상세 페이지 상단 정보 추출 시작")
        
        # 모든 이미지 URL 추출
        image_elements = detail_top.select('div.s_img li img')
        logging.info(f"[차량 {car_index}] 이미지 요소 {len(image_elements)}개 발견")
        
        all_images = []
        for img_index, img in enumerate(image_elements):
            if 'onclick' in img.attrs:
                onclick_value = img['onclick']
                try:
                    # 'imageShowLarge('URL')' 형식에서 URL 추출
                    large_img_url = onclick_value.split("'")[1] if "'" in onclick_value else ''
                    if large_img_url:
                        all_images.append(large_img_url)
                        logging.debug(f"[차량 {car_index}] 이미지 {img_index+1}: {large_img_url}")
                except Exception as e:
                    logging.warning(f"[차량 {car_index}] 이미지 URL 추출 중 오류: {str(e)}")
        
        car_dict['all_images'] = all_images
        logging.info(f"[차량 {car_index}] 총 {len(all_images)}개의 이미지 URL 추출 완료")
        
        # 판매가 및 차량번호 추출
        try:
            detail_text = detail_top.select_one('div.detail-text')
            if detail_text:
                # 판매가 추출
                price_element = detail_text.select_one('table.detail1 th:nth-of-type(1) span.red')
                if price_element:
                    car_dict['sale_price'] = price_element.text.strip()
                
                # 차량번호 추출
                car_number_element = detail_text.select_one('table.detail1 th:nth-of-type(2) span.red')
                if car_number_element:
                    car_dict['car_number'] = car_number_element.text.strip()
                
                logging.info(f"[차량 {car_index}] 판매가: {car_dict['sale_price']}, 차량번호: {car_dict['car_number']}")
            else:
                logging.warning(f"[차량 {car_index}] detail-text 요소를 찾을 수 없습니다.")
        except Exception as e:
            logging.error(f"[차량 {car_index}] 판매가/차량번호 추출 중 오류: {str(e)}")
        
        # 차량 상세 정보 추출 (.detail2 테이블)
        try:
            detail_rows = detail_top.select('table.detail2 tr')
            logging.info(f"[차량 {car_index}] 상세 정보 행 {len(detail_rows)}개 발견")
            
            for detail_row_index, row in enumerate(detail_rows):
                ths = row.find_all('th')
                tds = row.find_all('td')
                
                # 각 행의 첫 번째 th와 td 쌍 처리
                if len(ths) > 0 and len(tds) > 0:
                    key = ths[0].text.strip()
                    value = tds[0].text.strip()
                    logging.debug(f"[차량 {car_index}] 상세 정보: {key} = {value}")
                    
                    # 각 항목별로 변수에 할당
                    if '년 형 | 등록' in key:
                        parts = value.split('|')
                        if len(parts) >= 1:
                            car_dict['year_model'] = parts[0].strip()
                        if len(parts) >= 2:
                            car_dict['registration_date'] = parts[1].strip()
                    elif '연료' in key:
                        car_dict['fuel_type'] = value
                    elif '색상' in key:
                        car_dict['color'] = value
                    elif '차대번호' in key:
                        car_dict['vin'] = value
                    elif '압류 | 저당' in key:
                        car_dict['seizure_info'] = value
                    elif '세금미납' in key:
                        car_dict['tax_unpaid'] = value
                    elif '제시번호' in key:
                        car_dict['reference_number'] = value
                    elif '조합정보' in key:
                        car_dict['association_info'] = value
                
                # 같은 행의 두 번째 th와 td 쌍 처리 (있는 경우)
                if len(ths) > 1 and len(tds) > 1:
                    key2 = ths[1].text.strip()
                    value2 = tds[1].text.strip()
                    logging.debug(f"[차량 {car_index}] 상세 정보: {key2} = {value2}")
                    
                    if '변속기' in key2:
                        car_dict['transmission_type'] = value2
                    elif '주행거리' in key2:
                        car_dict['detailed_mileage'] = value2
                    elif '성능번호' in key2:
                        car_dict['performance_number'] = value2
                    elif '사고정보' in key2:
                        car_dict['accident_info'] = value2
            
            logging.info(f"[차량 {car_index}] 차량 상세 정보 추출 완료: 연식: {car_dict['year_model']}, 등록일: {car_dict['registration_date']}")
        except Exception as e:
            logging.error(f"[차량 {car_index}] 상세 정보 추출 중 오류: {str(e)}")
        
        # 판매자 정보 추출 (.detail3 테이블)
        try:
            seller_table = detail_top.select_one('table.detail3')
            if seller_table:
                # 판매자 이미지 추출
                seller_img = seller_table.select_one('img')
                if seller_img and 'src' in seller_img.attrs:
                    car_dict['seller_img_url'] = seller_img['src']
                
                # 판매자 정보 추출
                seller_rows = seller_table.select('tr')
                logging.info(f"[차량 {car_index}] 판매자 정보 행 {len(seller_rows)}개 발견")
                
                for seller_row_index, row in enumerate(seller_rows):
                    # 첫 번째 행: 판매자 이름
                    if seller_row_index == 0:
                        name_cell = row.select_one('td')
                        if name_cell:
                            car_dict['seller_name'] = name_cell.text.strip()
                    
                    # 두 번째 행: 연락처
                    elif seller_row_index == 1:
                        contact_cell = row.select_one('td')
                        if contact_cell:
                            car_dict['seller_contact'] = contact_cell.text.strip()
                    
                    # 세 번째 행: 상사
                    elif seller_row_index == 2:
                        company_cell = row.select_one('td')
                        if company_cell:
                            car_dict['seller_company'] = company_cell.text.strip()
                    
                    # 네 번째 행: 사원증번호
                    elif seller_row_index == 3:
                        license_cell = row.select_one('td')
                        if license_cell:
                            car_dict['seller_license'] = license_cell.text.strip()
                    
                    # 다섯 번째 행: 주소
                    elif seller_row_index == 4:
                        address_cell = row.select_one('th[colspan="3"]')
                        if address_cell:
                            car_dict['seller_address'] = address_cell.text.strip()
                
                logging.info(f"[차량 {car_index}] 판매자 정보 추출 완료: 이름: {car_dict['seller_name']}, 연락처: {car_dict['seller_contact']}")
            else:
                logging.warning(f"[차량 {car_index}] 판매자 정보 테이블을 찾을 수 없습니다.")
        except Exception as e:
            logging.error(f"[차량 {car_index}] 판매자 정보 추출 중 오류: {str(e)}")
        
        return car_dict
    except Exception as e:
        logging.error(f"[차량 {car_index}] 상세 페이지 데이터 추출 중 오류: {str(e)}")
        return car_dict

def index_car_to_opensearch(client, car_dict, car_index):
    """OpenSearch에 차량 데이터 인덱싱"""
    try:
        # 인덱싱 전 데이터 검증
        if not car_dict['car_info']:
            logging.warning(f"차량 {car_index} 데이터 누락: car_info 필드가 비어 있습니다.")
        
        # 차대번호 검증 (중요 필드)
        if not car_dict['vin']:
            logging.warning(f"차량 {car_index} 데이터 누락: vin(차대번호) 필드가 비어 있습니다.")
        
        # OpenSearch에 인덱싱
        response = client.index(
            index='carku_goods_detail',
            body=car_dict,
            refresh=True
        )
        
        if response['result'] == 'created':
            logging.info(f"차량 {car_index} 인덱싱 성공. ID: {response['_id']}")
            return True
        else:
            logging.warning(f"차량 {car_index} 인덱싱 결과: {response['result']}")
            return False
    except Exception as e:
        logging.error(f"차량 {car_index} 인덱싱 중 오류 발생: {str(e)}")
        return False

def handle_carku_cloudflare(scraper, url):
    """Cloudflare 방화벽 처리"""
    try:
        # User-Agent를 데스크톱으로 변경
        desktop_ua = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/110.0.0.0 Safari/537.36'
        scraper.headers.update({'User-Agent': desktop_ua})
        
        # 쿠키 수집을 위해 메인 페이지 방문
        logging.info("Cloudflare 방화벽 우회를 위해 메인 페이지 방문...")
        scraper.get('https://www.carku.kr/', timeout=30)
        time.sleep(3)  # 쿠키 설정 대기
        
        # 실제 요청
        logging.info(f"Cloudflare 처리 후 페이지 요청: {url}")
        response = scraper.get(url, timeout=30)
        
        if response.status_code == 200:
            return response.text
        else:
            logging.warning(f"Cloudflare 우회 후에도 응답 코드: {response.status_code}")
            return None
    except Exception as e:
        logging.error(f"Cloudflare 우회 중 오류: {str(e)}")
        return None

def scrape_page(scraper, url, client):
    """페이지 스크랩 및 데이터 인덱싱"""
    try:
        # 랜덤 지연
        delay = get_random_delay(8, 20)
        logging.info(f"요청 전 {delay:.2f}초 대기 중...")
        time.sleep(delay)
        
        # 요청 전 User-Agent 변경
        scraper.headers.update({'User-Agent': get_random_user_agent()})
        
        logging.info(f"URL 요청 시작: {url}")
        start_time = time.time()
        
        # 일반 요청 시도
        response = scraper.get(url, timeout=30)
        
        elapsed = time.time() - start_time
        logging.info(f"응답 수신 완료. 소요 시간: {elapsed:.2f}초, 상태 코드: {response.status_code}")
        
        if response.status_code != 200:
            logging.warning(f"응답 상태 코드 {response.status_code} 받음. URL: {url}")
            if response.status_code == 429:  # Too Many Requests
                logging.warning("속도 제한 감지. 5분 대기 중...")
                time.sleep(300)  # 5분 대기
            return None, 0
        
        html = response.text
        logging.info(f"HTML 응답 수신 완료. 길이: {len(html)} 바이트")
        
        # 응답 내용 유효성 검사
        if not validate_html_content(html):
            logging.warning("응답이 유효하지 않습니다. Cloudflare 방화벽 우회 시도...")
            # Cloudflare 방화벽 우회 시도
            html = handle_carku_cloudflare(scraper, url)
            if not html:
                logging.error("Cloudflare 방화벽 우회 실패")
                # 오류 응답 저장
                save_error_response(response.text, url.split('wCurPage=')[1].split('&')[0])
                return None, 0
        
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
        
        # 상세 페이지 데이터 수집 및 OpenSearch 인덱싱
        logging.info(f"{len(car_data)}개의 차량에 대한 상세 정보 수집 및 인덱싱 시작...")
        indexed_count = 0
        
        for car_index, car_dict in enumerate(car_data):
            try:
                # 상세 페이지 URL 확인
                detail_page = car_dict.get('detail_page', '')
                if not detail_page:
                    logging.warning(f"차량 {car_index+1}/{len(car_data)}: 상세 페이지 URL이 없습니다.")
                    continue
                
                # 상세 페이지 데이터 가져오기
                detail_html = fetch_detail_page(scraper, detail_page, car_index+1)
                
                # 상세 페이지 데이터 추출
                if detail_html:
                    # 상세 데이터 추출
                    updated_car_dict = extract_detail_page_data(car_dict, detail_html, car_index+1)
                    
                    # OpenSearch에 인덱싱
                    if index_car_to_opensearch(client, updated_car_dict, car_index+1):
                        indexed_count += 1
                else:
                    logging.warning(f"차량 {car_index+1}/{len(car_data)}: 상세 페이지 HTML을 가져오지 못했습니다.")
                    # 기본 정보만으로 인덱싱 시도
                    if index_car_to_opensearch(client, car_dict, car_index+1):
                        indexed_count += 1
                
                # 상세 페이지 요청 사이에 랜덤 지연
                if car_index < len(car_data) - 1:  # 마지막 항목이 아니면
                    detail_delay = get_random_delay(3, 8)
                    logging.info(f"다음 상세 페이지 요청 전 {detail_delay:.2f}초 대기 중...")
                    time.sleep(detail_delay)
            
            except Exception as e:
                logging.error(f"차량 {car_index+1} 처리 중 오류 발생: {str(e)}")
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
        # 스택 트레이스 로깅
        import traceback
        logging.error(traceback.format_exc())
        return None, 0

def scrape_and_index_data(client):
    """모든 페이지 스크랩 및 인덱싱"""
    base_url = 'https://www.carku.kr/search/search.html'
    
    total_indexed = 0
    page = 1
    
    # 클라우드스크래퍼 세션 생성 (방화벽 우회)
    scraper = create_scraper_with_retry()
    
    # 크롤링 시작 시간 기록
    start_time = time.time()
    
    try:
        while True:
            url = f"{base_url}?wCurPage={page}&wKmS=&wKmE=&wPageSize="
            logging.info(f"Scraping page {page}: {url}")
            
            car_data, indexed_count = scrape_page(scraper, url, client)
            
            if car_data is None:
                # 페이지가 없거나 오류 발생 시
                if page > 1:  # 첫 페이지가 아니면 크롤링 종료
                    logging.info(f"페이지 {page}에서 데이터를 찾을 수 없습니다. 크롤링을 종료합니다.")
                    break
                else:  # 첫 페이지에서 오류 발생 시 재시도
                    logging.warning("Error on first page. Retrying after 2 minutes...")
                    time.sleep(120)
                    # 세션 재생성
                    scraper = create_scraper_with_retry()
                    continue
            
            total_indexed += indexed_count
            logging.info(f"Indexed {indexed_count} cars from page {page}")
            
            # 다음 페이지 요청 전 긴 지연 (봇 감지 방지)
            page_delay = random.uniform(60, 120)
            logging.info(f"Waiting {page_delay:.2f} seconds before next page...")
            time.sleep(page_delay)
            
            page += 1
            
            # 과도한 크롤링 방지를 위한 안전장치
            if page > 100:  # 최대 100페이지로 제한
                logging.info("최대 페이지 수(100)에 도달했습니다. 크롤링을 종료합니다.")
                break
            
            # 크롤링 최대 시간 제한 (6시간)
            if time.time() - start_time > 6 * 60 * 60:
                logging.info("최대 크롤링 시간(6시간)에 도달했습니다. 크롤링을 종료합니다.")
                break
    
    except KeyboardInterrupt:
        logging.info("Crawling interrupted by user.")
    finally:
        try:
            scraper.close()
        except:
            pass
    
    return total_indexed

def main():
    """메인 함수"""
    logging.info("Starting data collection and indexing to OpenSearch...")
    
    try:
        # 오류 응답 디렉토리 생성
        error_dir = "error_responses"
        if not os.path.exists(error_dir):
            os.makedirs(error_dir)
        
        # OpenSearch 클라이언트 생성 및 인덱스 설정
        client = create_opensearch_client()
        create_carku_index(client)
        
        # 데이터 수집 및 인덱싱
        total_indexed = scrape_and_index_data(client)
        
        logging.info(f"Total cars indexed: {total_indexed}")
        
        # 최종 인덱스 통계
        try:
            index_stats = client.indices.stats(index='carku_goods_detail')
            logging.info(f"Total documents in index: {index_stats['indices']['carku_goods_detail']['total']['docs']['count']}")
        except Exception as e:
            logging.error(f"인덱스 통계 조회 중 오류: {str(e)}")
    
    except Exception as e:
        logging.error(f"Error: {str(e)}")
        # 스택 트레이스 로깅
        import traceback
        logging.error(traceback.format_exc())

if __name__ == "__main__":
    main()