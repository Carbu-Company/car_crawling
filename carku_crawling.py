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
    index_name = 'carku_goods'
    
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
        return []
        
    car_data = []
    rows = table.find_all('tr')[1:]
    
    for row in rows:
        cells = row.find_all('td')
        if len(cells) >= 8:
            img_tag = cells[0].find('img')
            img_url = img_tag['src'] if img_tag else ''
            car_info = cells[1].find('span').text.strip() if cells[1].find('span') else ''
            transmission = cells[2].text.strip()
            year = cells[3].text.strip()
            fuel = cells[4].text.strip()
            mileage = cells[5].text.strip()
            price = cells[6].text.strip()
            contact_info = cells[7].get_text(separator=' ').strip()
            
            car_dict = {
                'image_url': img_url,
                'car_info': car_info,
                'transmission': transmission,
                'year': year,
                'fuel': fuel,
                'mileage': mileage,
                'price': price,
                'contact_info': contact_info,
                'timestamp': datetime.now().isoformat()
            }
            car_data.append(car_dict)
    
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
        time.sleep(get_random_delay())
        
        response = session.get(url, headers=headers, timeout=30)
        
        # 응답 상태 코드 확인
        if response.status_code != 200:
            logging.warning(f"Received status code {response.status_code} for URL: {url}")
            if response.status_code == 429:  # Too Many Requests
                logging.warning("Rate limit detected. Sleeping for 5 minutes...")
                time.sleep(300)  # 5분 대기
                return None, 0
            return None, 0
            
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        
        if "데이터가 없습니다" in soup.text or "존재하지 않는 페이지" in soup.text:
            return None, 0
        
        car_data = scrape_car_data_from_page(soup)
        if not car_data:
            return None, 0
        
        # Index the data to OpenSearch
        indexed_count = 0
        for car in car_data:
            try:
                client.index(
                    index='carku_goods',
                    body=car,
                    refresh=True
                )
                indexed_count += 1
                # 각 인덱싱 사이에 랜덤 지연 (2~5초)
                time.sleep(random.uniform(0.5, 1.0))
            except Exception as e:
                logging.error(f"Error indexing car: {str(e)}")
                continue
        
        return car_data, indexed_count
    except requests.exceptions.RequestException as e:
        logging.error(f"Request error: {str(e)}")
        # 네트워크 오류 시 더 오래 대기
        time.sleep(60)
        return None, 0
    except Exception as e:
        logging.error(f"Error scraping page: {str(e)}")
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
        
        index_stats = client.indices.stats(index='carku_goods')
        logging.info(f"Total documents in index: {index_stats['indices']['carku_goods']['total']['docs']['count']}")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main()
