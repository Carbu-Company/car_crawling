import aiohttp
import asyncio
from bs4 import BeautifulSoup
import time
from opensearchpy import OpenSearch
from datetime import datetime
import logging
import sys
from concurrent.futures import ThreadPoolExecutor

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

async def scrape_page(session, url, headers, client, semaphore):
    async with semaphore:
        try:
            async with session.get(url, headers=headers) as response:
                html = await response.text()
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
                    except Exception as e:
                        logging.error(f"Error indexing car: {str(e)}")
                        continue
                
                return car_data, indexed_count
        except Exception as e:
            logging.error(f"Error scraping page: {str(e)}")
            return None, 0

async def scrape_and_index_data(client):
    base_url = 'https://www.carku.kr/search/search.html'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    total_indexed = 0
    page = 1
    
    # 동시 연결 수 제한
    semaphore = asyncio.Semaphore(5)  # 동시에 5개의 요청만 처리
    
    # TCP 연결 재사용을 위한 세션 생성
    async with aiohttp.ClientSession() as session:
        while True:
            url = f"{base_url}?wCurPage={page}&wKmS=&wKmE=&wPageSize="
            logging.info(f"Scraping page {page}: {url}")
            
            car_data, indexed_count = await scrape_page(session, url, headers, client, semaphore)
            time.sleep(2)
            
            if car_data is None:
                break
                
            total_indexed += indexed_count
            logging.info(f"Indexed {indexed_count} cars from page {page}")
            
            page += 1
            await asyncio.sleep(10)  # 1초 대기
    
    return total_indexed

async def main():
    logging.info("Starting data collection and indexing to OpenSearch...")
    
    try:
        client = create_opensearch_client()
        create_carku_index(client)
        
        total_indexed = await scrape_and_index_data(client)
        
        logging.info(f"Total cars indexed: {total_indexed}")
        
        index_stats = client.indices.stats(index='carku_goods')
        logging.info(f"Total documents in index: {index_stats['indices']['carku_goods']['total']['docs']['count']}")
        
    except Exception as e:
        logging.error(f"Error: {str(e)}")

if __name__ == "__main__":
    asyncio.run(main())
