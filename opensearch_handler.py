"""
Module for handling OpenSearch operations for the Encar crawler.
"""

from opensearchpy import OpenSearch, RequestsHttpConnection
from datetime import datetime
import logging
import sys
import config
from opensearchpy.exceptions import NotFoundError, RequestError

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
    """
    Create and configure an OpenSearch client.
    
    Returns:
        OpenSearch: Configured OpenSearch client
    """
    try:
        # OpenSearch 클라이언트 설정
        client = OpenSearch(
            hosts=[{'host': config.OPENSEARCH_HOST, 'port': config.OPENSEARCH_PORT}],
            http_auth=(config.OPENSEARCH_USERNAME, config.OPENSEARCH_PASSWORD),
            use_ssl=config.OPENSEARCH_USE_SSL,
            verify_certs=config.OPENSEARCH_VERIFY_CERTS,
            connection_class=RequestsHttpConnection
        )
        
        logging.info("OpenSearch 클라이언트가 성공적으로 생성되었습니다.")
        return client
    
    except Exception as e:
        logging.error(f"OpenSearch 클라이언트 생성 중 오류 발생: {e}")
        raise

def create_encar_index(client):
    """
    Create Encar index in OpenSearch
    
    Args:
        client: OpenSearch client
    """
    index_name = 'encar_cars_detail'
    
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
                        # Basic information fields
                        'car_id': {'type': 'keyword'},
                        'index': {'type': 'keyword'},
                        'manufacturer': {'type': 'keyword'},
                        'model': {'type': 'keyword'},
                        'detailed_model': {
                            'type': 'text',
                            'norms': 'false',
                            'similarity': 'scripted_no_idf'
                        },
                        'year': {'type': 'keyword'},
                        'mileage': {'type': 'keyword'},
                        'fuel_type': {'type': 'keyword'},
                        'location': {'type': 'keyword'},
                        'price': {'type': 'keyword'},
                        'price_value': {'type': 'keyword'},
                        'price_unit': {'type': 'keyword'},
                        'image_url': {'type': 'keyword'},
                        'badge': {'type': 'keyword'},
                        'performance_record': {'type': 'boolean'},
                        'encar_diagnosis': {'type': 'boolean'},
                        'ad_info': {'type': 'keyword'},
                        'detail_page_url': {'type': 'keyword'},
                        'page_number': {'type': 'integer'},
                        
                        # Detailed information fields
                        'car_number': {'type': 'keyword'},
                        'detailed_year': {'type': 'keyword'},
                        'detailed_mileage': {'type': 'keyword'},
                        'engine_displacement': {'type': 'keyword'},
                        'detailed_fuel_type': {'type': 'keyword'},
                        'transmission': {'type': 'keyword'},
                        'car_type': {'type': 'keyword'},
                        'color': {'type': 'keyword'},
                        'detailed_location': {'type': 'keyword'},
                        'seating_capacity': {'type': 'keyword'},
                        'import_type': {'type': 'keyword'},
                        'seizure_mortgage': {'type': 'keyword'},
                        'view_count': {'type': 'keyword'},
                        'favorite_count': {'type': 'keyword'},
                        
                        # Metadata
                        'crawling_time': {'type': 'date'}
                    }
                }
            }
            client.indices.create(index=index_name, body=index_body)
            logging.info(f"Created index: {index_name}")
    except Exception as e:
        logging.error(f"Error creating index: {str(e)}")
        raise

def index_car_to_opensearch(client, car_dict, car_index):
    """
    Index car data to OpenSearch
    
    Args:
        client: OpenSearch client
        car_dict: Dictionary containing car data
        car_index: Index of the car in the list
        
    Returns:
        bool: True if indexing was successful, False otherwise
    """
    try:
        # Field name mapping (Korean to English)
        field_mapping = {
            '차량ID': 'car_id',
            '인덱스': 'index',
            '제조사': 'manufacturer',
            '모델': 'model',
            '세부모델': 'detailed_model',
            '연식': 'year',
            '주행거리': 'mileage',
            '연료': 'fuel_type',
            '지역': 'location',
            '가격': 'price',
            '가격값': 'price_value',
            '가격단위': 'price_unit',
            '이미지URL': 'image_url',
            '배지': 'badge',
            '성능기록여부': 'performance_record',
            '엔카진단여부': 'encar_diagnosis',
            '광고정보': 'ad_info',
            '상세페이지URL': 'detail_page_url',
            '페이지번호': 'page_number',
            '차량번호': 'car_number',
            '상세연식': 'detailed_year',
            '상세주행거리': 'detailed_mileage',
            '배기량': 'engine_displacement',
            '상세연료': 'detailed_fuel_type',
            '변속기': 'transmission',
            '차종': 'car_type',
            '색상': 'color',
            '상세지역': 'detailed_location',
            '인승': 'seating_capacity',
            '수입구분': 'import_type',
            '압류저당': 'seizure_mortgage',
            '조회수': 'view_count',
            '찜수': 'favorite_count',
            '크롤링시간': 'crawling_time'
        }
        
        # Convert Korean field names to English
        english_car_dict = {}
        for k, v in car_dict.items():
            if k in field_mapping:
                english_car_dict[field_mapping[k]] = v
            else:
                # Keep original field name if not in mapping
                english_car_dict[k] = v
        
        # Data validation before indexing
        if not english_car_dict.get('detailed_model'):
            logging.warning(f"Car {car_index} data missing: detailed_model field is empty")
        
        # Validate car_id (important field)
        if not english_car_dict.get('car_id'):
            logging.warning(f"Car {car_index} data missing: car_id field is empty")
        
        # Convert crawling_time to date format if it's a string
        if isinstance(english_car_dict.get('crawling_time'), str):
            try:
                # Convert to ISO format
                english_car_dict['crawling_time'] = datetime.fromisoformat(english_car_dict['crawling_time']).isoformat()
            except:
                # Use current time if conversion fails
                english_car_dict['crawling_time'] = datetime.now().isoformat()
        
        # Index to OpenSearch
        response = client.index(
            index='encar_cars_detail',
            body=english_car_dict,
            refresh=True
        )
        
        if response['result'] == 'created':
            logging.info(f"Car {car_index} indexed successfully. ID: {response['_id']}")
            return True
        else:
            logging.warning(f"Car {car_index} indexing result: {response['result']}")
            return False
    except Exception as e:
        logging.error(f"Error indexing car {car_index}: {str(e)}")
        return False

def get_index_stats(client):
    """
    Get index statistics
    
    Args:
        client: OpenSearch client
        
    Returns:
        dict: Index statistics
    """
    try:
        index_stats = client.indices.stats(index='encar_cars_detail')
        doc_count = index_stats['indices']['encar_cars_detail']['total']['docs']['count']
        logging.info(f"Total documents in index: {doc_count}")
        return {
            'doc_count': doc_count,
            'index_stats': index_stats
        }
    except Exception as e:
        logging.error(f"Error retrieving index statistics: {str(e)}")
        return None 