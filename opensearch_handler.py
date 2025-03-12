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
    엔카 인덱스 생성
    
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
                        # 기본 정보 필드
                        '차량ID': {'type': 'keyword'},
                        '인덱스': {'type': 'keyword'},
                        '제조사': {'type': 'keyword'},
                        '모델': {'type': 'keyword'},
                        '세부모델': {
                            'type': 'text',
                            'norms': 'false',
                            'similarity': 'scripted_no_idf'
                        },
                        '연식': {'type': 'keyword'},
                        '주행거리': {'type': 'keyword'},
                        '연료': {'type': 'keyword'},
                        '지역': {'type': 'keyword'},
                        '가격': {'type': 'keyword'},
                        '가격값': {'type': 'keyword'},
                        '가격단위': {'type': 'keyword'},
                        '이미지URL': {'type': 'keyword'},
                        '배지': {'type': 'keyword'},
                        '성능기록여부': {'type': 'boolean'},
                        '엔카진단여부': {'type': 'boolean'},
                        '광고정보': {'type': 'keyword'},
                        '상세페이지URL': {'type': 'keyword'},
                        '페이지번호': {'type': 'integer'},
                        
                        # 상세 정보 필드
                        '차량번호': {'type': 'keyword'},
                        '상세연식': {'type': 'keyword'},
                        '상세주행거리': {'type': 'keyword'},
                        '배기량': {'type': 'keyword'},
                        '상세연료': {'type': 'keyword'},
                        '변속기': {'type': 'keyword'},
                        '차종': {'type': 'keyword'},
                        '색상': {'type': 'keyword'},
                        '상세지역': {'type': 'keyword'},
                        '인승': {'type': 'keyword'},
                        '수입구분': {'type': 'keyword'},
                        '압류저당': {'type': 'keyword'},
                        '조회수': {'type': 'keyword'},
                        '찜수': {'type': 'keyword'},
                        
                        # 메타데이터
                        '크롤링시간': {'type': 'date'}
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
    OpenSearch에 차량 데이터 인덱싱
    
    Args:
        client: OpenSearch client
        car_dict: Dictionary containing car data
        car_index: Index of the car in the list
        
    Returns:
        bool: True if indexing was successful, False otherwise
    """
    try:
        # 인덱싱 전 데이터 검증
        if not car_dict.get('세부모델'):
            logging.warning(f"차량 {car_index} 데이터 누락: 세부모델 필드가 비어 있습니다.")
        
        # 차량ID 검증 (중요 필드)
        if not car_dict.get('차량ID'):
            logging.warning(f"차량 {car_index} 데이터 누락: 차량ID 필드가 비어 있습니다.")
        
        # 크롤링 시간이 문자열이면 날짜 형식으로 변환
        if isinstance(car_dict.get('크롤링시간'), str):
            try:
                # ISO 형식으로 변환
                car_dict['크롤링시간'] = datetime.fromisoformat(car_dict['크롤링시간']).isoformat()
            except:
                # 변환 실패 시 현재 시간 사용
                car_dict['크롤링시간'] = datetime.now().isoformat()
        
        # OpenSearch에 인덱싱
        response = client.index(
            index='encar_cars_detail',
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

def get_index_stats(client):
    """
    인덱스 통계 조회
    
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
        logging.error(f"인덱스 통계 조회 중 오류: {str(e)}")
        return None 