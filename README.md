# Encar 차량 정보 크롤러

이 프로젝트는 Encar 웹사이트에서 차량 정보를 수집하고 분석하는 크롤러입니다. 수집된 데이터는 CSV 파일로 저장되거나 OpenSearch에 인덱싱됩니다.

## 기능

- Encar 웹사이트에서 차량 목록 및 상세 정보 수집
- 페이지네이션 처리를 통한 다중 페이지 크롤링
- 수집된 데이터를 CSV 파일로 저장
- OpenSearch에 데이터 인덱싱 및 검색 기능
- 로깅 시스템을 통한 크롤링 과정 모니터링
- 오류 발생 시 자동 재시도 및 스크린샷 저장

## 설치 방법

1. 저장소 클론:
   ```
   git clone https://github.com/yourusername/encar-crawler.git
   cd encar-crawler
   ```

2. 필요한 패키지 설치:
   ```
   pip install -r requirements.txt
   ```

## 사용 방법

### 기본 실행

```
python run.py
```

### 명령행 옵션

- `--pages`: 크롤링할 최대 페이지 수 (기본값: 5)
- `--start-page`: 크롤링을 시작할 페이지 번호 (기본값: 1)
- `--headless`: 헤드리스 모드로 실행 (UI 없음)
- `--save-all`: 모든 데이터를 하나의 파일로 저장
- `--use-opensearch`: OpenSearch에 데이터 인덱싱
- `--retries`: 오류 발생 시 재시도 횟수 (기본값: 3)

### 예시

```
# 10페이지 크롤링 및 OpenSearch에 저장
python run.py --pages 10 --use-opensearch

# 헤드리스 모드로 실행하고 모든 데이터를 하나의 파일로 저장
python run.py --headless --save-all

# 5페이지부터 시작하여 20페이지까지 크롤링
python run.py --start-page 5 --pages 20
```

## 프로젝트 구조

```
encar_crawler/
├── run.py                  # 메인 실행 파일
├── main.py                 # 크롤링 메인 로직
├── config.py               # 설정 파일
├── driver_setup.py         # WebDriver 설정
├── car_detail_extractor.py # 차량 상세 정보 추출
├── pagination_handler.py   # 페이지네이션 처리
├── data_processor.py       # 데이터 처리 및 저장
├── opensearch_handler.py   # OpenSearch 연동
├── data/                   # 수집된 데이터 저장 디렉토리
├── logs/                   # 로그 파일 저장 디렉토리
├── screenshots/            # 스크린샷 저장 디렉토리
└── requirements.txt        # 필요한 패키지 목록
```

## 설정

`config.py` 파일에서 다음 설정을 변경할 수 있습니다:

- 크롤링 URL 및 페이지 수
- WebDriver 설정
- 데이터 저장 경로
- OpenSearch 연결 정보
- 로깅 설정

## OpenSearch 설정

OpenSearch를 사용하려면 `config.py` 파일에서 다음 설정을 확인하세요:

```python
OPENSEARCH_HOST = "your-opensearch-host"
OPENSEARCH_PORT = 9200
OPENSEARCH_USERNAME = "username"
OPENSEARCH_PASSWORD = "password"
OPENSEARCH_INDEX_NAME = "encar_vehicles"
OPENSEARCH_USE_SSL = True
OPENSEARCH_VERIFY_CERTS = True
```

## 로깅

로그 파일은 `logs` 디렉토리에 저장되며, 콘솔에도 출력됩니다. 로그 레벨은 `config.py` 파일에서 설정할 수 있습니다.

## 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 LICENSE 파일을 참조하세요. 