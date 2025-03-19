"""
Module for processing and saving car data.
"""

import pandas as pd
import time
import logging
import config

def save_page_data(car_data, page_number):
    """
    Save data from a single page to a CSV file.
    
    Args:
        car_data: List of car data dictionaries
        page_number: Page number
        
    Returns:
        str: Filename where data was saved
    """
    if not car_data:
        logging.warning(f"페이지 {page_number}에 저장할 데이터가 없습니다.")
        return None
    
    try:
        # 데이터프레임으로 변환
        df = pd.DataFrame(car_data)
        
        # 파일명 생성
        filename = config.get_page_filename(page_number)
        
        # CSV 파일로 저장
        # df.to_csv(filename, index=False, encoding="utf-8-sig")
        
        logging.info(f"페이지 {page_number}의 데이터가 {filename}에 저장되었습니다.")
        return filename
    except Exception as e:
        logging.error(f"페이지 {page_number} 데이터 저장 중 오류 발생: {e}")
        return None

def save_all_data(all_car_data):
    """
    Save all collected data to a CSV file.
    
    Args:
        all_car_data: List of all car data dictionaries
        
    Returns:
        str: Filename where data was saved
    """
    if not all_car_data:
        logging.warning("저장할 데이터가 없습니다.")
        return None
    

def print_data_summary(car_data):
    """
    Print a summary of the collected data.
    
    Args:
        car_data: List of car data dictionaries
    """
    if not car_data:
        logging.warning("요약할 데이터가 없습니다.")
        return
    
    try:
        # 데이터프레임으로 변환
        df = pd.DataFrame(car_data)
        
        # 데이터 요약 출력
        logging.info("\n===== 데이터 요약 =====")
        logging.info(f"총 차량 수: {len(df)}")
        
        # 제조사별 차량 수
        logging.info("\n제조사별 차량 수:")
        for manufacturer, count in df["제조사"].value_counts().items():
            logging.info(f"{manufacturer}: {count}")
        
        # 연료별 차량 수
        logging.info("\n연료별 차량 수:")
        for fuel, count in df["연료"].value_counts().items():
            logging.info(f"{fuel}: {count}")
        
        # 지역별 차량 수
        logging.info("\n지역별 차량 수:")
        for location, count in df["지역"].value_counts().items():
            logging.info(f"{location}: {count}")
        
        # 가격 통계
        if "가격값" in df.columns:
            # 숫자로 변환 가능한 가격만 선택
            numeric_prices = pd.to_numeric(df["가격값"].replace("정보없음", None), errors="coerce")
            logging.info("\n가격 통계 (만원):")
            logging.info(f"최소: {numeric_prices.min()}")
            logging.info(f"최대: {numeric_prices.max()}")
            logging.info(f"평균: {numeric_prices.mean():.2f}")
            logging.info(f"중앙값: {numeric_prices.median()}")
        
    except Exception as e:
        logging.error(f"데이터 요약 출력 중 오류 발생: {e}")

def save_checkpoint(car_data, idx, page_number):
    """
    Save a checkpoint of the data at regular intervals.
    
    Args:
        car_data: List of car data dictionaries
        idx: Current index
        page_number: Current page number
        
    Returns:
        bool: True if checkpoint was saved, False otherwise
    """
    # 5개 차량마다 중간 저장
    if (idx + 1) % 5 == 0:
        try:
            temp_df = pd.DataFrame(car_data)
            checkpoint_filename = f"encar_benz_data_checkpoint_page{page_number}_{idx+1}.csv"
            temp_df.to_csv(checkpoint_filename, index=False, encoding="utf-8-sig")
            logging.info(f"중간 저장 완료: 페이지 {page_number}에서 {idx+1}개 차량 처리됨")
            return True
        except Exception as e:
            logging.error(f"중간 저장 중 오류 발생: {e}")
            return False
    
    return False 