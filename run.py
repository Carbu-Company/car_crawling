#!/usr/bin/env python3
"""
Main entry point for the Encar crawler.
"""

import argparse
import logging
import sys
import time
import traceback
import signal
import platform
import subprocess
from main import crawl_encar
import driver_setup
import config

def signal_handler(sig, frame):
    """
    Handle termination signals and clean up resources.
    """
    logging.info("프로그램 종료 신호를 받았습니다. 리소스를 정리합니다...")
    
    # Chrome 관련 프로세스 강제 종료
    try:
        driver_setup.kill_chrome_processes()
        logging.info("Chrome 프로세스 정리 완료")
    except Exception as e:
        logging.error(f"프로세스 정리 중 오류: {e}")
    
    sys.exit(0)

def setup_logging():
    """
    Set up logging configuration.
    
    Returns:
        logging.Logger: Configured logger
    """
    # 로그 파일명 생성
    log_filename = config.get_log_filename()
    
    # 로거 설정
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, config.LOG_LEVEL))
    
    # 포맷터 설정
    formatter = logging.Formatter(
        config.LOG_FORMAT,
        datefmt=config.LOG_DATE_FORMAT
    )
    
    # 파일 핸들러 설정
    file_handler = logging.FileHandler(log_filename, encoding='utf-8')
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    
    # 콘솔 핸들러 설정
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    return logger

def parse_arguments():
    """
    Parse command-line arguments.
    
    Returns:
        argparse.Namespace: Parsed arguments
    """
    parser = argparse.ArgumentParser(description='Encar 차량 정보 크롤러')
    
    parser.add_argument(
        '--pages', 
        type=int, 
        default=config.MAX_PAGES,
        help=f'크롤링할 최대 페이지 수 (기본값: {config.MAX_PAGES})'
    )
    
    parser.add_argument(
        '--start-page', 
        type=int, 
        default=1,
        help='크롤링을 시작할 페이지 번호 (기본값: 1)'
    )
    
    parser.add_argument(
        '--headless', 
        action='store_true',
        default=config.HEADLESS_MODE,
        help='헤드리스 모드로 실행 (UI 없음)'
    )
    
    parser.add_argument(
        '--save-all', 
        action='store_true',
        help='모든 데이터를 하나의 파일로 저장'
    )
    
    parser.add_argument(
        '--use-opensearch', 
        action='store_true',
        help='OpenSearch에 데이터 인덱싱'
    )
    
    parser.add_argument(
        '--retries', 
        type=int, 
        default=config.MAX_RETRIES,
        help=f'오류 발생 시 재시도 횟수 (기본값: {config.MAX_RETRIES})'
    )
    
    return parser.parse_args()

def main():
    """
    Main function to run the crawler.
    """
    # 종료 신호 핸들러 등록
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # 로깅 설정
    logger = setup_logging()
    
    # 명령행 인수 파싱
    args = parse_arguments()
    
    # 설정 업데이트
    config.MAX_PAGES = args.pages
    config.HEADLESS_MODE = args.headless
    config.MAX_RETRIES = args.retries
    
    # 기존 Chrome 프로세스 정리
    try:
        driver_setup.kill_chrome_processes()
        logger.info("기존 Chrome 프로세스 정리 완료")
    except Exception as e:
        logger.error(f"프로세스 정리 중 오류: {e}")
    
    # 크롤링 시작 메시지
    logger.info("=" * 50)
    logger.info("Encar 차량 정보 크롤러 시작")
    logger.info(f"최대 페이지 수: {args.pages}")
    logger.info(f"시작 페이지: {args.start_page}")
    logger.info(f"헤드리스 모드: {args.headless}")
    logger.info(f"OpenSearch 사용: {args.use_opensearch}")
    logger.info("=" * 50)
    
    # 크롤링 실행
    start_time = time.time()
    
    try:
        crawl_encar(
            start_page=args.start_page,
            max_pages=args.pages,
            save_all=args.save_all,
            use_opensearch=args.use_opensearch
        )
        
        # 크롤링 완료 메시지
        elapsed_time = time.time() - start_time
        logger.info("=" * 50)
        logger.info("크롤링이 성공적으로 완료되었습니다.")
        logger.info(f"소요 시간: {elapsed_time:.2f}초")
        logger.info("=" * 50)
        
    except Exception as e:
        # 오류 발생 시 처리
        elapsed_time = time.time() - start_time
        logger.error("=" * 50)
        logger.error(f"크롤링 중 오류 발생: {e}")
        logger.error(traceback.format_exc())
        logger.error(f"소요 시간: {elapsed_time:.2f}초")
        logger.error("=" * 50)
        return 1
    finally:
        # 프로그램 종료 전 최종 프로세스 정리
        try:
            driver_setup.kill_chrome_processes()
            logger.info("Chrome 프로세스 정리 완료")
        except Exception as e:
            logger.error(f"프로세스 정리 중 오류: {e}")
    
    return 0

if __name__ == "__main__":
    sys.exit(main()) 