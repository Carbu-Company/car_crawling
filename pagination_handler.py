"""
Module for handling pagination on the Encar website.
"""

import time
import logging
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import config

def get_total_pages(driver):
    """
    Get the total number of pages from the pagination section.
    
    Args:
        driver: Selenium WebDriver instance
        
    Returns:
        int: Total number of pages
    """
    try:
        # 페이지네이션 영역 찾기
        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".paginate"))
        )
        
        # 마지막 페이지 번호 찾기
        page_links = pagination.find_elements(By.TAG_NAME, "a")
        if not page_links:
            logging.warning("페이지 링크를 찾을 수 없습니다.")
            return 1
        
        # 마지막 페이지 번호 추출
        last_page_text = page_links[-1].text
        if last_page_text.isdigit():
            total_pages = int(last_page_text)
        else:
            # 마지막 링크가 숫자가 아니면 (예: "다음" 버튼), 그 전 링크 확인
            for link in reversed(page_links):
                if link.text.isdigit():
                    total_pages = int(link.text)
                    break
            else:
                logging.warning("페이지 번호를 찾을 수 없습니다. 기본값 1을 사용합니다.")
                total_pages = 1
        
        logging.info(f"총 페이지 수: {total_pages}")
        return total_pages
    
    except (TimeoutException, NoSuchElementException) as e:
        logging.error(f"총 페이지 수 확인 중 오류 발생: {e}")
        return 1

def go_to_page(driver, page_number):
    """
    Navigate to a specific page in the pagination.
    
    Args:
        driver: Selenium WebDriver instance
        page_number: Page number to navigate to
        
    Returns:
        bool: True if navigation was successful, False otherwise
    """
    try:
        logging.info(f"페이지 {page_number}로 이동 시도 중...")
        
        # 페이지네이션 영역 찾기
        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, ".paginate"))
        )
        
        # 현재 표시된 페이지 링크들 확인
        page_links = pagination.find_elements(By.TAG_NAME, "a")
        
        # 직접 페이지 번호 클릭 시도
        for link in page_links:
            if link.text.isdigit() and int(link.text) == page_number:
                link.click()
                time.sleep(2)  # 페이지 로딩 대기
                logging.info(f"페이지 {page_number}로 이동 완료")
                return True
        
        # 직접 페이지를 찾지 못한 경우, 다음/이전 버튼 사용
        if page_number > 1:
            # 다음 버튼 찾기
            next_buttons = [link for link in page_links if not link.text.isdigit()]
            if next_buttons:
                next_button = next_buttons[-1]  # 마지막 비숫자 링크는 보통 '다음' 버튼
                next_button.click()
                time.sleep(2)
                logging.info("다음 페이지 세트로 이동")
                return go_to_page(driver, page_number)  # 재귀적으로 다시 시도
        
        logging.warning(f"페이지 {page_number}를 찾을 수 없습니다.")
        return False
    
    except (TimeoutException, NoSuchElementException) as e:
        logging.error(f"페이지 {page_number}로 이동 중 오류 발생: {e}")
        return False

def is_last_page(driver, current_page, total_pages):
    """
    Check if the current page is the last page.
    
    Args:
        driver: Selenium WebDriver instance
        current_page: Current page number
        total_pages: Total number of pages
        
    Returns:
        bool: True if current page is the last page, False otherwise
    """
    return current_page >= total_pages

def navigate_to_page(driver, page_number):
    """
    Navigate to a specific page in the search results.
    
    Args:
        driver: Selenium WebDriver instance
        page_number: Page number to navigate to
        
    Returns:
        bool: True if navigation was successful, False otherwise
    """
    try:
        # 현재 페이지 URL 설정
        url = config.BASE_URL.format(page=page_number)
        
        # 페이지 로드
        driver.get(url)
        
        # 페이지가 완전히 로드될 때까지 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, config.SELECTORS["car_list"]))
        )
        
        # 인간처럼 행동하기 위한 랜덤 대기
        time.sleep(config.get_page_load_wait())
        
        return True
    except Exception as e:
        print(f"페이지 {page_number}로 이동 중 오류 발생: {e}")
        return False

def go_to_next_page(driver, current_page):
    """
    Navigate to the next page in the search results.
    
    Args:
        driver: Selenium WebDriver instance
        current_page: Current page number
        
    Returns:
        int or None: Next page number if successful, None otherwise
    """
    try:
        # 페이지네이션 요소 찾기
        pagination = WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, config.SELECTORS["pagination"]))
        )
        
        # 다음 페이지 버튼 찾기 (현재 페이지가 10의 배수이면 다음 10페이지 버튼, 아니면 다음 페이지 번호)
        if current_page % 10 == 0:
            # 다음 10페이지 버튼 클릭
            next_button = pagination.find_element(By.CSS_SELECTOR, config.SELECTORS["next_button"])
            next_page = int(next_button.get_attribute("data-page"))
            next_button.click()
        else:
            # 다음 페이지 번호 클릭
            next_page = current_page + 1
            next_page_link = pagination.find_element(By.CSS_SELECTOR, f"a[data-page='{next_page}']")
            next_page_link.click()
        
        # 페이지 로드 대기
        time.sleep(config.get_pagination_wait())
        
        # 페이지가 완전히 로드될 때까지 대기
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, config.SELECTORS["car_list"]))
        )
        
        return next_page
    except Exception as e:
        print(f"다음 페이지로 이동 중 오류 발생: {e}")
        return None

def should_continue_crawling(current_page, max_pages=None):
    """
    Determine if crawling should continue based on the current page number.
    
    Args:
        current_page: Current page number
        max_pages: Maximum number of pages to crawl (default: config.MAX_PAGES)
        
    Returns:
        bool: True if crawling should continue, False otherwise
    """
    if max_pages is None:
        max_pages = config.MAX_PAGES
        
    if current_page > max_pages:
        print(f"최대 페이지 수({max_pages})에 도달했습니다. 크롤링을 종료합니다.")
        return False
    
    return True 