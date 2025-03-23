"""
Main module for the Encar crawler.
"""

import time
import logging
import sys
import random
from selenium.webdriver.common.by import By
from selenium.common.exceptions import (
    TimeoutException, 
    WebDriverException, 
    UnexpectedAlertPresentException, 
    NoAlertPresentException
)

import config
import driver_setup
import car_detail_extractor
import pagination_handler
import data_processor
import opensearch_handler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crawler.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

class EncarCrawler:
    """Class to manage the crawling of Encar website"""
    
    def __init__(self, start_page=100, max_pages=None, save_all=True, use_opensearch=True):
        """
        Initialize the crawler.
        
        Args:
            start_page: Page number to start crawling from
            max_pages: Maximum number of pages to crawl
            save_all: Whether to save all data to a single file
            use_opensearch: Whether to use OpenSearch for indexing
        """
        self.start_page = start_page
        self.max_pages = max_pages or config.MAX_PAGES
        self.save_all = save_all
        self.use_opensearch = use_opensearch
        self.driver = None
        self.opensearch_client = None
        self.all_car_data = []
        
        # Initialize robot detection counters
        if not hasattr(config, 'ROBOT_DETECTION_COUNT'):
            config.ROBOT_DETECTION_COUNT = 0
        if not hasattr(config, 'ROBOT_DETECTION_COOLDOWN'):
            config.ROBOT_DETECTION_COOLDOWN = 300  # Default 5 minutes
        if not hasattr(config, 'LAST_ROBOT_DETECTION'):
            config.LAST_ROBOT_DETECTION = 0

    def initialize_driver(self):
        """Initialize and set up the WebDriver"""
        self.driver = driver_setup.setup_driver()
        
        # Set WebDriver command timeout
        if hasattr(self.driver, 'command_executor'):
            self.driver.command_executor._conn.timeout = 600.0
            logging.info(f"WebDriver command timeout set to {self.driver.command_executor._conn.timeout} seconds")
            
            # Set page load and script timeouts
            self.driver.set_page_load_timeout(300)
            self.driver.set_script_timeout(300)
            logging.info("Page load and script timeouts set to 300 seconds")
        
        # Try to randomize user agent
        try:
            self.driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": config.get_random_user_agent()
            })
            logging.info("User agent randomized")
        except Exception as e:
            logging.error(f"Failed to set user agent: {e}")
    
    def initialize_opensearch(self):
        """Initialize OpenSearch client if enabled"""
        if not self.use_opensearch:
            return None
            
        try:
            logging.info("Creating OpenSearch client and setting up index...")
            self.opensearch_client = opensearch_handler.create_opensearch_client()
            opensearch_handler.create_encar_index(self.opensearch_client)
            logging.info("OpenSearch setup complete")
        except Exception as e:
            logging.error(f"Error setting up OpenSearch: {e}")
            logging.warning("Continuing without OpenSearch indexing")
            self.opensearch_client = None
    
    def accept_cookies_and_setup(self):
        """Accept cookies and set up initial page"""
        try:
            self.driver.get("http://www.encar.com")
            time.sleep(random.uniform(1, 3))
            
            # Accept cookies if button exists
            try:
                cookie_accept = self.driver.find_element(By.CSS_SELECTOR, ".btn_accept")
                if cookie_accept:
                    cookie_accept.click()
                    logging.info("Clicked cookie accept button")
                    time.sleep(random.uniform(1, 2))
            except Exception:
                pass
                
        except Exception as e:
            logging.error(f"Error accessing homepage: {e}")
    
    def handle_alert(self, alert_context=""):
        """
        Handle any browser alerts and treat them as robot detection
        
        Args:
            alert_context: Context where the alert was triggered
            
        Returns:
            bool: True if alert was handled, False otherwise
        """
        try:
            alert = self.driver.switch_to.alert
            alert_text = alert.text
            logging.warning(f"Alert detected during {alert_context}: {alert_text}")
            alert.accept()
            
            # Treat as robot detection
            config.LAST_ROBOT_DETECTION = time.time()
            config.ROBOT_DETECTION_COUNT += 1
            
            # Apply exponential backoff (max 30 minutes)
            backoff_time = min(300 * (2 ** config.ROBOT_DETECTION_COUNT), 1800)
            config.ROBOT_DETECTION_COOLDOWN = backoff_time
            
            logging.info(f"Waiting {backoff_time} seconds after robot detection...")
            
            return True
        except NoAlertPresentException:
            logging.error("Alert detected but couldn't be processed")
            return False
    
    def reset_driver(self):
        """Reset the WebDriver after errors or robot detection"""
        logging.warning("Resetting WebDriver session")
        
        # Clean up existing driver
        if self.driver:
            driver_setup.cleanup_driver(self.driver)
        
        # Set up new driver
        self.initialize_driver()
        
        # Wait randomly to reduce robot detection chances
        wait_time = random.uniform(30, 60)
        logging.info(f"Waiting {wait_time:.0f} seconds after driver reset...")
        time.sleep(wait_time)
    
    def crawl_page(self, page_number):
        """
        Crawl a single page of car listings.
        
        Args:
            page_number: Page number to crawl
            
        Returns:
            list: List of car data dictionaries from this page
            bool: Flag indicating if driver needs to be reset
        """
        logging.info(f"\n===== Starting crawl of page {page_number} =====\n")
        
        # Check if enough time has passed since last robot detection
        current_time = time.time()
        if config.LAST_ROBOT_DETECTION > 0:
            time_since_detection = current_time - config.LAST_ROBOT_DETECTION
            if time_since_detection < config.ROBOT_DETECTION_COOLDOWN:
                wait_time = min(config.ROBOT_DETECTION_COOLDOWN - time_since_detection, 10)
                logging.info(f"Waiting {wait_time:.0f} seconds after robot detection...")
                time.sleep(wait_time)
        
        # Check session validity
        if not car_detail_extractor.is_session_valid(self.driver):
            logging.error("WebDriver session is invalid. Driver needs to be reset.")
            return [], True
        
        # Navigate to page
        try:
            if not pagination_handler.navigate_to_page(self.driver, page_number):
                return [], False
        except UnexpectedAlertPresentException:
            if self.handle_alert("page navigation"):
                return [], True
            return [], True
        
        # Get car listings
        try:
            # Simulate human behavior - slight delay after page load
            random_delay = random.uniform(2, 5)
            time.sleep(random_delay)
            
            car_items = self.driver.find_elements(By.CSS_SELECTOR, config.SELECTORS["car_items"])
            logging.info(f"Found {len(car_items)} cars")
        except UnexpectedAlertPresentException:
            if self.handle_alert("getting car list"):
                return [], True
            return [], True
        except Exception as e:
            logging.error(f"Error getting car list: {e}")
            return [], True
        
        # If no cars found, return empty list
        if len(car_items) == 0:
            logging.info("No more cars found")
            return [], False
        
        # List to store car data for current page
        page_car_data = []
        
        # Show progress
        total_cars = len(car_items)
        indexed_count = 0
        reset_needed = False
        
        for idx, car in enumerate(car_items):
            try:
                logging.info(f"Processing car {idx+1}/{total_cars}...")
                
                # Check session validity again
                if not car_detail_extractor.is_session_valid(self.driver):
                    logging.error("WebDriver session is invalid. Stopping car processing.")
                    reset_needed = True
                    break
                
                # Extract basic car info
                car_info = car_detail_extractor.extract_car_info(car, self.all_car_data)
                
                # Skip if duplicate or extraction failed
                if car_info is None:
                    continue
                
                # Add page number
                car_info["페이지번호"] = page_number
                
                # Simulate human behavior - variable wait time
                random_delay = random.uniform(0.5, 2.5)
                time.sleep(random_delay)
                
                # Get detail info
                logging.info(f"Getting details for car ID {car_info['차량ID']}...")
                try:
                    detail_info = car_detail_extractor.get_car_detail_info(self.driver, car_info["상세페이지URL"])
                except UnexpectedAlertPresentException:
                    if self.handle_alert("getting car details"):
                        reset_needed = True
                        break
                    reset_needed = True
                    break
                
                # Check for session error
                if "세션오류" in detail_info:
                    logging.error("Session error detected. Driver needs to be reset.")
                    reset_needed = True
                    break
                
                # Merge basic and detail info
                car_info.update(detail_info)
                
                # Add to data lists
                page_car_data.append(car_info)
                self.all_car_data.append(car_info)
                
                # Index to OpenSearch if client exists
                if self.opensearch_client:
                    if opensearch_handler.index_car_to_opensearch(self.opensearch_client, car_info, idx+1):
                        indexed_count += 1
                
                # Simulate human behavior - variable wait time
                wait_time = config.get_car_processing_wait() * random.uniform(0.8, 1.2)
                time.sleep(wait_time)
                
            except UnexpectedAlertPresentException:
                if self.handle_alert("processing car"):
                    reset_needed = True
                    break
                reset_needed = True
                break
            except Exception as e:
                logging.error(f"Error extracting car info: {e}")
                # Check for session error
                if "invalid session id" in str(e) or "no such session" in str(e):
                    logging.error("Session error detected. Driver needs to be reset.")
                    reset_needed = True
                    break
                continue
        
        # Log indexing summary
        if self.opensearch_client and page_car_data:
            logging.info(f"Page {page_number}: Indexed {indexed_count}/{len(page_car_data)} cars")
        
        return page_car_data, reset_needed
    
    def run(self):
        """Main method to run the crawler"""
        try:
            # Initialize WebDriver
            self.initialize_driver()
            
            # Accept cookies and initial setup
            self.accept_cookies_and_setup()
            
            # Initialize OpenSearch
            self.initialize_opensearch()
            
            # Crawling state variables
            current_page = self.start_page
            pages_crawled = 0
            continue_crawling = True
            
            while continue_crawling and (pages_crawled < self.max_pages):
                # Check for and handle alerts before page crawl
                try:
                    alert = self.driver.switch_to.alert
                    if self.handle_alert("before page crawl"):
                        self.reset_driver()
                        continue
                except NoAlertPresentException:
                    pass  # No alert, continue normally
                
                # Crawl current page
                try:
                    page_car_data, reset_needed = self.crawl_page(current_page)
                    
                    # Reset driver if needed
                    if reset_needed:
                        self.reset_driver()
                        # Retry current page
                        logging.info(f"Retrying page {current_page}")
                        continue
                    
                    # Stop if no cars found
                    if not page_car_data:
                        logging.info("No more cars found. Stopping crawl.")
                        break
                    
                    # Increment page count
                    pages_crawled += 1
                    
                    # Check if max pages reached
                    if pages_crawled >= self.max_pages:
                        logging.info(f"Reached maximum page count ({self.max_pages}). Stopping crawl.")
                        break
                    
                    # Simulate human behavior - variable wait time
                    random_delay = random.uniform(2, 5)
                    logging.info(f"Waiting {random_delay:.1f} seconds before next page...")
                    time.sleep(random_delay)
                    
                    # Go to next page
                    try:
                        next_page = pagination_handler.go_to_next_page(self.driver, current_page)
                        
                        if next_page is None:
                            logging.info("Reached last page or failed to navigate.")
                            continue_crawling = False
                        else:
                            current_page = next_page
                    except UnexpectedAlertPresentException:
                        if self.handle_alert("navigating to next page"):
                            self.reset_driver()
                            continue
                        self.reset_driver()
                        continue
                    except TimeoutException as e:
                        logging.error(f"Timeout during page navigation: {e}")
                        self.reset_driver()
                        current_page += 1
                        continue
                    
                except UnexpectedAlertPresentException:
                    if self.handle_alert("during page crawl"):
                        self.reset_driver()
                        continue
                    self.reset_driver()
                    continue
                except TimeoutException as e:
                    logging.error(f"Timeout processing page {current_page}: {e}")
                    self.reset_driver()
                    current_page += 1
                    continue
                except WebDriverException as e:
                    if "timeout" in str(e).lower():
                        logging.error(f"WebDriver timeout: {e}")
                        self.reset_driver()
                        current_page += 1
                        continue
                    else:
                        raise
            
            # Save all data if requested
            if self.all_car_data and self.save_all:
                data_processor.save_all_data(self.all_car_data)
                data_processor.print_data_summary(self.all_car_data)
                
                logging.info(f"\n===== Crawling complete =====")
                logging.info(f"Collected information on {len(self.all_car_data)} cars.")
                
                # Print OpenSearch index stats if available
                if self.opensearch_client:
                    opensearch_handler.get_index_stats(self.opensearch_client)
        
        except Exception as e:
            logging.error(f"Error during crawling: {e}")
            import traceback
            logging.error(traceback.format_exc())
        
        finally:
            # Random wait before closing browser
            time.sleep(config.get_browser_close_wait())
            
            # Clean up WebDriver
            try:
                if self.driver:
                    # Set short timeouts for clean shutdown
                    try:
                        self.driver.set_page_load_timeout(30)
                        self.driver.set_script_timeout(30)
                    except Exception:
                        pass
                        
                    driver_setup.cleanup_driver(self.driver)
            except Exception as e:
                logging.error(f"Error cleaning up WebDriver: {e}")
                # Force kill processes
                driver_setup.kill_chrome_processes()


def cleanup_existing_processes():
    """Clean up any existing Chrome and ChromeDriver processes before starting"""
    logging.info("Cleaning up existing Chrome and ChromeDriver processes...")
    driver_setup.kill_chrome_processes()
    logging.info("Process cleanup complete")


def main():
    """Entry point of the program with retry mechanism"""
    logging.info("Starting Encar car data collection and OpenSearch indexing...")
    
    # Clean up existing Chrome processes
    cleanup_existing_processes()
    
    # Retry counter
    retry_count = 0
    
    while retry_count < config.MAX_RETRIES:
        try:
            # Create and run crawler
            crawler = EncarCrawler(
                start_page=66,  # Starting page number
                max_pages=None,  # Use default from config if None
                save_all=True,   # Save all data to a single file
                use_opensearch=True  # Use OpenSearch for indexing
            )
            
            crawler.run()
            break  # Exit loop if successful
            
        except Exception as e:
            retry_count += 1
            logging.error(f"Crawling failed ({retry_count}/{config.MAX_RETRIES}): {e}")
            
            # Clean up processes after failure
            driver_setup.kill_chrome_processes()
            
            if retry_count < config.MAX_RETRIES:
                wait_time = config.get_retry_wait()
                logging.info(f"Retrying in {wait_time:.0f} seconds...")
                time.sleep(wait_time)
            else:
                logging.error("Maximum retry count exceeded. Exiting program.")
    
    # Final process cleanup
    driver_setup.kill_chrome_processes()


if __name__ == "__main__":
    main() 