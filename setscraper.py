from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import time

def scrape_bbc6_episode(url):
    # Set up Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    try:
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "segments-list__item"))
        )
        print(f"Successfully loaded: {url}")

        dj_name = driver.find_element(By.CSS_SELECTOR, "a.context__item").text
        episode_title = driver.find_element(By.CSS_SELECTOR, "h1.no-margin").text
        print(f"DJ: {dj_name}")
        print(f"Episode: {episode_title}")

    except TimeoutException:
        print("Timed out waiting for page to load")
    
    finally:
        driver.quit()

# Test with one URL
url = "https://www.bbc.co.uk/programmes/m002845d"
scrape_bbc6_episode(url)