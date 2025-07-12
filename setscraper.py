from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
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
    time.sleep(10)
    
    print(f"Successfully loaded: {url}")
    
    driver.quit()

# Test with one URL
url = "https://www.bbc.co.uk/programmes/m002845d"
scrape_bbc6_episode(url)