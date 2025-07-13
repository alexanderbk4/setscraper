"""
Episode-level scraping for BBC 6 Music episodes.

This module handles scraping episode metadata and orchestrating track extraction.
"""

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
import pandas as pd
import time

from .tracks import extract_tracks_from_episode


def scrape_bbc6_episode(url):
    """
    Scrape a BBC 6 Music episode and extract all track information.
    
    Args:
        url (str): URL of the BBC 6 Music episode
        
    Returns:
        pd.DataFrame: DataFrame containing all tracks with episode metadata
    """
    # Set up Chrome options
    chrome_options = Options()
    # chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    
    # Initialize the driver
    driver = webdriver.Chrome(options=chrome_options)
    driver.get(url)
    
    try:
        # Wait for page to load
        WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.CLASS_NAME, "segments-list__item"))
        )
        print(f"Successfully loaded: {url}")

        # Extract episode metadata
        dj_name = driver.find_element(By.CSS_SELECTOR, "a.context__item").text
        episode_title = driver.find_element(By.CSS_SELECTOR, "h1.no-margin").text
        print(f"DJ: {dj_name}")
        print(f"Episode: {episode_title}")

        # Click "Show more" button if it exists to reveal all tracks
        try:
            show_more = driver.find_element(By.CLASS_NAME, "ml__label--more")
            driver.execute_script("arguments[0].click();", show_more)
            time.sleep(2)
            print("Clicked 'Show more' button")
        except:
            print("No 'Show more' button found")

        # Extract tracks using the tracks module
        tracks_df = extract_tracks_from_episode(driver, dj_name, episode_title)
        
        return tracks_df
        
    except TimeoutException:
        print("Timed out waiting for page to load")
        return pd.DataFrame()
    
    finally:
        driver.quit()


def analyze_multiple_djs(episode_urls):
    """
    Scrape multiple episodes and combine the data.
    
    Args:
        episode_urls (list): List of BBC 6 Music episode URLs
        
    Returns:
        pd.DataFrame: Combined DataFrame of all tracks from all episodes
    """
    all_data = []
    
    for url in episode_urls:
        df = scrape_bbc6_episode(url)
        if not df.empty:
            all_data.append(df)
    
    # Combine all data
    if all_data:
        combined_df = pd.concat(all_data, ignore_index=True)
        return combined_df
    else:
        return pd.DataFrame() 