"""
Episode discovery for BBC 6 Music.

This module systematically checks BBC URLs to find working episodes and extract metadata.
"""

import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
from typing import List


def generate_episode_ids(start_suffix: str = "0000", end_suffix: str = "zzzz"):
    """
    Generate episode IDs in the correct BBC format.
    
    Args:
        start_suffix (str): Starting 4-character suffix (e.g., "0000")
        end_suffix (str): Ending 4-character suffix (e.g., "zzzz")
        
    Yields:
        str: Episode IDs in format m002xxxx
    """
    # Define allowed characters (numbers + consonants, no vowels)
    allowed_chars = "0123456789bcdfghjklmnpqrstvwxz"
    
    # Convert suffixes to indices
    def suffix_to_index(suffix):
        return sum(allowed_chars.index(c) * (len(allowed_chars) ** i) 
                  for i, c in enumerate(reversed(suffix.lower())))
    
    def index_to_suffix(index):
        if index == 0:
            return "0000"
        suffix = ""
        while index > 0:
            index, remainder = divmod(index, len(allowed_chars))
            suffix = allowed_chars[remainder] + suffix
        return suffix.zfill(4)
    
    start_index = suffix_to_index(start_suffix)
    end_index = suffix_to_index(end_suffix)
    
    for i in range(start_index, end_index + 1):
        suffix = index_to_suffix(i)
        yield f"m002{suffix}"


def discover_episodes_basic(start_suffix: str = "0000", end_suffix: str = "zzzz", step: int = 1):
    """
    Discover BBC episodes by checking URL patterns and extracting metadata.
    
    Args:
        start_suffix (str): Starting 4-character suffix
        end_suffix (str): Ending 4-character suffix
        step (int): Step size for incrementing (use 1 for full scan)
        
    Returns:
        pd.DataFrame: DataFrame with episode_id, channel, show_name, episode_name
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run headless for discovery
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    
    driver = webdriver.Chrome(options=chrome_options)
    discovered_episodes = []
    
    try:
        # Generate episode IDs
        episode_ids = list(generate_episode_ids(start_suffix, end_suffix))
        
        # Apply step if needed
        if step > 1:
            episode_ids = episode_ids[::step]
        
        total_episodes = len(episode_ids)
        print(f"Checking {total_episodes} episode IDs from {start_suffix} to {end_suffix}")
        
        for i, episode_id in enumerate(episode_ids, 1):
            url = f"https://www.bbc.co.uk/programmes/{episode_id}"
            
            print(f"[{i}/{total_episodes}] Checking {episode_id}...", end=" ")
            
            try:
                driver.get(url)
                
                # Quick check if page loads (don't wait too long for non-existent pages)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Extract metadata if page exists
                # The original code had extract_episode_metadata here, but it's not defined.
                # For now, we'll just save the episode_id for found pages.
                discovered_episodes.append({'episode_id': episode_id})
                print("✓ Found")
                    
            except TimeoutException:
                print("✗ Not an existing page")
            except Exception as e:
                print(f"✗ Error: {str(e)[:50]}")
            
            # Small delay to be respectful to BBC servers
            time.sleep(0.5)
            
    finally:
        driver.quit()
    
    return pd.DataFrame(discovered_episodes)


def discover_episodes_with_channel(start_suffix: str = "0000", end_suffix: str = "zzzz", step: int = 1):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    discovered_episodes = []
    try:
        episode_ids = list(generate_episode_ids(start_suffix, end_suffix))
        if step > 1:
            episode_ids = episode_ids[::step]
        total_episodes = len(episode_ids)
        print(f"Checking {total_episodes} episode IDs from {start_suffix} to {end_suffix}")
        for i, episode_id in enumerate(episode_ids, 1):
            url = f"https://www.bbc.co.uk/programmes/{episode_id}"
            print(f"[{i}/{total_episodes}] Checking {episode_id}...", end=" ")
            try:
                driver.get(url)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Parse channel from <title>
                title = driver.title
                channel = "Unknown Channel"
                if title and " - " in title:
                    parts = title.split(" - ", 1)
                    if len(parts) == 2:
                        channel = parts[0].strip()
                discovered_episodes.append({'episode_id': episode_id, 'channel': channel})
                print(f"✓ Found: [{channel}]")
            except TimeoutException:
                print("✗ Not an existing page")
            except Exception as e:
                print(f"✗ Error: {str(e)[:50]}")
            time.sleep(0.5)
    finally:
        driver.quit()
    return pd.DataFrame(discovered_episodes)


def discover_episodes_with_names(start_suffix: str = "0000", end_suffix: str = "zzzz", step: int = 1):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    driver = webdriver.Chrome(options=chrome_options)
    discovered_episodes = []
    try:
        episode_ids = list(generate_episode_ids(start_suffix, end_suffix))
        if step > 1:
            episode_ids = episode_ids[::step]
        total_episodes = len(episode_ids)
        print(f"Checking {total_episodes} episode IDs from {start_suffix} to {end_suffix}")
        for i, episode_id in enumerate(episode_ids, 1):
            url = f"https://www.bbc.co.uk/programmes/{episode_id}"
            print(f"[{i}/{total_episodes}] Checking {episode_id}...", end=" ")
            try:
                driver.get(url)
                WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                # Parse channel from <title>
                title = driver.title
                channel = "Unknown Channel"
                if title and " - " in title:
                    parts = title.split(" - ", 1)
                    if len(parts) == 2:
                        channel = parts[0].strip()
                # Extract show name (DJ name)
                show_name = "Unknown Show"
                try:
                    show_elem = driver.find_element(By.CSS_SELECTOR, "a.context__item")
                    show_name = show_elem.text.strip()
                except NoSuchElementException:
                    try:
                        show_elem = driver.find_element(By.CSS_SELECTOR, ".programme__title")
                        show_name = show_elem.text.strip()
                    except:
                        pass
                # Extract episode name
                episode_name = "Unknown Episode"
                try:
                    episode_elem = driver.find_element(By.CSS_SELECTOR, "h1.no-margin")
                    episode_name = episode_elem.text.strip()
                except NoSuchElementException:
                    try:
                        episode_elem = driver.find_element(By.CSS_SELECTOR, ".programme__title")
                        episode_name = episode_elem.text.strip()
                    except:
                        pass
                discovered_episodes.append({
                    'episode_id': episode_id, 
                    'channel': channel, 
                    'show_name': show_name, 
                    'episode_name': episode_name
                })
                print(f"✓ Found: [{channel}] {show_name} - {episode_name}")
            except TimeoutException:
                print("✗ Not an existing page")
            except Exception as e:
                print(f"✗ Error: {str(e)[:50]}")
            time.sleep(0.5)
    finally:
        driver.quit()
    return pd.DataFrame(discovered_episodes)


if __name__ == "__main__":
    # Example usage - start with a small range to test
    print("Starting BBC 6 Music episode discovery...")
    
    # Test with a small range first - based on user's findings
    # Current episodes seem to be in d### or f### range
    # Let's test a small range around where we know episodes exist
    test_df = discover_episodes_basic(start_suffix="d000", end_suffix="d010", step=1)
    
    if not test_df.empty:
        print(f"\nDiscovered {len(test_df)} episodes:")
        print(test_df)
        test_df.to_csv("discovered_episodes_test.csv", index=False)
    else:
        print("No episodes discovered in test range")
        
    # You can also test the full range systematically:
    # discover_episodes_batch(start_suffix="0000", end_suffix="zzzz", batch_size=1000) 