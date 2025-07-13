"""
Episode discovery for BBC 6 Music.

This module systematically checks BBC URLs to find working episodes and extract metadata.
"""

import time
import json
import os
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import pandas as pd
from typing import List, Dict, Optional


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


def discover_episodes(start_suffix: str = "0000", end_suffix: str = "zzzz", step: int = 1, 
                     benchmark: bool = True, notes: str = ""):
    """
    Discover BBC episodes by checking URL patterns and extracting metadata.
    
    Args:
        start_suffix (str): Starting 4-character suffix
        end_suffix (str): Ending 4-character suffix
        step (int): Step size for incrementing (use 1 for full scan)
        benchmark (bool): Whether to save benchmark data
        notes (str): Additional notes for benchmarking
        
    Returns:
        pd.DataFrame: DataFrame with episode_id, channel, show_name, episode_name, broadcast_date
    """
    start_time = time.time()
    commit_id = get_commit_id() if benchmark else None
    
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run headless for discovery
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    # Speed optimizations
    chrome_options.add_argument("--disable-images")  # Don't load images
    chrome_options.add_argument("--disable-javascript")  # Disable JS for faster loading
    chrome_options.add_argument("--disable-css")  # Disable CSS
    chrome_options.add_argument("--disable-plugins")  # Disable plugins
    chrome_options.add_argument("--disable-extensions")  # Disable extensions
    chrome_options.add_argument("--disable-logging")  # Disable logging
    chrome_options.add_argument("--disable-default-apps")  # Disable default apps
    chrome_options.add_argument("--disable-sync")  # Disable sync
    chrome_options.add_argument("--disable-translate")  # Disable translate
    chrome_options.add_argument("--disable-web-security")  # Disable web security
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")  # Disable compositor
    chrome_options.add_argument("--memory-pressure-off")  # Reduce memory pressure
    chrome_options.add_argument("--max_old_space_size=4096")  # Increase memory limit
    
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
                
                # Faster timeout for non-existent pages
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Extract metadata if page exists
                episode_data = extract_episode_metadata(driver, episode_id)
                if episode_data:
                    discovered_episodes.append(episode_data)
                    print(f"✓ Found: [{episode_data['channel']}] {episode_data['show_name']} - {episode_data['episode_name']} ({episode_data['broadcast_date']})")
                else:
                    print("✗ No metadata found")
                    
            except TimeoutException:
                print("✗ Not an existing page")
            except Exception as e:
                print(f"✗ Error: {str(e)[:50]}")
            
            # Reduced delay for faster processing
            time.sleep(0.1)
            
    finally:
        driver.quit()
    
    end_time = time.time()
    
    # Save benchmark data
    if benchmark:
        save_benchmark(
            start_time=start_time,
            end_time=end_time,
            episodes_processed=total_episodes,
            start_suffix=start_suffix,
            end_suffix=end_suffix,
            commit_id=commit_id,
            notes=notes
        )
    
    return pd.DataFrame(discovered_episodes)


def extract_episode_metadata(driver, episode_id: str) -> Optional[Dict]:
    """
    Extract episode metadata from a working BBC page.
    
    Args:
        driver: Selenium WebDriver instance
        episode_id (str): The episode ID
        
    Returns:
        dict: Episode metadata or None if extraction fails
    """
    try:
        # Parse title to get channel
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
            # Try alternative selectors
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
        
        # Extract broadcast date
        broadcast_date = "Unknown Date"
        try:
            date_elem = driver.find_element(By.CSS_SELECTOR, ".broadcast-event__time")
            # Try to get the content attribute first (ISO format)
            date_content = date_elem.get_attribute("content")
            if date_content:
                broadcast_date = date_content
            else:
                # Fallback to title attribute (human readable)
                date_title = date_elem.get_attribute("title")
                if date_title:
                    broadcast_date = date_title
        except NoSuchElementException:
            pass
        
        # Only return if we found meaningful data
        if show_name != "Unknown Show" or episode_name != "Unknown Episode":
            return {
                'episode_id': episode_id,
                'channel': channel,
                'show_name': show_name,
                'episode_name': episode_name,
                'broadcast_date': broadcast_date
            }
        
        return None
        
    except Exception as e:
        print(f"Error extracting metadata: {e}")
        return None


def discover_episodes_batch(start_suffix: str = "0000", end_suffix: str = "zzzz", batch_size: int = 1000,
                           benchmark: bool = True, notes: str = ""):
    """
    Discover episodes in batches to avoid memory issues.
    
    Args:
        start_suffix (str): Starting 4-character suffix
        end_suffix (str): Ending 4-character suffix
        batch_size (int): Number of episodes to process per batch
        benchmark (bool): Whether to save benchmark data
        notes (str): Additional notes for benchmarking
        
    Returns:
        pd.DataFrame: Combined results from all batches
    """
    start_time = time.time()
    commit_id = get_commit_id() if benchmark else None
    
    all_results = []
    
    # Generate all episode IDs first
    all_episode_ids = list(generate_episode_ids(start_suffix, end_suffix))
    total_episodes = len(all_episode_ids)
    
    print(f"Total episodes to check: {total_episodes}")
    
    for i in range(0, total_episodes, batch_size):
        batch_end_idx = min(i + batch_size, total_episodes)
        batch_episode_ids = all_episode_ids[i:batch_end_idx]
        
        batch_start_suffix = batch_episode_ids[0][4:]  # Remove 'm002' prefix
        batch_end_suffix = batch_episode_ids[-1][4:]
        
        print(f"\n=== Processing batch {batch_start_suffix} to {batch_end_suffix} ===")
        
        batch_df = discover_episodes_batch_ids(batch_episode_ids)
        if not batch_df.empty:
            all_results.append(batch_df)
            print(f"Found {len(batch_df)} episodes in this batch")
        
        # Save intermediate results
        if all_results:
            combined_df = pd.concat(all_results, ignore_index=True)
            combined_df.to_csv(f"discovered_episodes_{batch_start_suffix}_{batch_end_suffix}.csv", index=False)
            print(f"Saved {len(combined_df)} episodes to CSV")
    
    end_time = time.time()
    
    # Save benchmark data
    if benchmark:
        save_benchmark(
            start_time=start_time,
            end_time=end_time,
            episodes_processed=total_episodes,
            start_suffix=start_suffix,
            end_suffix=end_suffix,
            batch_size=batch_size,
            commit_id=commit_id,
            notes=notes
        )
    
    if all_results:
        return pd.concat(all_results, ignore_index=True)
    else:
        return pd.DataFrame()


def discover_episodes_batch_ids(episode_ids: List[str]):
    """
    Discover episodes from a list of episode IDs.
    
    Args:
        episode_ids (List[str]): List of episode IDs to check
        
    Returns:
        pd.DataFrame: DataFrame with discovered episodes
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    # Speed optimizations
    chrome_options.add_argument("--disable-images")  # Don't load images
    chrome_options.add_argument("--disable-javascript")  # Disable JS for faster loading
    chrome_options.add_argument("--disable-css")  # Disable CSS
    chrome_options.add_argument("--disable-plugins")  # Disable plugins
    chrome_options.add_argument("--disable-extensions")  # Disable extensions
    chrome_options.add_argument("--disable-logging")  # Disable logging
    chrome_options.add_argument("--disable-default-apps")  # Disable default apps
    chrome_options.add_argument("--disable-sync")  # Disable sync
    chrome_options.add_argument("--disable-translate")  # Disable translate
    chrome_options.add_argument("--disable-web-security")  # Disable web security
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")  # Disable compositor
    chrome_options.add_argument("--memory-pressure-off")  # Reduce memory pressure
    chrome_options.add_argument("--max_old_space_size=4096")  # Increase memory limit
    
    driver = webdriver.Chrome(options=chrome_options)
    discovered_episodes = []
    
    try:
        total_episodes = len(episode_ids)
        
        for i, episode_id in enumerate(episode_ids, 1):
            url = f"https://www.bbc.co.uk/programmes/{episode_id}"
            
            print(f"[{i}/{total_episodes}] Checking {episode_id}...", end=" ")
            
            try:
                driver.get(url)
                
                # Faster timeout for non-existent pages
                WebDriverWait(driver, 3).until(
                    EC.presence_of_element_located((By.TAG_NAME, "body"))
                )
                
                # Extract metadata if page exists
                episode_data = extract_episode_metadata(driver, episode_id)
                if episode_data:
                    discovered_episodes.append(episode_data)
                    print(f"✓ Found: [{episode_data['channel']}] {episode_data['show_name']} - {episode_data['episode_name']} ({episode_data['broadcast_date']})")
                else:
                    print("✗ No metadata found")
                    
            except TimeoutException:
                print("✗ Not an existing page")
            except Exception as e:
                print(f"✗ Error: {str(e)[:50]}")
            
            # Reduced delay for faster processing
            time.sleep(0.1)
            
    finally:
        driver.quit()
    
    return pd.DataFrame(discovered_episodes)


def save_benchmark(start_time: float, end_time: float, episodes_processed: int, 
                  start_suffix: str, end_suffix: str, batch_size: int = None, 
                  commit_id: str = None, notes: str = ""):
    """
    Save benchmark data to track performance improvements.
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp
        episodes_processed: Number of episodes processed
        start_suffix: Starting suffix range
        end_suffix: Ending suffix range
        batch_size: Batch size if using batch processing
        commit_id: Git commit ID for tracking
        notes: Additional notes about the run
    """
    duration = end_time - start_time
    episodes_per_second = episodes_processed / duration if duration > 0 else 0
    episodes_per_minute = episodes_per_second * 60
    
    benchmark_data = {
        'timestamp': datetime.now().isoformat(),
        'start_time': start_time,
        'end_time': end_time,
        'duration_seconds': duration,
        'episodes_processed': episodes_processed,
        'episodes_per_second': episodes_per_second,
        'episodes_per_minute': episodes_per_minute,
        'start_suffix': start_suffix,
        'end_suffix': end_suffix,
        'batch_size': batch_size,
        'commit_id': commit_id,
        'notes': notes
    }
    
    # Load existing benchmarks or create new file
    benchmark_file = 'episode_discovery_benchmarks.json'
    benchmarks = []
    
    if os.path.exists(benchmark_file):
        try:
            with open(benchmark_file, 'r') as f:
                benchmarks = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            benchmarks = []
    
    benchmarks.append(benchmark_data)
    
    # Save updated benchmarks
    with open(benchmark_file, 'w') as f:
        json.dump(benchmarks, f, indent=2)
    
    print(f"\n📊 BENCHMARK SAVED:")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   Episodes processed: {episodes_processed}")
    print(f"   Speed: {episodes_per_second:.3f} episodes/second ({episodes_per_minute:.1f}/minute)")
    print(f"   Benchmark saved to: {benchmark_file}")


def get_commit_id():
    """Get current git commit ID for benchmarking."""
    try:
        import subprocess
        result = subprocess.run(['git', 'rev-parse', 'HEAD'], 
                              capture_output=True, text=True, check=True)
        return result.stdout.strip()[:8]  # First 8 characters
    except:
        return "unknown"


def print_benchmark_summary():
    """Print summary of all benchmarks."""
    benchmark_file = 'episode_discovery_benchmarks.json'
    
    if not os.path.exists(benchmark_file):
        print("No benchmarks found.")
        return
    
    try:
        with open(benchmark_file, 'r') as f:
            benchmarks = json.load(f)
        
        if not benchmarks:
            print("No benchmarks found.")
            return
        
        print(f"\n📈 BENCHMARK SUMMARY ({len(benchmarks)} runs):")
        print("-" * 60)
        
        for i, bench in enumerate(benchmarks[-5:], 1):  # Show last 5 runs
            print(f"{i}. {bench['timestamp'][:19]} | "
                  f"{bench['episodes_processed']} episodes | "
                  f"{bench['episodes_per_minute']:.1f}/min | "
                  f"{bench['notes']}")
        
        # Calculate averages
        avg_speed = sum(b['episodes_per_minute'] for b in benchmarks) / len(benchmarks)
        total_episodes = sum(b['episodes_processed'] for b in benchmarks)
        total_time = sum(b['duration_seconds'] for b in benchmarks)
        
        print("-" * 60)
        print(f"Average speed: {avg_speed:.1f} episodes/minute")
        print(f"Total episodes processed: {total_episodes}")
        print(f"Total time: {total_time/3600:.1f} hours")
        
    except Exception as e:
        print(f"Error reading benchmarks: {e}")


if __name__ == "__main__":
    # Example usage - start with a small range to test
    print("Starting BBC 6 Music episode discovery...")
    
    # Show previous benchmarks
    print_benchmark_summary()
    
    # Test with a small range first - based on user's findings
    # Current episodes seem to be in d### or f### range
    # Let's test a small range around where we know episodes exist
    test_df = discover_episodes(
        start_suffix="d000", 
        end_suffix="d010", 
        step=1,
        benchmark=True,
        notes="Test run - small range"
    )
    
    if not test_df.empty:
        print(f"\nDiscovered {len(test_df)} episodes:")
        print(test_df)
        test_df.to_csv("discovered_episodes_test.csv", index=False)
    else:
        print("No episodes discovered in test range")
        
    # Show updated benchmarks
    print_benchmark_summary()
        
    # You can also test the full range systematically:
    # discover_episodes_batch(start_suffix="0000", end_suffix="zzzz", batch_size=1000) 