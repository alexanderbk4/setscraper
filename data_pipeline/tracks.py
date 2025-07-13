"""
Track-level extraction for BBC 6 Music episodes.

This module handles extracting individual track information from episode pages.
"""

import pandas as pd
import time
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from sqlalchemy import create_engine
from typing import List, Dict, Optional


def load_bbc6_episodes_from_db(db_url: str = "postgresql://setscraper:setscraper_password@localhost:5432/setscraper") -> pd.DataFrame:
    """
    Load BBC Radio 6 Music episodes from database.
    
    Args:
        db_url: Database connection URL
        
    Returns:
        pd.DataFrame: DataFrame containing BBC Radio 6 Music episodes
    """
    engine = create_engine(db_url)
    
    # Load episodes filtered for BBC Radio 6 Music
    query = """
    SELECT episode_id, channel, show_name, episode_name, broadcast_date
    FROM episodes 
    WHERE channel = 'BBC Radio 6 Music'
    ORDER BY broadcast_date DESC
    """
    
    episodes_df = pd.read_sql(query, engine)
    print(f"Loaded {len(episodes_df)} BBC Radio 6 Music episodes from database")
    return episodes_df


def scrape_tracks_for_episodes(episodes_df: pd.DataFrame, 
                              max_episodes: Optional[int] = None,
                              save_to_csv: bool = True) -> pd.DataFrame:
    """
    Scrape tracks for a list of episodes.
    
    Args:
        episodes_df: DataFrame containing episodes to process
        max_episodes: Maximum number of episodes to process (for testing)
        save_to_csv: Whether to save results to CSV
        
    Returns:
        pd.DataFrame: Combined DataFrame of all tracks
    """
    if max_episodes:
        episodes_df = episodes_df.head(max_episodes)
        print(f"Processing first {max_episodes} episodes for testing")
    
    all_tracks = []
    
    for idx, episode in episodes_df.iterrows():
        print(f"\nProcessing episode {idx + 1}/{len(episodes_df)}: {episode['episode_id']}")
        
        try:
            episode_tracks = scrape_tracks_for_single_episode(episode)
            if not episode_tracks.empty:
                all_tracks.append(episode_tracks)
                print(f"  ✓ Found {len(episode_tracks)} tracks")
            else:
                print(f"  ✗ No tracks found")
                
        except Exception as e:
            print(f"  ✗ Error processing episode: {e}")
            continue
    
    if all_tracks:
        combined_tracks = pd.concat(all_tracks, ignore_index=True)
        print(f"\nTotal tracks scraped: {len(combined_tracks)}")
        
        if save_to_csv:
            timestamp = time.strftime("%Y%m%d_%H%M%S")
            filename = f"scraped_tracks_{timestamp}.csv"
            combined_tracks.to_csv(filename, index=False)
            print(f"Saved tracks to: {filename}")
        
        return combined_tracks
    else:
        print("No tracks were scraped")
        return pd.DataFrame()


def scrape_tracks_for_single_episode(episode: pd.Series) -> pd.DataFrame:
    """
    Scrape tracks for a single episode.
    
    Args:
        episode: Series containing episode information
        
    Returns:
        pd.DataFrame: DataFrame containing tracks for this episode
    """
    episode_id = episode['episode_id']
    dj_name = episode['show_name']
    episode_title = episode['episode_name']
    
    # Construct BBC URL
    url = f"https://www.bbc.co.uk/programmes/{episode_id}"
    
    # Set up Chrome options
    chrome_options = Options()
    chrome_options.add_argument("--headless")  # Run in background
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
        print(f"  Successfully loaded: {url}")

        # Click "Show more" button if it exists to reveal all tracks
        try:
            show_more = driver.find_element(By.CLASS_NAME, "ml__label--more")
            driver.execute_script("arguments[0].click();", show_more)
            time.sleep(2)
            print("  Clicked 'Show more' button")
        except:
            print("  No 'Show more' button found")

        # Extract tracks with order tracking
        tracks_df = extract_tracks_from_episode_with_order(driver, dj_name, episode_title, episode_id)
        return tracks_df
        
    except TimeoutException:
        print(f"  Timed out waiting for page to load")
        return pd.DataFrame()
    
    finally:
        driver.quit()


def extract_tracks_from_episode_with_order(driver, dj_name: str, episode_title: str, episode_id: str) -> pd.DataFrame:
    """
    Extract all tracks from a loaded episode page with order tracking.
    
    Args:
        driver: Selenium WebDriver instance with loaded episode page
        dj_name: Name of the DJ
        episode_title: Title of the episode
        episode_id: BBC episode ID
        
    Returns:
        pd.DataFrame: DataFrame containing all tracks with metadata and order
    """
    # Extract all music segments
    music_segments = driver.find_elements(By.CSS_SELECTOR, "li.segments-list__item--music")
    print(f"  Found {len(music_segments)} music segments")
    
    tracks = []
    for order, segment in enumerate(music_segments, 1):
        try:
            track_data = extract_single_track_with_order(segment, dj_name, episode_title, episode_id, order)
            if track_data:
                tracks.append(track_data)
                
        except Exception as e:
            print(f"  Error extracting track {order}: {e}")
            continue
    
    return pd.DataFrame(tracks)


def extract_single_track_with_order(segment, dj_name: str, episode_title: str, episode_id: str, order: int) -> Optional[Dict]:
    """
    Extract information from a single track segment with order tracking.
    
    Args:
        segment: Selenium WebElement representing a track segment
        dj_name: Name of the DJ
        episode_title: Title of the episode
        episode_id: BBC episode ID
        order: Track order in the episode (1, 2, 3, etc.)
        
    Returns:
        dict: Track information including artist, title, featured artists, and order
    """
    try:
        # Extract artist
        artist_elem = segment.find_element(By.CLASS_NAME, "artist")
        artist = artist_elem.text if artist_elem else "Unknown Artist"
        
        # Extract track title
        title_container = segment.find_element(By.CSS_SELECTOR, "p.no-margin")
        title_span = title_container.find_element(By.TAG_NAME, "span")
        title = title_span.text if title_span else "Unknown Title"
        
        # Look for featured artists
        featured_artists = []
        try:
            # Find any additional artist spans after "feat."
            feat_artists = title_container.find_elements(By.CLASS_NAME, "artist")
            if len(feat_artists) > 0:
                featured_artists = [fa.text for fa in feat_artists]
        except:
            pass
        
        return {
            'episode_id': episode_id,
            'dj_name': dj_name,
            'episode_title': episode_title,
            'track_order': order,
            'artist': artist,
            'title': title,
            'featured_artists': featured_artists if featured_artists else None,
        }
        
    except Exception as e:
        print(f"    Error extracting track {order}: {e}")
        return None


def extract_tracks_from_episode(driver, dj_name, episode_title, ep_id):
    """
    Legacy function for backward compatibility.
    Extract all tracks from a loaded episode page.
    
    Args:
        driver: Selenium WebDriver instance with loaded episode page
        dj_name (str): Name of the DJ
        episode_title (str): Title of the episode
        
    Returns:
        pd.DataFrame: DataFrame containing all tracks with metadata
    """
    return extract_tracks_from_episode_with_order(driver, dj_name, episode_title, ep_id)


def extract_single_track(segment, dj_name, episode_title, ep_id):
    """
    Legacy function for backward compatibility.
    Extract information from a single track segment.
    
    Args:
        segment: Selenium WebElement representing a track segment
        dj_name (str): Name of the DJ
        episode_title (str): Title of the episode
        
    Returns:
        dict: Track information including artist, title, featured artists
    """
    # Use the new function with order=1 (legacy doesn't track order)
    return extract_single_track_with_order(segment, dj_name, episode_title, ep_id, 1)


def main():
    """Main function to run track scraping for BBC Radio 6 Music episodes."""
    print("🎵 BBC Radio 6 Music Track Scraper")
    print("=" * 50)
    
    # Load episodes from database
    episodes_df = load_bbc6_episodes_from_db()
    
    if episodes_df.empty:
        print("No BBC Radio 6 Music episodes found in database")
        return
    
    # Scrape tracks (start with a small number for testing)
    tracks_df = scrape_tracks_for_episodes(episodes_df, max_episodes=5, save_to_csv=True)
    
    if not tracks_df.empty:
        print(f"\n📊 Scraping Results:")
        print(f"   Episodes processed: {tracks_df['episode_id'].nunique()}")
        print(f"   Total tracks: {len(tracks_df)}")
        print(f"   Unique artists: {tracks_df['artist'].nunique()}")
        
        # Show sample of tracks
        print(f"\n🎵 Sample tracks:")
        print(tracks_df[['track_order', 'artist', 'title', 'dj_name']].head(10))


if __name__ == "__main__":
    main() 