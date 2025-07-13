"""
Track-level extraction for BBC 6 Music episodes.

This module handles extracting individual track information from episode pages.
"""

import pandas as pd
import time
import os
import json
from datetime import datetime
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException
from sqlalchemy import create_engine
from typing import List, Dict, Optional


def save_track_benchmark(start_time: float, end_time: float, episodes_processed: int, 
                        tracks_found: int, max_episodes: Optional[int] = None,
                        commit_id: str = None, notes: str = ""):
    """
    Save benchmark data for track scraping performance.
    
    Args:
        start_time: Start timestamp
        end_time: End timestamp
        episodes_processed: Number of episodes processed
        tracks_found: Number of tracks found
        max_episodes: Maximum episodes limit if used
        commit_id: Git commit ID for tracking
        notes: Additional notes about the run
    """
    duration = end_time - start_time
    episodes_per_second = episodes_processed / duration if duration > 0 else 0
    episodes_per_minute = episodes_per_second * 60
    tracks_per_second = tracks_found / duration if duration > 0 else 0
    tracks_per_episode = tracks_found / episodes_processed if episodes_processed > 0 else 0
    
    benchmark_data = {
        'timestamp': datetime.now().isoformat(),
        'start_time': start_time,
        'end_time': end_time,
        'duration_seconds': duration,
        'episodes_processed': episodes_processed,
        'tracks_found': tracks_found,
        'episodes_per_second': episodes_per_second,
        'episodes_per_minute': episodes_per_minute,
        'tracks_per_second': tracks_per_second,
        'tracks_per_episode': tracks_per_episode,
        'max_episodes': max_episodes,
        'commit_id': commit_id,
        'notes': notes
    }
    
    # Load existing benchmarks or create new file
    benchmark_file = os.path.join(os.path.dirname(__file__), 'track_scraping_benchmarks.json')
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
    
    print(f"\n📊 TRACK SCRAPING BENCHMARK SAVED:")
    print(f"   Duration: {duration:.2f} seconds")
    print(f"   Episodes processed: {episodes_processed}")
    print(f"   Tracks found: {tracks_found}")
    print(f"   Speed: {episodes_per_second:.3f} episodes/second ({episodes_per_minute:.1f}/minute)")
    print(f"   Track rate: {tracks_per_second:.3f} tracks/second")
    print(f"   Average tracks per episode: {tracks_per_episode:.1f}")
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


def print_track_benchmark_summary():
    """Print summary of all track scraping benchmarks."""
    benchmark_file = os.path.join(os.path.dirname(__file__), 'track_scraping_benchmarks.json')
    
    if not os.path.exists(benchmark_file):
        print("No track scraping benchmarks found.")
        return
    
    try:
        with open(benchmark_file, 'r') as f:
            benchmarks = json.load(f)
        
        if not benchmarks:
            print("No track scraping benchmarks found.")
            return
        
        print(f"\n📈 TRACK SCRAPING BENCHMARK SUMMARY ({len(benchmarks)} runs):")
        print("-" * 80)
        
        for i, bench in enumerate(benchmarks[-5:], 1):  # Show last 5 runs
            print(f"{i}. {bench['timestamp'][:19]} | "
                  f"{bench['episodes_processed']} episodes | "
                  f"{bench['tracks_found']} tracks | "
                  f"{bench['episodes_per_minute']:.1f} ep/min | "
                  f"{bench['tracks_per_episode']:.1f} tracks/ep | "
                  f"{bench['notes']}")
        
        # Calculate averages
        avg_episodes_per_min = sum(b['episodes_per_minute'] for b in benchmarks) / len(benchmarks)
        avg_tracks_per_episode = sum(b['tracks_per_episode'] for b in benchmarks) / len(benchmarks)
        total_episodes = sum(b['episodes_processed'] for b in benchmarks)
        total_tracks = sum(b['tracks_found'] for b in benchmarks)
        total_time = sum(b['duration_seconds'] for b in benchmarks)
        
        print("-" * 80)
        print(f"Average speed: {avg_episodes_per_min:.1f} episodes/minute")
        print(f"Average tracks per episode: {avg_tracks_per_episode:.1f}")
        print(f"Total episodes processed: {total_episodes}")
        print(f"Total tracks found: {total_tracks}")
        print(f"Total time: {total_time/3600:.1f} hours")
        
    except Exception as e:
        print(f"Error reading track scraping benchmarks: {e}")


def scrape_tracks_with_benchmark(episodes_df: pd.DataFrame, 
                                max_episodes: Optional[int] = None,
                                save_to_csv: bool = True,
                                benchmark: bool = True,
                                notes: str = "") -> pd.DataFrame:
    """
    Scrape tracks for episodes with benchmarking.
    
    Args:
        episodes_df: DataFrame containing episodes to process
        max_episodes: Maximum number of episodes to process (for testing)
        save_to_csv: Whether to save results to CSV
        benchmark: Whether to save benchmark data
        notes: Additional notes for benchmarking
        
    Returns:
        pd.DataFrame: Combined DataFrame of all tracks
    """
    start_time = time.time()
    commit_id = get_commit_id() if benchmark else None
    
    print(f"🎵 Starting track scraping benchmark...")
    if max_episodes:
        print(f"   Processing first {max_episodes} episodes")
    else:
        print(f"   Processing all {len(episodes_df)} episodes")
    
    # Perform the actual scraping
    tracks_df = scrape_tracks_for_episodes(episodes_df, max_episodes, save_to_csv)
    
    end_time = time.time()
    
    # Calculate metrics
    episodes_processed = len(episodes_df.head(max_episodes) if max_episodes else episodes_df)
    tracks_found = len(tracks_df) if not tracks_df.empty else 0
    
    # Save benchmark data
    if benchmark:
        save_track_benchmark(
            start_time=start_time,
            end_time=end_time,
            episodes_processed=episodes_processed,
            tracks_found=tracks_found,
            max_episodes=max_episodes,
            commit_id=commit_id,
            notes=notes
        )
    
    return tracks_df


def load_bbc6_episodes_from_db(db_url: str = "postgresql://setscraper:setscraper_password@localhost:5432/setscraper") -> pd.DataFrame:
    """
    Load BBC Radio 6 Music episodes from database.
    
    Args:
        db_url: Database connection URL
        
    Returns:
        pd.DataFrame: DataFrame containing BBC Radio 6 Music episodes
    """
    engine = create_engine(db_url)
    
    # Load episodes filtered for BBC Radio 6 Music and exclude recent episodes (last 48 hours)
    query = """
    SELECT episode_id, channel, show_name, episode_name, broadcast_date
    FROM episodes 
    WHERE channel = 'BBC Radio 6 Music'
    AND broadcast_date < NOW() - INTERVAL '48 hours'
    ORDER BY broadcast_date DESC
    """
    
    episodes_df = pd.read_sql(query, engine)
    print(f"Loaded {len(episodes_df)} BBC Radio 6 Music episodes from database (excluding last 48 hours)")
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

        # # Click "Show more" button if it exists to reveal all tracks
        # try:
        #     show_more = driver.find_element(By.CLASS_NAME, "ml__label--more")
        #     driver.execute_script("arguments[0].click();", show_more)
        #     time.sleep(2)
        #     print("  Clicked 'Show more' button")
        # except:
        #     print("  No 'Show more' button found")

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
    
    # Show previous benchmarks
    print_track_benchmark_summary()
    
    # Load episodes from database
    episodes_df = load_bbc6_episodes_from_db()
    
    if episodes_df.empty:
        print("No BBC Radio 6 Music episodes found in database")
        return
    
    # Scrape tracks with benchmarking (test with 50 episodes)
    tracks_df = scrape_tracks_with_benchmark(
        episodes_df, 
        max_episodes=50, 
        save_to_csv=True,
        benchmark=True,
        notes="50 episode benchmark test"
    )
    
    if not tracks_df.empty:
        print(f"\n📊 Scraping Results:")
        print(f"   Episodes processed: {tracks_df['episode_id'].nunique()}")
        print(f"   Total tracks: {len(tracks_df)}")
        print(f"   Unique artists: {tracks_df['artist'].nunique()}")
        
        # Show sample of tracks
        print(f"\n🎵 Sample tracks:")
        print(tracks_df[['track_order', 'artist', 'title', 'dj_name']].head(10))
    
    # Show updated benchmarks
    print_track_benchmark_summary()


if __name__ == "__main__":
    main() 