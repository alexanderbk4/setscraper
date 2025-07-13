"""
Track-level extraction for BBC 6 Music episodes.

This module handles extracting individual track information from episode pages.
"""

from selenium.webdriver.common.by import By
import pandas as pd


def extract_tracks_from_episode(driver, dj_name, episode_title, ep_id):
    """
    Extract all tracks from a loaded episode page.
    
    Args:
        driver: Selenium WebDriver instance with loaded episode page
        dj_name (str): Name of the DJ
        episode_title (str): Title of the episode
        
    Returns:
        pd.DataFrame: DataFrame containing all tracks with metadata
    """
    # Extract all music segments
    music_segments = driver.find_elements(By.CSS_SELECTOR, "li.segments-list__item--music")
    print(f"Found {len(music_segments)} music segments")
    
    tracks = []
    for segment in music_segments:
        try:
            track_data = extract_single_track(segment, dj_name, episode_title, ep_id)
            if track_data:
                tracks.append(track_data)
                
        except Exception as e:
            print(f"Error extracting track: {e}")
            continue
    
    return pd.DataFrame(tracks)


def extract_single_track(segment, dj_name, episode_title, ep_id):
    """
    Extract information from a single track segment.
    
    Args:
        segment: Selenium WebElement representing a track segment
        dj_name (str): Name of the DJ
        episode_title (str): Title of the episode
        
    Returns:
        dict: Track information including artist, title, featured artists
    """
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
        'episode_id': ep_id,
        'dj': dj_name,
        'episode': episode_title,
        'artist': artist,
        'title': title,
        'featured_artists': featured_artists if featured_artists else None,
    } 