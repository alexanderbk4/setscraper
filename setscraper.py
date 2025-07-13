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

        # Click "Show more" button if it exists
        try:
            show_more = driver.find_element(By.CLASS_NAME, "ml__label--more")
            driver.execute_script("arguments[0].click();", show_more)
            time.sleep(2)
            print("Clicked 'Show more' button")
        except:
            print("No 'Show more' button found")

        # Extract all music segments
        music_segments = driver.find_elements(By.CSS_SELECTOR, "li.segments-list__item--music")
        print(f"Found {len(music_segments)} music segments")
        
        tracks = []
        for segment in music_segments:
            try:
                # Extract artist
                artist_elem = segment.find_element(By.CLASS_NAME, "artist")
                artist = artist_elem.text if artist_elem else "Unknown Artist"
                
                # Extract track title (improved)
                title_container = segment.find_element(By.CSS_SELECTOR, "p.no-margin")
                title_span = title_container.find_element(By.TAG_NAME, "span")
                title = title_span.text if title_span else "Unknown Title"
                
                tracks.append({
                    'artist': artist,
                    'title': title,
                })
                
            except Exception as e:
                print(f"Error extracting track: {e}")
                continue
        
        return tracks
        
    except TimeoutException:
        print("Timed out waiting for page to load")
        return []
    
    finally:
        driver.quit()

# Test with one URL
url = "https://www.bbc.co.uk/programmes/m002845d"
