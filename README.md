# Setscraper

> Analyzing musical patterns in DJ hosts and DJ sets via machine learning

## Overview

Each DJ has unique musical tastes, and thus the set of tracks they play will also be unique, and have a unique musical fingerprint. 

The aim of this project is to use machine learning to identify the musical fingerprint of each DJ, and to translate that into digestible insights:
- Does this DJ prefer certain genres?
- Is there a time period that is referenced a lot?
- How danceable are the tunes?
- What is the favorite day of the week of this DJ?
- Does the DJ make eclectic changes, or gradual ones?

This project will start out with analyzing DJ sets on BBC6 music. This is done, because they offer a wide range of hosts, with differing but all very alternative musical tastes. BBC6 is my go-to radio station, in the morning or in the evening, during weekdays or in the weekend, because the hosts are incredibly enthuasistic, seemingly have the freedom to play (mostly) whatever they want, and are all very well versed in a wide variety of genres. Because of the BBCs documentation of the sets that have been played in the recent weeks, it also provides an ideal scenario for music scraping.

In this project we will: scrape musical playlists, clean that data for most important characteristics, check if the song can be matched to a Spotify entry, analyze musical characteristics using the Spotify API, and use machine learning to discover patterns in host preferences, music evolution, and recommendation opportunities.

## Why This Project?

**For Music Lovers**: Discover new music based on DJ similarity and predict what you'll love next

**For ML Engineers**: Real-world application of clustering, recommendation systems, and time-series analysis

**For Me**: Combining my love of BBC6 Music with machine learning to build something genuinely useful

## Possible Research Questions To Answer

- **Musical DNA**: Can we quantify each DJ's unique "musical fingerprint" using audio features?
- **Host Similarity**: Which DJs have the most/least similar musical tastes?
- **Temporal Patterns**: Do DJs have time-based preferences (morning vs evening energy)?
- **Transition Analysis**: Can we predict the next track based on current track features?
- **Genre Analysis**: How do we measure and visualize genre boundaries across hosts?

## Current Status

🟡 **Phase 1: Data Collection** (In Progress)
- [ ] BBC Sounds scraper for tracklists
- [ ] Basic data extraction and cleaning
- [ ] Database storage
- [ ] Track coupling with Spotify
- [ ] Data ingestion pipeline set-up

⚪ **Phase 2: Analysis & ML** (Planned)
- [ ] Track feature engineering via Spotify API
- [ ] Host clustering and similarity analysis  
- [ ] Temporal pattern analysis
- [ ] Recommendation engine development
- [ ] Performance metrics and validation

⚪ **Phase 3: Dashboard & API** (Planned)
- [ ] Interactive web dashboard
- [ ] Host and set characteristics
- [ ] Music recommendations based on host and set

### Proposed Tech Stack

- **Data Collection**: Python, BeautifulSoup
- **APIs**: Spotify Web API for audio features
- **Database**: PostgreSQL
- **ML/Analytics**: scikit-learn, pandas, NumPy, PyTorch
- **Backend**: FastAPI, async processing
- **Frontend**: React, D3.js for visualizations
- **Deployment**: Docker, CI/CD with GitHub Actions

## Installation & Usage

This project uses [uv](https://github.com/astral-sh/uv) as the package manager instead of pip for faster dependency resolution and installation.

```bash
# Install uv (if not already installed)
curl -LsSf https://astral.sh/uv/install.sh | sh

# Clone and setup
git clone https://github.com/alexanderbk4/setscraper.git
cd setscraper

# Install dependencies  
uv sync

# Setup virtual environment
uv sync
source .venv/bin/activate
```
