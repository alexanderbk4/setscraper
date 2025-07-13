"""
Data Pipeline for BBC 6 Music Scraping

This package contains modules for scraping BBC 6 Music episodes and tracks.
"""

from .episodes import scrape_bbc6_episode, analyze_multiple_djs
from .tracks import extract_tracks_from_episode
from .episode_discovery import (
    discover_episodes, 
    discover_episodes_batch, 
    discover_episodes_batch_ids,
    discover_episodes_parallel,
    discover_episodes_batch_parallel,
    discover_episodes_parallel_ids,
    save_benchmark,
    print_benchmark_summary,
    get_commit_id
)
from .csv_merge_clean import (
    find_csv_files,
    merge_csv_files,
    clean_episode_data,
    analyze_data,
    print_analysis,
    save_cleaned_data
)

__all__ = [
    'scrape_bbc6_episode',
    'analyze_multiple_djs', 
    'extract_tracks_from_episode',
    'discover_episodes',
    'discover_episodes_batch',
    'discover_episodes_batch_ids',
    'discover_episodes_parallel',
    'discover_episodes_batch_parallel',
    'discover_episodes_parallel_ids',
    'save_benchmark',
    'print_benchmark_summary',
    'get_commit_id',
    'find_csv_files',
    'merge_csv_files',
    'clean_episode_data',
    'analyze_data',
    'print_analysis',
    'save_cleaned_data'
] 