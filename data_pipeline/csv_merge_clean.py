#!/usr/bin/env python3
"""
CSV Merge and Clean Script for BBC Episode Discovery

This script merges multiple CSV files from episode discovery runs,
removes duplicates, cleans the data, and provides statistics.
"""

import pandas as pd
import os
import glob
from datetime import datetime
import argparse
from typing import List, Dict, Optional


def find_csv_files(pattern: str = "discovered_episodes_*.csv") -> List[str]:
    """
    Find all CSV files matching the pattern.
    
    Args:
        pattern (str): Glob pattern to match CSV files
        
    Returns:
        List[str]: List of matching CSV file paths
    """
    csv_files = glob.glob(pattern)
    csv_files.sort()  # Sort for consistent ordering
    return csv_files


def load_csv_file(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load a CSV file with error handling.
    
    Args:
        file_path (str): Path to the CSV file
        
    Returns:
        pd.DataFrame or None: Loaded DataFrame or None if error
    """
    try:
        df = pd.read_csv(file_path)
        print(f"✓ Loaded {file_path}: {len(df)} rows")
        return df
    except Exception as e:
        print(f"✗ Error loading {file_path}: {e}")
        return None


def merge_csv_files(csv_files: List[str]) -> pd.DataFrame:
    """
    Merge multiple CSV files into a single DataFrame.
    
    Args:
        csv_files (List[str]): List of CSV file paths
        
    Returns:
        pd.DataFrame: Merged DataFrame
    """
    if not csv_files:
        print("No CSV files found!")
        return pd.DataFrame()
    
    print(f"Found {len(csv_files)} CSV files to merge:")
    for file in csv_files:
        print(f"  - {file}")
    
    # Load all CSV files
    dataframes = []
    for file_path in csv_files:
        df = load_csv_file(file_path)
        if df is not None and not df.empty:
            dataframes.append(df)
    
    if not dataframes:
        print("No valid CSV files found!")
        return pd.DataFrame()
    
    # Merge all DataFrames
    merged_df = pd.concat(dataframes, ignore_index=True)
    print(f"\nMerged {len(dataframes)} files into {len(merged_df)} total rows")
    
    return merged_df


def clean_episode_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean and standardize episode data.
    
    Args:
        df (pd.DataFrame): Raw episode DataFrame
        
    Returns:
        pd.DataFrame: Cleaned DataFrame
    """
    if df.empty:
        return df
    
    print(f"\nCleaning {len(df)} rows...")
    
    # Make a copy to avoid modifying original
    cleaned_df = df.copy()
    
    # Remove completely empty rows
    initial_rows = len(cleaned_df)
    cleaned_df = cleaned_df.dropna(how='all')
    if len(cleaned_df) < initial_rows:
        print(f"  - Removed {initial_rows - len(cleaned_df)} completely empty rows")
    
    # Clean episode_id column
    if 'episode_id' in cleaned_df.columns:
        # Remove any rows with invalid episode IDs
        cleaned_df = cleaned_df[cleaned_df['episode_id'].str.match(r'^m002[a-z0-9]{4}$', na=False)]
        print(f"  - Validated episode_id format: {len(cleaned_df)} rows")
    
    # Clean channel column
    if 'channel' in cleaned_df.columns:
        # Remove rows with unknown/empty channels
        cleaned_df = cleaned_df[cleaned_df['channel'] != 'Unknown Channel']
        cleaned_df = cleaned_df[cleaned_df['channel'].notna()]
        print(f"  - Cleaned channel data: {len(cleaned_df)} rows")
    
    # Clean show_name column
    if 'show_name' in cleaned_df.columns:
        # Remove rows with unknown/empty show names
        cleaned_df = cleaned_df[cleaned_df['show_name'] != 'Unknown Show']
        cleaned_df = cleaned_df[cleaned_df['show_name'].notna()]
        print(f"  - Cleaned show_name data: {len(cleaned_df)} rows")
    
    # Clean episode_name column
    if 'episode_name' in cleaned_df.columns:
        # Remove rows with unknown/empty episode names
        cleaned_df = cleaned_df[cleaned_df['episode_name'] != 'Unknown Episode']
        cleaned_df = cleaned_df[cleaned_df['episode_name'].notna()]
        print(f"  - Cleaned episode_name data: {len(cleaned_df)} rows")
    
    # Clean broadcast_date column
    if 'broadcast_date' in cleaned_df.columns:
        # Remove rows with unknown dates
        cleaned_df = cleaned_df[cleaned_df['broadcast_date'] != 'Unknown Date']
        cleaned_df = cleaned_df[cleaned_df['broadcast_date'].notna()]
        print(f"  - Cleaned broadcast_date data: {len(cleaned_df)} rows")
    
    # Remove duplicates based on episode_id
    initial_rows = len(cleaned_df)
    cleaned_df = cleaned_df.drop_duplicates(subset=['episode_id'], keep='first')
    if len(cleaned_df) < initial_rows:
        print(f"  - Removed {initial_rows - len(cleaned_df)} duplicate episode_ids")
    
    # Remove rows that are identical except for episode_id
    initial_rows = len(cleaned_df)
    # Get all columns except episode_id for duplicate detection
    duplicate_columns = [col for col in cleaned_df.columns if col != 'episode_id']
    if duplicate_columns:
        cleaned_df = cleaned_df.drop_duplicates(subset=duplicate_columns, keep='first')
        if len(cleaned_df) < initial_rows:
            print(f"  - Removed {initial_rows - len(cleaned_df)} rows identical except episode_id")
    
    print(f"  - Final cleaned dataset: {len(cleaned_df)} rows")
    
    return cleaned_df


def analyze_data(df: pd.DataFrame) -> Dict:
    """
    Analyze the cleaned dataset and provide statistics.
    
    Args:
        df (pd.DataFrame): Cleaned episode DataFrame
        
    Returns:
        Dict: Analysis results
    """
    if df.empty:
        return {}
    
    analysis = {
        'total_episodes': len(df),
        'unique_channels': df['channel'].nunique() if 'channel' in df.columns else 0,
        'unique_shows': df['show_name'].nunique() if 'show_name' in df.columns else 0,
        'date_range': None,
        'top_channels': [],
        'top_shows': []
    }
    
    # Date range analysis
    if 'broadcast_date' in df.columns:
        try:
            # Try to parse dates (assuming ISO format)
            dates = pd.to_datetime(df['broadcast_date'], errors='coerce')
            valid_dates = dates.dropna()
            if len(valid_dates) > 0:
                analysis['date_range'] = {
                    'earliest': valid_dates.min().strftime('%Y-%m-%d'),
                    'latest': valid_dates.max().strftime('%Y-%m-%d'),
                    'total_days': (valid_dates.max() - valid_dates.min()).days
                }
        except:
            pass
    
    # Top channels
    if 'channel' in df.columns:
        channel_counts = df['channel'].value_counts().head(10)
        analysis['top_channels'] = channel_counts.to_dict()
    
    # Top shows
    if 'show_name' in df.columns:
        show_counts = df['show_name'].value_counts().head(10)
        analysis['top_shows'] = show_counts.to_dict()
    
    return analysis


def print_analysis(analysis: Dict):
    """
    Print the analysis results in a formatted way.
    
    Args:
        analysis (Dict): Analysis results
    """
    if not analysis:
        print("No data to analyze.")
        return
    
    print(f"\n📊 DATASET ANALYSIS:")
    print("=" * 50)
    print(f"Total Episodes: {analysis['total_episodes']:,}")
    print(f"Unique Channels: {analysis['unique_channels']}")
    print(f"Unique Shows: {analysis['unique_shows']}")
    
    if analysis['date_range']:
        print(f"\n📅 Date Range:")
        print(f"  Earliest: {analysis['date_range']['earliest']}")
        print(f"  Latest: {analysis['date_range']['latest']}")
        print(f"  Span: {analysis['date_range']['total_days']} days")
    
    if analysis['top_channels']:
        print(f"\n📻 Top Channels:")
        for channel, count in list(analysis['top_channels'].items())[:5]:
            print(f"  {channel}: {count:,} episodes")
    
    if analysis['top_shows']:
        print(f"\n🎵 Top Shows:")
        for show, count in list(analysis['top_shows'].items())[:5]:
            print(f"  {show}: {count:,} episodes")


def save_cleaned_data(df: pd.DataFrame, output_file: str = None):
    """
    Save the cleaned data to a CSV file.
    
    Args:
        df (pd.DataFrame): Cleaned DataFrame
        output_file (str): Output file path (optional)
    """
    if df.empty:
        print("No data to save.")
        return
    
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = f"cleaned_episodes_{timestamp}.csv"
    
    try:
        df.to_csv(output_file, index=False)
        print(f"\n💾 Saved cleaned data to: {output_file}")
        print(f"   File size: {os.path.getsize(output_file) / 1024:.1f} KB")
    except Exception as e:
        print(f"✗ Error saving file: {e}")


def main():
    """Main function to run the CSV merge and clean process."""
    parser = argparse.ArgumentParser(description='Merge and clean BBC episode discovery CSV files')
    parser.add_argument('--pattern', default='discovered_episodes_*.csv', 
                       help='Pattern to match CSV files (default: discovered_episodes_*.csv)')
    parser.add_argument('--output', help='Output file name (default: auto-generated)')
    parser.add_argument('--no-clean', action='store_true', 
                       help='Skip data cleaning step')
    parser.add_argument('--no-save', action='store_true', 
                       help='Skip saving output file')
    
    args = parser.parse_args()
    
    print("🔄 BBC Episode Discovery - CSV Merge & Clean")
    print("=" * 50)
    
    # Find CSV files
    csv_files = find_csv_files(args.pattern)
    
    if not csv_files:
        print(f"No CSV files found matching pattern: {args.pattern}")
        return
    
    # Merge CSV files
    merged_df = merge_csv_files(csv_files)
    
    if merged_df.empty:
        print("No data to process.")
        return
    
    # Clean data (unless --no-clean is specified)
    if not args.no_clean:
        cleaned_df = clean_episode_data(merged_df)
    else:
        cleaned_df = merged_df
        print("\n⚠️  Skipping data cleaning (--no-clean specified)")
    
    # Analyze data
    analysis = analyze_data(cleaned_df)
    print_analysis(analysis)
    
    # Save cleaned data (unless --no-save is specified)
    if not args.no_save:
        save_cleaned_data(cleaned_df, args.output)
    
    print(f"\n✅ Process completed successfully!")
    print(f"   Input files: {len(csv_files)}")
    print(f"   Total rows: {len(merged_df):,}")
    print(f"   Cleaned rows: {len(cleaned_df):,}")


if __name__ == "__main__":
    main() 