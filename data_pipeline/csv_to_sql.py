#!/usr/bin/env python3
"""
CSV to SQL Processor for Setscraper

This module handles loading cleaned CSV files into PostgreSQL database
with proper file management and error handling.
"""

import os
import shutil
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, IntegrityError


class CSVProcessor:
    """
    Handles CSV file processing and database loading with file management.
    """
    
    def __init__(self, 
                 db_url: str = "postgresql://setscraper:setscraper_password@localhost:5432/setscraper",
                 base_dir: str = "data/processed/episodes",
                 log_level: str = "INFO"):
        """
        Initialize the CSV processor.
        
        Args:
            db_url: Database connection URL
            base_dir: Base directory for file management
            log_level: Logging level
        """
        self.db_url = db_url
        self.base_dir = Path(base_dir)
        
        # Setup directories
        self.pending_dir = self.base_dir / "pending"
        self.processed_dir = self.base_dir / "processed"
        self.failed_dir = self.base_dir / "failed"
        
        # Create directories if they don't exist
        for directory in [self.pending_dir, self.processed_dir, self.failed_dir]:
            directory.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging(log_level)
        
        # Setup database connection
        self.engine = create_engine(db_url)
        
        self.logger.info(f"CSV Processor initialized")
        self.logger.info(f"Pending directory: {self.pending_dir}")
        self.logger.info(f"Processed directory: {self.processed_dir}")
        self.logger.info(f"Failed directory: {self.failed_dir}")
    
    def _setup_logging(self, log_level: str):
        """Setup logging configuration."""
        logging.basicConfig(
            level=getattr(logging, log_level.upper()),
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('csv_processor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)
    
    def get_pending_files(self) -> List[Path]:
        """Get list of CSV files in pending directory."""
        csv_files = list(self.pending_dir.glob("*.csv"))
        self.logger.info(f"Found {len(csv_files)} pending CSV files")
        return csv_files
    
    def process_file(self, file_path: Path) -> bool:
        """
        Process a single CSV file and load it into the database.
        
        Args:
            file_path: Path to the CSV file
            
        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info(f"Processing file: {file_path.name}")
        
        try:
            # Load CSV file
            df = self._load_csv(file_path)
            if df is None:
                return False
            
            # Validate data
            if not self._validate_data(df):
                return False
            
            # Load into database
            if not self._load_to_database(df, file_path.name):
                return False
            
            # Move file to processed directory
            self._move_file(file_path, self.processed_dir)
            
            self.logger.info(f"Successfully processed {file_path.name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error processing {file_path.name}: {str(e)}")
            self._move_file(file_path, self.failed_dir)
            return False
    
    def _load_csv(self, file_path: Path) -> Optional[pd.DataFrame]:
        """Load CSV file with error handling."""
        try:
            df = pd.read_csv(file_path)
            self.logger.info(f"Loaded {len(df)} rows from {file_path.name}")
            return df
        except Exception as e:
            self.logger.error(f"Error loading CSV {file_path.name}: {str(e)}")
            return None
    
    def _validate_data(self, df: pd.DataFrame) -> bool:
        """Validate that the DataFrame has the expected structure."""
        required_columns = ['episode_id', 'channel', 'show_name', 'episode_name', 'broadcast_date']
        
        # Check for required columns
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            self.logger.error(f"Missing required columns: {missing_columns}")
            return False
        
        # Check for empty DataFrame
        if df.empty:
            self.logger.warning("DataFrame is empty")
            return False
        
        # Check for duplicate episode_ids
        duplicates = df['episode_id'].duplicated().sum()
        if duplicates > 0:
            self.logger.warning(f"Found {duplicates} duplicate episode_ids, removing them")
            df.drop_duplicates(subset=['episode_id'], keep='first', inplace=True)
        
        # Validate episode_id format
        invalid_episodes = df[~df['episode_id'].str.match(r'^m002[a-z0-9]{4}$', na=False)]
        if len(invalid_episodes) > 0:
            self.logger.warning(f"Found {len(invalid_episodes)} invalid episode_ids, removing them")
            df = df[df['episode_id'].str.match(r'^m002[a-z0-9]{4}$', na=False)]
        
        self.logger.info(f"Data validation passed: {len(df)} valid rows")
        return True
    
    def _load_to_database(self, df: pd.DataFrame, filename: str) -> bool:
        """Load DataFrame into PostgreSQL database."""
        try:
            # Convert broadcast_date to datetime if it's a string
            if 'broadcast_date' in df.columns and df['broadcast_date'].dtype == 'object':
                df['broadcast_date'] = pd.to_datetime(df['broadcast_date'], errors='coerce')
            
            # Load to database
            df.to_sql(
                'episodes', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'  # Faster for large datasets
            )
            
            self.logger.info(f"Successfully loaded {len(df)} rows to database from {filename}")
            return True
            
        except IntegrityError as e:
            self.logger.error(f"Integrity error loading {filename}: {str(e)}")
            # Handle duplicate key errors
            if "duplicate key" in str(e).lower():
                self.logger.info("Attempting to handle duplicates...")
                return self._handle_duplicates(df, filename)
            return False
            
        except SQLAlchemyError as e:
            self.logger.error(f"Database error loading {filename}: {str(e)}")
            return False
    
    def _handle_duplicates(self, df: pd.DataFrame, filename: str) -> bool:
        """Handle duplicate episode_ids by updating existing records."""
        try:
            # Get existing episode_ids from database
            existing_query = "SELECT episode_id FROM episodes"
            existing_df = pd.read_sql(existing_query, self.engine)
            
            # Filter out duplicates
            new_df = df[~df['episode_id'].isin(existing_df['episode_id'])]
            
            if len(new_df) == 0:
                self.logger.info(f"All episodes in {filename} already exist in database")
                return True
            
            # Load only new episodes
            new_df.to_sql(
                'episodes', 
                self.engine, 
                if_exists='append', 
                index=False,
                method='multi'
            )
            
            self.logger.info(f"Loaded {len(new_df)} new episodes from {filename} (skipped {len(df) - len(new_df)} duplicates)")
            return True
            
        except Exception as e:
            self.logger.error(f"Error handling duplicates for {filename}: {str(e)}")
            return False
    
    def _move_file(self, file_path: Path, destination_dir: Path):
        """Move file to destination directory with timestamp."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        new_name = f"{file_path.stem}_{timestamp}{file_path.suffix}"
        destination = destination_dir / new_name
        
        shutil.move(str(file_path), str(destination))
        self.logger.info(f"Moved {file_path.name} to {destination}")
    
    def process_all_pending(self) -> Dict[str, int]:
        """
        Process all pending CSV files.
        
        Returns:
            Dict with processing statistics
        """
        pending_files = self.get_pending_files()
        
        if not pending_files:
            self.logger.info("No pending files to process")
            return {"processed": 0, "failed": 0, "total": 0}
        
        stats = {"processed": 0, "failed": 0, "total": len(pending_files)}
        
        for file_path in pending_files:
            if self.process_file(file_path):
                stats["processed"] += 1
            else:
                stats["failed"] += 1
        
        self.logger.info(f"Processing complete: {stats['processed']} successful, {stats['failed']} failed")
        return stats
    
    def get_database_stats(self) -> Dict[str, int]:
        """Get statistics about the database."""
        try:
            with self.engine.connect() as conn:
                # Get total episodes
                result = conn.execute(text("SELECT COUNT(*) FROM episodes"))
                total_episodes = result.scalar()
                
                # Get unique shows
                result = conn.execute(text("SELECT COUNT(DISTINCT show_name) FROM episodes"))
                unique_shows = result.scalar()
                
                # Get date range
                result = conn.execute(text("SELECT MIN(broadcast_date), MAX(broadcast_date) FROM episodes WHERE broadcast_date IS NOT NULL"))
                date_range = result.fetchone()
                
                stats = {
                    "total_episodes": total_episodes,
                    "unique_shows": unique_shows,
                    "earliest_date": date_range[0] if date_range[0] else None,
                    "latest_date": date_range[1] if date_range[1] else None
                }
                
                self.logger.info(f"Database stats: {stats}")
                return stats
                
        except Exception as e:
            self.logger.error(f"Error getting database stats: {str(e)}")
            return {}


def main():
    """Main function to run the CSV processor."""
    processor = CSVProcessor()
    
    print("🔄 CSV to SQL Processor")
    print("=" * 50)
    
    # Process all pending files
    stats = processor.process_all_pending()
    
    # Show database statistics
    db_stats = processor.get_database_stats()
    
    print(f"\n📊 Processing Results:")
    print(f"   Processed: {stats['processed']}")
    print(f"   Failed: {stats['failed']}")
    print(f"   Total: {stats['total']}")
    
    if db_stats:
        print(f"\n🗄️  Database Statistics:")
        print(f"   Total Episodes: {db_stats['total_episodes']:,}")
        print(f"   Unique Shows: {db_stats['unique_shows']}")
        if db_stats['earliest_date']:
            print(f"   Date Range: {db_stats['earliest_date']} to {db_stats['latest_date']}")


if __name__ == "__main__":
    main()