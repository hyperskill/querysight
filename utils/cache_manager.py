import os
import pandas as pd
from datetime import datetime, timedelta
from typing import Optional, Dict, Any
import json

class QueryLogsCacheManager:
    def __init__(self, cache_dir: str = ".cache"):
        """Initialize cache manager with a specified cache directory."""
        self.cache_dir = cache_dir
        self.cache_file = os.path.join(cache_dir, "query_logs_cache.parquet")
        self.metadata_file = os.path.join(cache_dir, "cache_metadata.json")
        self._ensure_cache_dir()

    def _ensure_cache_dir(self):
        """Ensure cache directory exists."""
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)

    def _load_metadata(self) -> Dict[str, Any]:
        """Load cache metadata from file."""
        if not os.path.exists(self.metadata_file):
            return {"last_update": None, "date_ranges": []}
        try:
            with open(self.metadata_file, 'r') as f:
                return json.load(f)
        except Exception:
            return {"last_update": None, "date_ranges": []}

    def _save_metadata(self, metadata: Dict[str, Any]):
        """Save cache metadata to file."""
        with open(self.metadata_file, 'w') as f:
            json.dump(metadata, f)

    def get_cached_data(self, start_date: datetime, end_date: datetime) -> Optional[pd.DataFrame]:
        """
        Retrieve cached data for the given date range if available.
        Returns None if cache miss or invalid.
        """
        if not os.path.exists(self.cache_file):
            return None

        metadata = self._load_metadata()
        if not metadata["last_update"]:
            return None

        # Convert stored string dates to datetime
        last_update = datetime.fromisoformat(metadata["last_update"])
        
        # Check if cache is too old (more than 1 hour)
        if datetime.now() - last_update > timedelta(hours=1):
            return None

        try:
            df = pd.read_parquet(self.cache_file)
            mask = (df['query_start_time'] >= start_date) & (df['query_start_time'] <= end_date)
            filtered_df = df[mask]
            
            if len(filtered_df) > 0:
                return filtered_df
            return None
        except Exception:
            return None

    def update_cache(self, df: pd.DataFrame, start_date: datetime, end_date: datetime):
        """
        Update cache with new data.
        Merges new data with existing cache if present.
        """
        try:
            if os.path.exists(self.cache_file):
                existing_df = pd.read_parquet(self.cache_file)
                # Remove overlapping data
                mask = ~((existing_df['query_start_time'] >= start_date) & 
                        (existing_df['query_start_time'] <= end_date))
                existing_df = existing_df[mask]
                # Concatenate with new data
                df = pd.concat([existing_df, df]).drop_duplicates(subset=['query_id'])
            
            # Save updated DataFrame
            df.to_parquet(self.cache_file, index=False)
            
            # Update metadata
            metadata = self._load_metadata()
            metadata["last_update"] = datetime.now().isoformat()
            metadata["date_ranges"].append({
                "start": start_date.isoformat(),
                "end": end_date.isoformat()
            })
            self._save_metadata(metadata)
        except Exception as e:
            print(f"Failed to update cache: {str(e)}")
