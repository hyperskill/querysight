import os
import pytest
import pandas as pd
from datetime import datetime, timedelta
from utils.cache_manager import QueryLogsCacheManager

@pytest.fixture
def temp_cache_dir(tmp_path):
    """Fixture to provide a temporary cache directory."""
    cache_dir = tmp_path / "test_cache"
    cache_dir.mkdir()
    return str(cache_dir)

@pytest.fixture
def cache_manager(temp_cache_dir):
    """Fixture to provide a cache manager instance."""
    return QueryLogsCacheManager(cache_dir=temp_cache_dir)

@pytest.fixture
def sample_df():
    """Fixture to provide a sample DataFrame for testing."""
    return pd.DataFrame({
        'query_id': ['1', '2', '3'],
        'query_start_time': [
            datetime.now() - timedelta(minutes=30),
            datetime.now() - timedelta(minutes=20),
            datetime.now() - timedelta(minutes=10)
        ],
        'query_text': ['test1', 'test2', 'test3']
    })

def test_cache_dir_creation(temp_cache_dir):
    """Test that cache directory is created if it doesn't exist."""
    # Remove the directory created by fixture
    os.rmdir(temp_cache_dir)
    
    cache_manager = QueryLogsCacheManager(cache_dir=temp_cache_dir)
    assert os.path.exists(temp_cache_dir)

def test_initial_metadata(cache_manager):
    """Test that initial metadata is created correctly."""
    metadata = cache_manager._load_metadata()
    assert metadata == {"last_update": None, "date_ranges": []}

def test_cache_update_and_retrieval(cache_manager, sample_df):
    """Test updating cache and retrieving data."""
    start_date = datetime.now() - timedelta(hours=1)
    end_date = datetime.now()
    
    # Update cache with sample data
    cache_manager.update_cache(sample_df, start_date, end_date)
    
    # Retrieve cached data
    cached_df = cache_manager.get_cached_data(start_date, end_date)
    
    assert cached_df is not None
    assert len(cached_df) == len(sample_df)
    assert all(cached_df['query_id'].isin(sample_df['query_id']))

def test_cache_expiration(cache_manager, sample_df, monkeypatch):
    """Test that cache expires after 1 hour."""
    start_date = datetime.now() - timedelta(hours=1)
    end_date = datetime.now()
    
    # Update cache with sample data
    cache_manager.update_cache(sample_df, start_date, end_date)
    
    # Mock datetime.now() to return a future time
    class MockDateTime:
        @classmethod
        def now(cls):
            return datetime.now() + timedelta(hours=2)
        
        @classmethod
        def fromisoformat(cls, date_string):
            return datetime.fromisoformat(date_string)
    
    monkeypatch.setattr('utils.cache_manager.datetime', MockDateTime)
    
    # Try to retrieve cached data
    cached_df = cache_manager.get_cached_data(start_date, end_date)
    assert cached_df is None

def test_cache_date_range_filtering(cache_manager, sample_df):
    """Test that date range filtering works correctly."""
    start_date = datetime.now() - timedelta(hours=1)
    end_date = datetime.now()
    
    # Update cache with sample data
    cache_manager.update_cache(sample_df, start_date, end_date)
    
    # Try to retrieve data for a different date range
    different_start = datetime.now() - timedelta(days=2)
    different_end = datetime.now() - timedelta(days=1)
    
    cached_df = cache_manager.get_cached_data(different_start, different_end)
    assert cached_df is None or len(cached_df) == 0

def test_cache_merge_behavior(cache_manager):
    """Test that cache merges data correctly."""
    # Create two different datasets
    df1 = pd.DataFrame({
        'query_id': ['1', '2'],
        'query_start_time': [
            datetime.now() - timedelta(minutes=30),
            datetime.now() - timedelta(minutes=20)
        ],
        'query_text': ['test1', 'test2']
    })
    
    df2 = pd.DataFrame({
        'query_id': ['2', '3'],  # Note: query_id '2' overlaps
        'query_start_time': [
            datetime.now() - timedelta(minutes=20),
            datetime.now() - timedelta(minutes=10)
        ],
        'query_text': ['test2_updated', 'test3']
    })
    
    start_date = datetime.now() - timedelta(hours=1)
    end_date = datetime.now()
    
    # Update cache with first dataset
    cache_manager.update_cache(df1, start_date, end_date)
    # Update cache with second dataset
    cache_manager.update_cache(df2, start_date, end_date)
    
    # Retrieve merged data
    cached_df = cache_manager.get_cached_data(start_date, end_date)
    
    assert cached_df is not None
    assert len(cached_df) == 2  # Should have 2 unique query_ids after deduplication
    assert 'test2_updated' in cached_df['query_text'].values  # Should contain updated value
