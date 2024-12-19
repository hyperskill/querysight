import pytest
from datetime import datetime, timedelta
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.config import Config

@pytest.fixture
def ch_client():
    """Create a ClickHouse client using configuration from Config."""
    return ClickHouseDataAcquisition(
        host=Config.CLICKHOUSE_HOST,
        port=Config.CLICKHOUSE_PORT,
        user=Config.CLICKHOUSE_USER,
        password=Config.CLICKHOUSE_PASSWORD,
        database=Config.CLICKHOUSE_DATABASE
    )

def test_connection(ch_client):
    """Test that connection to ClickHouse is established successfully."""
    # The connection is tested during initialization
    assert ch_client.client is not None

def test_basic_query_logs(ch_client):
    """Test basic query log retrieval without filters."""
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)
    
    result = ch_client.get_query_logs(
        start_date=start_date,
        end_date=end_date,
        sample_size=0.1
    )
    
    assert result['status'] in ['completed', 'in_progress']
    assert 'data' in result
    assert 'total_rows' in result
    assert 'loaded_rows' in result
    
    if result['status'] == 'completed':
        assert isinstance(result['data'], list)
        if len(result['data']) > 0:
            log_entry = result['data'][0]
            required_fields = [
                'query_id', 'query', 'type', 'user', 'query_start_time',
                'query_duration_ms', 'read_rows', 'read_bytes', 'result_rows',
                'result_bytes', 'memory_usage', 'normalized_query_hash'
            ]
            for field in required_fields:
                assert field in log_entry

def test_query_logs_with_filters(ch_client):
    """Test query log retrieval with various filters."""
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)
    
    result = ch_client.get_query_logs(
        start_date=start_date,
        end_date=end_date,
        sample_size=0.1,
        user_include=['default'],
        query_types=['SELECT'],
        query_focus=['Slow Queries']
    )
    
    assert result['status'] in ['completed', 'in_progress']
    if result['status'] == 'completed' and len(result['data']) > 0:
        # Verify that all queries are SELECT queries
        assert all('SELECT' in log['query'].upper() for log in result['data'])
        # Verify that all queries are from included users
        assert all(log['user'] == 'default' for log in result['data'])
        # Verify that all queries are slow (>1000ms)
        assert all(log['query_duration_ms'] > 1000 for log in result['data'])

def test_query_logs_sampling(ch_client):
    """Test that sampling works correctly."""
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)
    
    # Get counts with different sampling rates
    full_result = ch_client.get_query_logs(
        start_date=start_date,
        end_date=end_date,
        sample_size=1.0
    )
    
    sampled_result = ch_client.get_query_logs(
        start_date=start_date,
        end_date=end_date,
        sample_size=0.5
    )
    
    if full_result['status'] == 'completed' and sampled_result['status'] == 'completed':
        # The sampled result should have approximately half the rows
        # Allow for some variance due to the nature of sampling
        full_count = len(full_result['data'])
        sampled_count = len(sampled_result['data'])
        if full_count > 0:
            ratio = sampled_count / full_count
            assert 0.3 <= ratio <= 0.7  # Allow for sampling variance

def test_query_pattern_analysis(ch_client):
    """Test query pattern analysis functionality."""
    # Create some sample query logs
    sample_logs = [
        {
            'query': 'SELECT * FROM table1 WHERE id = 123',
            'query_duration_ms': 100,
            'read_rows': 1000,
            'read_bytes': 10000
        },
        {
            'query': 'SELECT * FROM table1 WHERE id = 456',
            'query_duration_ms': 150,
            'read_rows': 1500,
            'read_bytes': 15000
        },
        {
            'query': 'SELECT COUNT(*) FROM table2',
            'query_duration_ms': 200,
            'read_rows': 2000,
            'read_bytes': 20000
        }
    ]
    
    patterns = ch_client.analyze_query_patterns(sample_logs)
    
    assert isinstance(patterns, list)
    assert len(patterns) > 0
    
    # Check pattern structure
    for pattern in patterns:
        assert 'pattern' in pattern
        assert 'frequency' in pattern
        assert 'avg_duration_ms' in pattern
        assert 'avg_read_rows' in pattern
        assert 'avg_read_bytes' in pattern
        assert 'example_query' in pattern
    
    # Verify that similar queries are grouped
    table1_pattern = next((p for p in patterns if 'table1' in p['pattern']), None)
    if table1_pattern:
        assert table1_pattern['frequency'] == 2  # Two similar queries should be grouped
        assert 125 <= table1_pattern['avg_duration_ms'] <= 125  # (100 + 150) / 2
        assert 1250 <= table1_pattern['avg_read_rows'] <= 1250  # (1000 + 1500) / 2

def test_error_handling(ch_client):
    """Test error handling for invalid queries."""
    end_date = datetime.now()
    start_date = end_date + timedelta(hours=1)  # Invalid date range
    
    result = ch_client.get_query_logs(
        start_date=start_date,
        end_date=end_date,
        sample_size=0.1
    )
    
    assert result['status'] == 'error'
    assert 'error' in result
    assert len(result['data']) == 0

def test_batch_processing(ch_client):
    """Test that batch processing works correctly."""
    end_date = datetime.now()
    start_date = end_date - timedelta(hours=1)
    
    result = ch_client.get_query_logs(
        start_date=start_date,
        end_date=end_date,
        batch_size=50,  # Small batch size to test batching
        sample_size=1.0
    )
    
    assert result['status'] in ['completed', 'in_progress']
    assert 'loaded_rows' in result
    assert 'total_rows' in result
    
    if result['status'] == 'completed':
        assert result['loaded_rows'] <= result['total_rows']
