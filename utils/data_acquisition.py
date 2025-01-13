import time
import hashlib
try:
    from clickhouse_driver import Client
except ImportError:
    raise ImportError("clickhouse-driver is required. Please install it using 'pip install clickhouse-driver'")

from datetime import datetime, timedelta
import pandas as pd
from typing import List, Dict, Any, Optional, Tuple, Union
import re
from .cache_manager import QueryLogsCacheManager
from .models import QueryLog, QueryPattern, QueryType, QueryFocus
from .sql_parser import extract_tables_from_query
import logging
import sys

logger = logging.getLogger(__name__)

class ClickHouseDataAcquisition:
    def __init__(self, host: str, port: int, user: str, password: str, database: str):
        """Initialize ClickHouse connection with optimized settings"""
        try:
            self.client = Client(
                host=host,
                port=port,
                user=user,
                password=password,
                database=database,
                settings={
                    'max_execution_time': 30,  # 30 seconds timeout
                    'max_threads': 2,  # Limit thread usage
                    'use_uncompressed_cache': 1,
                    'max_block_size': 100000,
                    'connect_timeout': 10
                }
            )
            self.cache_manager = QueryLogsCacheManager()
            logger.info("Successfully initialized ClickHouse connection")
        except Exception as e:
            logger.error(f"Failed to initialize ClickHouse connection: {str(e)}")
            raise

    def get_query_logs(
        self,
        days: int = 7,
        focus: QueryFocus = QueryFocus.ALL,
        include_users: Optional[List[str]] = None,
        exclude_users: Optional[List[str]] = None,
        query_types: Optional[List[QueryType]] = None,
        sample_size: float = 1.0,
        batch_size: int = 1000,
        use_cache: bool = True
    ) -> List[QueryLog]:
        """Fetch and process query logs from ClickHouse"""
        try:
            cache_key = self._generate_cache_key(days, focus, include_users, exclude_users, query_types, sample_size)
            
            if use_cache and self.cache_manager.has_valid_cache(cache_key):
                logger.info("Using cached query logs")
                return self.cache_manager.get_cached_data(cache_key)
            
            query_logs = []
            total_processed = 0
            
            # Build query conditions
            conditions = []
            params = {}
            
            # Time range condition
            conditions.append("event_time >= %(start_time)s")
            params['start_time'] = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
            
            # User filters
            if include_users:
                conditions.append("user IN %(include_users)s")
                params['include_users'] = tuple(include_users)
            if exclude_users:
                conditions.append("user NOT IN %(exclude_users)s")
                params['exclude_users'] = tuple(exclude_users)
            
            # Query type filter
            if query_types:
                conditions.append("type IN %(query_types)s")
                params['query_types'] = tuple(qt.value for qt in query_types)
            
            # Build WHERE clause
            where_clause = " AND ".join(conditions) if conditions else "1"
            
            # Add focus-specific conditions
            if focus == QueryFocus.SLOW:
                where_clause += " AND query_duration_ms > 1000"  # Slow queries > 1s
            
            # Build final query with sampling
            query = f"""
                SELECT 
                    query_id,
                    query,
                    type,
                    user,
                    event_time as query_start_time,
                    query_duration_ms,
                    read_rows,
                    read_bytes,
                    result_rows,
                    result_bytes,
                    memory_usage,
                    cityHash64(normalizeQuery(query)) as normalized_query_hash
                FROM system.query_log
                WHERE {where_clause}
                ORDER BY event_time DESC
            """
            
            # Execute query in batches
            for offset in range(0, sys.maxsize, batch_size):
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                batch_results = self.client.execute(batch_query, params)
                
                if not batch_results:
                    break
                    
                for row in batch_results:
                    query_logs.append(QueryLog(
                        query_id=row[0],
                        query=row[1],
                        type=row[2],
                        user=row[3],
                        query_start_time=row[4],
                        query_duration_ms=row[5],
                        read_rows=row[6],
                        read_bytes=row[7],
                        result_rows=row[8],
                        result_bytes=row[9],
                        memory_usage=row[10],
                        normalized_query_hash=str(row[11])
                    ))
                
                total_processed += len(batch_results)
                logger.info(f"Processed {total_processed} query logs")
                
                if len(batch_results) < batch_size:
                    break
            
            if use_cache:
                self.cache_manager.cache_data(cache_key, query_logs)
                logger.info("Cached query logs")
            
            return query_logs
            
        except Exception as e:
            logger.error(f"Error fetching query logs: {str(e)}")
            raise

    def analyze_query_patterns(
        self,
        query_logs: List[QueryLog],
        min_frequency: int = 2
    ) -> List[QueryPattern]:
        """Analyze query logs to identify patterns"""
        try:
            # Group queries by normalized hash
            patterns: Dict[str, QueryPattern] = {}
            
            for log in query_logs:
                pattern_id = log.normalized_query_hash
                
                if pattern_id not in patterns:
                    # Extract table references
                    tables = extract_tables_from_query(log.query)
                    
                    patterns[pattern_id] = QueryPattern(
                        pattern_id=pattern_id,
                        sql_pattern=log.query,
                        model_name=None,  
                        tables_accessed=tables
                    )
                
                # Update pattern metrics
                patterns[pattern_id].update_from_log(log)
            
            # Filter by minimum frequency
            frequent_patterns = [
                pattern for pattern in patterns.values()
                if pattern.frequency >= min_frequency
            ]
            
            # Sort by impact (frequency * avg duration)
            return sorted(
                frequent_patterns,
                key=lambda p: p.frequency * p.avg_duration_ms,
                reverse=True
            )
            
        except Exception as e:
            logger.error(f"Error analyzing query patterns: {str(e)}")
            raise

    def _generate_cache_key(self, *args) -> str:
        """Generate cache key from query parameters"""
        key_parts = [str(arg) for arg in args if arg is not None]
        return hashlib.md5('_'.join(key_parts).encode()).hexdigest()

    def test_connection(self) -> None:
        """Test the connection to ClickHouse"""
        try:
            self.client.execute("SELECT 1")
        except Exception as e:
            raise Exception(f"Failed to connect to ClickHouse: {str(e)}")
