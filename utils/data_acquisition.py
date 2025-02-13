import logging
import sys
import hashlib
from datetime import datetime, timedelta
from typing import List, Dict, Optional
try:
    from clickhouse_driver import Client
except ImportError as exc:
    raise ImportError(
        "clickhouse-driver is required. Please install it using 'pip install clickhouse-driver'"
    ) from exc

from .cache_manager import QueryLogsCacheManager
from .models import QueryLog, QueryPattern, QueryKind, QueryFocus
from .sql_parser import extract_tables_from_query
import pandas as pd
import re

logger = logging.getLogger(__name__)

class ClickHouseDataAcquisition:
    """Handles data acquisition from ClickHouse database.
    Provides methods to fetch and analyze query logs with various filtering options."""
    def __init__(self, host: str, port: int, user: str, password: str, database: str, force_reset: bool = False):
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
            self.cache_manager = QueryLogsCacheManager(force_reset=force_reset)
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
        query_kinds: Optional[List[QueryKind]] = None,
        sample_size: float = 1.0,
        batch_size: int = 1000,
        use_cache: bool = True
    ) -> List[QueryLog]:
        """Fetch and process query logs from ClickHouse"""
        try:
            logger.info("Starting query log collection with parameters:")
            logger.info(f"  Days: {days}")
            logger.info(f"  Focus: {focus}")
            logger.info(f"  Include users: {include_users}")
            logger.info(f"  Exclude users: {exclude_users}")
            logger.info(f"  Query kinds: {query_kinds}")
            logger.info(f"  Sample size: {sample_size}")
            logger.info(f"  Batch size: {batch_size}")
            logger.info(f"  Use cache: {use_cache}")
            
            cache_key = self._generate_cache_key(days, focus, include_users, exclude_users, query_kinds, sample_size)
            logger.info(f"Generated cache key: {cache_key}")
            
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
                conditions.append("lower(user) IN %(include_users)s")
                params['include_users'] = tuple(u.lower() for u in include_users)
            if exclude_users:
                conditions.append("lower(user) NOT IN %(exclude_users)s")
                params['exclude_users'] = tuple(u.lower() for u in exclude_users)
            
            # Query kind filter
            if query_kinds:
                conditions.append("upper(query_kind) IN %(query_kinds)s")
                params['query_kinds'] = tuple(qk.value.upper() for qk in query_kinds)
            
            # Build WHERE clause
            where_clause = " AND ".join(conditions) if conditions else "1"
            
            # Add focus-specific conditions
            if focus == QueryFocus.SLOW:
                where_clause += " AND query_duration_ms > 1000"  # Slow queries > 1s
            
            logger.info(f"Building query with WHERE clause: {where_clause}")
            logger.info(f"Parameters: {params}")
            
            # Build base query without LIMIT/OFFSET
            query = f"""
                SELECT 
                    query_id,
                    query,
                    query_kind,
                    user,
                    event_time as query_start_time,
                    query_duration_ms,
                    read_rows,
                    read_bytes,
                    result_rows,
                    result_bytes,
                    memory_usage,
                    cityHash64(normalizeQuery(query)) as normalized_query_hash,
                    current_database,
                    databases,
                    tables,
                    columns
                FROM system.query_log
                WHERE {where_clause}
                ORDER BY event_time DESC
            """
            
            logger.info("Starting batch processing...")
            
            # Execute query in batches
            for offset in range(0, sys.maxsize, batch_size):
                batch_query = f"{query} LIMIT {batch_size} OFFSET {offset}"
                logger.info(f"Executing batch with offset {offset}")
                
                try:
                    batch_results = self.client.execute(batch_query, params)
                    if not batch_results:
                        logger.info(f"No more results at offset {offset}, stopping")
                        break
                        
                    logger.info(f"Processing batch of {len(batch_results)} rows")
                    for row in batch_results:
                        query_logs.append(QueryLog(
                            query_id=row[0],
                            query=row[1],
                            query_kind=row[2],
                            user=row[3],
                            query_start_time=row[4],
                            query_duration_ms=row[5],
                            read_rows=row[6],
                            read_bytes=row[7],
                            result_rows=row[8],
                            result_bytes=row[9],
                            memory_usage=row[10],
                            normalized_query_hash=str(row[11]),
                            current_database=row[12] or "",  # Handle NULL
                            databases=row[13] or [],         # Handle NULL
                            tables=row[14] or [],           # Handle NULL
                            columns=row[15] or []           # Handle NULL
                        ))
                        total_processed += 1
                except Exception as e:
                    logger.error(f"Batch query failed at offset {offset}: {str(e)}")
                    raise
            
            logger.info(f"Collected {total_processed} query logs from ClickHouse")
            logger.info(f"Query conditions: {where_clause}")
            logger.info(f"Parameters: {params}")
            
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
            
    def get_table_schema(self, table_name: str) -> List[Dict[str, str]]:
        """Get schema information for a table using DESCRIBE query
        
        Args:
            table_name: Name of the table to describe (can include database name)
            
        Returns:
            List of column information dictionaries with keys:
                name: Column name
                type: Column type
                default_type: Default expression type
                default_expression: Default expression
                comment: Column comment
                codec_expression: Compression codec
                ttl_expression: TTL expression
        """
        try:
            # If table includes database, use it, otherwise use current database
            full_table_name = table_name if '.' in table_name else f"{self.client.database}.{table_name}"
            
            # Execute DESCRIBE query
            schema = self.client.execute(
                f"DESCRIBE TABLE {full_table_name}",
                settings={'timeout_before_checking_execution_speed': 60}
            )
            
            # Convert to list of dicts for easier handling
            columns = []
            for col in schema:
                columns.append({
                    'name': col[0],
                    'type': col[1],
                    'default_type': col[2],
                    'default_expression': col[3],
                    'comment': col[4],
                    'codec_expression': col[5],
                    'ttl_expression': col[6]
                })
            
            return columns
            
        except Exception as e:
            logger.error(f"Error getting schema for table {table_name}: {str(e)}")
            raise
