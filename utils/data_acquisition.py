import time
try:
    from clickhouse_driver import Client
except ImportError:
    raise ImportError("clickhouse-driver is required. Please install it using 'pip install clickhouse-driver'")

from datetime import datetime, timedelta
try:
    import pandas as pd
except ImportError:
    raise ImportError("pandas is required. Please install it using 'pip install pandas'")

from typing import List, Dict, Any, Optional, Tuple, Union
import re
from .cache_manager import QueryLogsCacheManager
import logging

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
        start_date: datetime,
        end_date: datetime,
        batch_size: int = 100,
        sample_size: float = 0.1,
        user_include: Optional[List[str]] = None,
        user_exclude: Optional[List[str]] = None,
        query_focus: Optional[List[str]] = None,
        query_types: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Retrieve query logs from ClickHouse system.query_log table with advanced filtering
        
        Args:
            start_date: Start date for log retrieval
            end_date: End date for log retrieval
            batch_size: Number of rows to retrieve per batch
            sample_size: Fraction of data to sample (0.0 to 1.0)
            user_include: List of usernames to include
            user_exclude: List of usernames to exclude
            query_focus: List of query focus options ("Slow Queries", "Frequent Queries", "All Queries")
            query_types: List of query types to include ("SELECT", "INSERT", etc.)
        
        Returns:
            Dict[str, Any]: Always returns a dictionary with at least these keys:
                - status: 'error' | 'completed' | 'in_progress'
                - data: List of query logs or empty list
                - error: Optional error message
        """
        try:
            logger.info(f"Starting query log retrieval with sample_size={sample_size}")
            
            # Validate sample size
            if not 0 < sample_size <= 1:
                raise ValueError("Sample size must be between 0 and 1")
                
            # Try to get data from cache first
            cached_df = self.cache_manager.get_cached_data(start_date, end_date)
            if cached_df is not None:
                # Apply sampling to cached data
                if sample_size < 1.0:
                    cached_df = cached_df.sample(frac=sample_size, random_state=42)
                return {
                    'status': 'completed',
                    'data': cached_df.to_dict('records'),
                    'total_rows': len(cached_df),
                    'loaded_rows': len(cached_df),
                    'sampling_rate': sample_size
                }

            # If cache miss, fetch from ClickHouse
            sampling_clause = f"SAMPLE {sample_size:.4f}" if sample_size < 1.0 else ""
            logger.debug(f"Using sampling clause: {sampling_clause}")
            
            # Build the WHERE clause dynamically
            where_clauses = [
                "query_start_time BETWEEN %(start_date)s AND %(end_date)s",
                "type = 'QueryStart'",
                "query NOT LIKE '%%system.query_log%%'",
                "query NOT LIKE '%%SELECT 1%%'"
            ]
            
            # Add user filters
            if user_include:
                where_clauses.append(f"user IN ({','.join(['%s' for _ in user_include])})")
            if user_exclude:
                where_clauses.append(f"user NOT IN ({','.join(['%s' for _ in user_exclude])})")
                
            # Add query type filters
            if query_types and 'All' not in query_types:
                type_conditions = []
                for qtype in query_types:
                    type_conditions.append(f"query ILIKE '%%{qtype}%%'")
                where_clauses.append(f"({' OR '.join(type_conditions)})")
                
            # Add performance filters
            if query_focus and 'All Queries' not in query_focus:
                if 'Slow Queries' in query_focus:
                    where_clauses.append("query_duration_ms > 1000")  # Queries taking more than 1 second
                if 'Frequent Queries' in query_focus:
                    # This requires a subquery to count query occurrences
                    where_clauses.append("""
                        normalized_query_hash IN (
                            SELECT normalized_query_hash 
                            FROM system.query_log 
                            WHERE query_start_time BETWEEN %(start_date)s AND %(end_date)s
                            GROUP BY normalized_query_hash 
                            HAVING count() > 10
                        )
                    """)
            
            query = f"""
            SELECT
                query_id,
                query,
                type,
                user,
                query_start_time,
                query_duration_ms,
                read_rows,
                read_bytes,
                result_rows,
                result_bytes,
                memory_usage,
                normalized_query_hash
            FROM system.query_log{' ' + sampling_clause if sampling_clause else ''}
            WHERE {' AND '.join(where_clauses)}
            ORDER BY query_start_time DESC
            LIMIT %(limit)s
            OFFSET %(offset)s
            """
            
            params = {
                'start_date': start_date,
                'end_date': end_date
            }
            
            try:
                offset = 0
                all_rows = []
                total_count_query = f"""
                    SELECT count(*)
                    FROM system.query_log
                    WHERE query_start_time BETWEEN %(start_date)s AND %(end_date)s
                        AND type = 'QueryStart'
                        AND query NOT LIKE '%%system.query_log%%'
                        AND query NOT LIKE '%%SELECT 1%%'
                """
                
                # Get total count first
                total_count = self.client.execute(total_count_query, params)[0][0]
                
                while offset < total_count:
                    try:
                        batch_params = {
                            **params,
                            'limit': batch_size,
                            'offset': offset
                        }
                        
                        logger.debug(f"Executing batch with parameters: {batch_params}")
                        # Add retry logic and optimized settings
                        max_retries = 3
                        retry_count = 0
                        result = None
                        
                        while retry_count < max_retries:
                            try:
                                result = self.client.execute(
                                    query,
                                    batch_params,
                                    settings={
                                        'use_numpy': True,
                                        'types_check': True,
                                        'max_memory_usage': 10000000000,
                                        'timeout_before_checking_execution_speed': 1000
                                    },
                                    with_column_types=True
                                )
                                logger.debug(f"Successfully executed batch query, got {len(result[0])} rows")
                                break
                            except Exception as e:
                                logger.error(f"Retry {retry_count + 1} failed: {str(e)}")
                                logger.error(f"Failed query: {query}")
                                retry_count += 1
                                if retry_count == max_retries:
                                    return {
                                        'status': 'error',
                                        'data': [],
                                        'error': f"Failed to execute query after {max_retries} retries: {str(e)}",
                                        'total_rows': total_count,
                                        'loaded_rows': len(all_rows)
                                    }
                                time.sleep(1)
                        
                        if result is None:
                            raise Exception("Failed to execute query after retries")
                        
                        rows, columns = result
                        batch_rows = []
                        for row in rows:
                            processed_row = {
                                'query_id': row[0],
                                'query': row[1],
                                'type': str(row[2]),
                                'user': row[3],
                                'query_start_time': row[4],
                                'query_duration_ms': float(row[5]),
                                'read_rows': int(row[6]),
                                'read_bytes': int(row[7]),
                                'result_rows': int(row[8]),
                                'result_bytes': int(row[9]),
                                'memory_usage': int(row[10]),
                                'normalized_query_hash': row[11]
                            }
                            batch_rows.append(processed_row)
                        
                        all_rows.extend(batch_rows)
                        offset += batch_size
                        
                        if len(batch_rows) > 0:
                            try:
                                # Update cache with the current batch
                                batch_df = pd.DataFrame(batch_rows)
                                self.cache_manager.update_cache(batch_df, start_date, end_date)
                                logger.info(f"Successfully cached batch with {len(batch_rows)} rows")
                            except Exception as cache_error:
                                logger.error(f"Failed to update cache: {str(cache_error)}")
                                # Continue processing even if cache update fails
                        
                        # Only return if we have a significant batch or reached the end
                        if len(all_rows) >= batch_size or offset >= total_count:
                            return {
                                'status': 'in_progress' if offset < total_count else 'completed',
                                'data': all_rows,
                                'total_rows': total_count,
                                'loaded_rows': len(all_rows)
                            }
                        
                    except Exception as batch_error:
                        logger.error(f"Error processing batch at offset {offset}: {str(batch_error)}")
                        # Skip problematic batch and continue with next one
                        offset += batch_size
                
            except Exception as e:
                error_msg = f"Failed to retrieve query logs: {str(e)}"
                logger.error(error_msg)
                return {
                    'status': 'error',
                    'data': [],
                    'error': error_msg,
                    'total_rows': 0,
                    'loaded_rows': 0
                }

        except Exception as e:
            error_msg = f"Failed to process query logs: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'data': [],
                'error': error_msg,
                'total_rows': 0,
                'loaded_rows': 0
            }

    def analyze_query_patterns(self, query_logs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze query patterns from the logs
        """
        patterns = {}
        
        def normalize_query(query: str) -> str:
            # Remove literals
            query = re.sub(r"'[^']*'", "'?'", query)
            query = re.sub(r"\d+", "?", query)
            # Remove whitespace
            query = " ".join(query.split())
            return query

        for log in query_logs:
            query = log['query']
            normalized_query = normalize_query(query)
            
            if normalized_query in patterns:
                patterns[normalized_query]['count'] += 1
                patterns[normalized_query]['total_duration'] += log['query_duration_ms']
                patterns[normalized_query]['total_read_rows'] += log['read_rows']
                patterns[normalized_query]['total_read_bytes'] += log['read_bytes']
            else:
                patterns[normalized_query] = {
                    'count': 1,
                    'total_duration': log['query_duration_ms'],
                    'total_read_rows': log['read_rows'],
                    'total_read_bytes': log['read_bytes'],
                    'example_query': query
                }

        # Convert to list and calculate averages
        pattern_list = []
        for pattern, stats in patterns.items():
            pattern_list.append({
                'pattern': pattern,
                'frequency': stats['count'],
                'avg_duration_ms': stats['total_duration'] / stats['count'],
                'avg_read_rows': stats['total_read_rows'] / stats['count'],
                'avg_read_bytes': stats['total_read_bytes'] / stats['count'],
                'example_query': stats['example_query']
            })

        # Sort by frequency
        pattern_list.sort(key=lambda x: x['frequency'], reverse=True)
        return pattern_list
