import time
import hashlib
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
from .models import QueryLog, QueryPattern, QueryType, QueryFocus
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
        query_focus: Optional[List[QueryFocus]] = None,
        query_types: Optional[List[QueryType]] = None
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
            # Validate input parameters
            if start_date > end_date:
                return {
                    'status': 'error',
                    'data': [],
                    'error': 'Start date must be before end date',
                    'total_rows': 0,
                    'loaded_rows': 0
                }

            logger.info(f"Starting query log retrieval with parameters: start_date={start_date}, end_date={end_date}, sample_size={sample_size}, user_include={user_include}, query_types={query_types}, query_focus={query_focus}")
            
            # Validate sample size
            if not 0 < sample_size <= 1:
                return {
                    'status': 'error',
                    'data': [],
                    'error': 'Sample size must be between 0 and 1',
                    'total_rows': 0,
                    'loaded_rows': 0
                }
                
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

            # Build the WHERE clause dynamically
            where_clauses = [
                "query_start_time BETWEEN %(start_date)s AND %(end_date)s",
                "type = 'QueryStart'",
                "query NOT LIKE '%%system.query_log%%'",
                "query NOT LIKE '%%SELECT 1%%'"
            ]
            
            params = {
                'start_date': start_date,
                'end_date': end_date,
                'min_duration': 1000,  # Default for slow queries
                'min_frequency': 10    # Default for frequent queries
            }
            
            # First check if we have any queries at all in this time range
            base_count_query = """
                SELECT count(*)
                FROM system.query_log
                WHERE query_start_time BETWEEN %(start_date)s AND %(end_date)s
                AND type = 'QueryStart'
            """
            try:
                base_count = self.client.execute(base_count_query, params)[0][0]
                logger.info(f"Found {base_count} total queries in time range")
            except Exception as e:
                logger.error(f"Failed to get base query count: {str(e)}")
            
            logger.debug(f"Initial params: {params}")
            
            # Add user filters
            if user_include:
                placeholders = [f"%(user_include_{i})s" for i in range(len(user_include))]
                where_clauses.append(f"user IN ({','.join(placeholders)})")
                for i, user in enumerate(user_include):
                    params[f'user_include_{i}'] = user
                logger.debug(f"Added user include filters: {params}")

            if user_exclude:
                placeholders = [f"%(user_exclude_{i})s" for i in range(len(user_exclude))]
                where_clauses.append(f"user NOT IN ({','.join(placeholders)})")
                for i, user in enumerate(user_exclude):
                    params[f'user_exclude_{i}'] = user
                logger.debug(f"Added user exclude filters: {params}")
                
            # Add query type filters
            if query_types and 'All' not in query_types:
                type_conditions = []
                for i, qtype in enumerate(query_types):
                    param_name = f'query_type_{i}'
                    # Extract the actual SQL command type (SELECT, INSERT, etc.)
                    sql_type = qtype.value if hasattr(qtype, 'value') else str(qtype)
                    type_conditions.append(f"query ILIKE %(query_type_{i})s")
                    params[param_name] = f'%%{sql_type}%%'
                    logger.debug(f"Adding query type filter: {sql_type}")
                where_clauses.append(f"({' OR '.join(type_conditions)})")
                logger.debug(f"Added query type filters: {type_conditions}")
                
            # Add performance filters
            if query_focus and 'All Queries' not in query_focus:
                if 'Slow Queries' in query_focus:
                    where_clauses.append("query_duration_ms > %(min_duration)s")
                    params['min_duration'] = 100  # Lower threshold for testing (100ms instead of 1000ms)
                    
                    # Log the slow query count
                    try:
                        count_query = f"""
                            SELECT count(*) 
                            FROM system.query_log 
                            WHERE query_start_time BETWEEN %(start_date)s AND %(end_date)s
                            AND query_duration_ms > %(min_duration)s
                        """
                        slow_count = self.client.execute(count_query, params)[0][0]
                        logger.info(f"Found {slow_count} queries slower than {params['min_duration']}ms")
                    except Exception as e:
                        logger.error(f"Failed to count slow queries: {str(e)}")
                        
                if 'Frequent Queries' in query_focus:
                    # This requires a subquery to count query occurrences
                    frequency_subquery = """
                        SELECT normalized_query_hash 
                        FROM system.query_log 
                        WHERE query_start_time BETWEEN %(start_date)s AND %(end_date)s
                        GROUP BY normalized_query_hash 
                        HAVING count() > %(min_frequency)s
                    """
                    where_clauses.append(f"normalized_query_hash IN ({frequency_subquery})")
                    params['min_frequency'] = 3  # Lower threshold for testing
                    
                    # Log the subquery results
                    try:
                        count_query = f"SELECT count(*) FROM ({frequency_subquery})"
                        subquery_count = self.client.execute(count_query, params)[0][0]
                        logger.info(f"Found {subquery_count} query patterns with frequency > {params['min_frequency']}")
                    except Exception as e:
                        logger.error(f"Failed to count frequent queries: {str(e)}")
                logger.debug(f"Added performance filters: {params}")
            
            # Apply sampling at the application level instead of using SAMPLE clause
            limit_with_sampling = int(batch_size / sample_size) if sample_size < 1.0 else batch_size
            params['limit'] = limit_with_sampling
            params['offset'] = 0
            
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
            FROM system.query_log
            WHERE {' AND '.join(where_clauses)}
            ORDER BY query_start_time DESC
            LIMIT %(limit)s
            OFFSET %(offset)s
            """
            
            logger.info("Executing query with params:")
            logger.info(f"Query: {query}")
            logger.info(f"Params: {params}")
            
            try:
                offset = 0
                all_rows = []
                total_count_query = f"""
                    SELECT count(*)
                    FROM system.query_log
                    WHERE {' AND '.join(where_clauses)}
                """
                
                logger.info(f"Count query: {total_count_query}")
                
                # Get total count first
                try:
                    total_count = self.client.execute(total_count_query, params)[0][0]
                    logger.info(f"Total count: {total_count}")
                    
                    # If no rows found, return early
                    if total_count == 0:
                        return {
                            'status': 'completed',
                            'data': [],
                            'total_rows': 0,
                            'loaded_rows': 0
                        }
                except Exception as e:
                    logger.error(f"Failed to get total count: {str(e)}")
                    return {
                        'status': 'error',
                        'data': [],
                        'error': f"Failed to get total count: {str(e)}",
                        'total_rows': 0,
                        'loaded_rows': 0
                    }
                
                while offset < total_count:
                    try:
                        batch_params = {
                            **params,
                            'limit': limit_with_sampling,
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
                                    if len(all_rows) > 0:
                                        # If we have some data, return it with error status
                                        return {
                                            'status': 'error',
                                            'data': all_rows,
                                            'error': f"Failed to execute query after {max_retries} retries: {str(e)}",
                                            'total_rows': total_count,
                                            'loaded_rows': len(all_rows)
                                        }
                                    else:
                                        # If we have no data, return empty result with error
                                        return {
                                            'status': 'error',
                                            'data': [],
                                            'error': f"Failed to execute query after {max_retries} retries: {str(e)}",
                                            'total_rows': 0,
                                            'loaded_rows': 0
                                        }
                                time.sleep(1)
                        
                        if result is None:
                            if len(all_rows) > 0:
                                # If we have some data, return it with error status
                                return {
                                    'status': 'error',
                                    'data': all_rows,
                                    'error': "Failed to execute query after retries",
                                    'total_rows': total_count,
                                    'loaded_rows': len(all_rows)
                                }
                            else:
                                # If we have no data, return empty result with error
                                return {
                                    'status': 'error',
                                    'data': [],
                                    'error': "Failed to execute query after retries",
                                    'total_rows': 0,
                                    'loaded_rows': 0
                                }
                        
                        rows, columns = result
                        batch_rows = []
                        for row in rows:
                            processed_row = QueryLog(
                                query_id=row[0],
                                query=row[1],
                                type=str(row[2]),
                                user=row[3],
                                query_start_time=row[4],
                                query_duration_ms=float(row[5]),
                                read_rows=int(row[6]),
                                read_bytes=int(row[7]),
                                result_rows=int(row[8]),
                                result_bytes=int(row[9]),
                                memory_usage=int(row[10]),
                                normalized_query_hash=row[11]
                            )
                            batch_rows.append(processed_row)
                        
                        # Apply sampling at application level
                        if sample_size < 1.0:
                            import random
                            random.seed(42)  # For reproducibility
                            batch_rows = random.sample(batch_rows, int(len(batch_rows) * sample_size))
                        
                        all_rows.extend(batch_rows)
                        offset += batch_size
                        
                        if len(batch_rows) > 0:
                            try:
                                # Update cache with the current batch
                                batch_df = pd.DataFrame([log.to_dict() for log in batch_rows])
                                self.cache_manager.update_cache(batch_df, start_date, end_date)
                                logger.info(f"Successfully cached batch with {len(batch_rows)} rows")
                            except Exception as cache_error:
                                logger.error(f"Failed to update cache: {str(cache_error)}")
                                # Continue processing even if cache update fails
                        
                        # Only return if we have a significant batch or reached the end
                        if len(all_rows) >= batch_size or offset >= total_count:
                            return {
                                'status': 'in_progress' if offset < total_count else 'completed',
                                'data': [log.to_dict() for log in all_rows],
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
            error_msg = f"Unexpected error in get_query_logs: {str(e)}"
            logger.error(error_msg)
            return {
                'status': 'error',
                'data': [],
                'error': error_msg,
                'total_rows': 0,
                'loaded_rows': 0
            }

    def test_connection(self) -> None:
        """Test the connection to ClickHouse"""
        try:
            self.client.execute("SELECT 1")
        except Exception as e:
            raise Exception(f"Failed to connect to ClickHouse: {str(e)}")

    def analyze_query_patterns(
        self,
        query_logs: List[QueryLog],
        min_frequency: int = 5
    ) -> List[QueryPattern]:
        """
        Analyze query patterns from the logs
        
        Args:
            query_logs: List of query logs to analyze
            min_frequency: Minimum frequency threshold for patterns
        """
        patterns: Dict[str, QueryPattern] = {}
        
        def normalize_query(query: str) -> tuple[str, str]:
            # Remove literals
            query = re.sub(r"'[^']*'", "'?'", query)
            query = re.sub(r"\d+", "?", query)
            # Extract model name if present
            model_match = re.search(r'FROM\s+([a-zA-Z0-9_]+\.)?([a-zA-Z0-9_]+)', query, re.IGNORECASE)
            model_name = model_match.group(2) if model_match else 'Unknown'
            # Remove whitespace
            query = " ".join(query.split())
            return query, model_name

        for log in query_logs:
            normalized_query, model_name = normalize_query(log.query)
            pattern_id = hashlib.md5(normalized_query.encode()).hexdigest()
            
            if pattern_id not in patterns:
                patterns[pattern_id] = QueryPattern(
                    pattern_id=pattern_id,
                    sql_pattern=normalized_query,
                    model_name=model_name
                )
            
            patterns[pattern_id].update_from_log(log)
            
        # Filter patterns by frequency
        return [p for p in patterns.values() if p.frequency >= min_frequency]
