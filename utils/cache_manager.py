import os
import json
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from .models import QueryLog, QueryPattern, AnalysisResult, AIRecommendation, DBTModel
from .logger import setup_logger
from pathlib import Path
from .config import Config

logger = setup_logger(__name__)

class QueryLogsCacheManager:
    """Manages caching of query logs and analysis results"""
    
    def __init__(self, force_reset: bool = False):
        """Initialize cache manager"""
        self.force_reset = force_reset
        self.db_path = Path(Config.CACHE_DIR) / "query_logs.db"
        
        # Create cache directory if it doesn't exist
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # If force reset, remove the database file completely
        if self.force_reset and self.db_path.exists():
            os.remove(self.db_path)
            logger.info("Cache database reset forced")
        
        self._init_db()
        logger.info("Cache database initialized successfully")
        
        # Define level-specific cache durations
        self.cache_durations = {
            1: timedelta(hours=24),  # Data collection
            2: timedelta(hours=12),  # Pattern analysis
            3: timedelta(hours=6),   # DBT integration
            4: timedelta(hours=3)    # Optimization
        }
        
        self.cache_enabled = True

    def _init_db(self):
        """Initialize SQLite database with required tables"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            
            # Drop all existing tables if force reset
            if self.force_reset:
                tables = [
                    "cache_metadata", "query_logs", "query_patterns", "pattern_users",
                    "pattern_tables", "pattern_dbt_models", "pattern_relationships",
                    "dbt_models", "model_columns", "model_tests", "model_dependencies",
                    "model_references", "analysis_cache", "analysis_results"
                ]
                for table in tables:
                    cursor.execute(f"DROP TABLE IF EXISTS {table}")
                conn.commit()
            
            # Create tables in order of dependencies
            
            # Core tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cache_metadata (
                    cache_key TEXT PRIMARY KEY,
                    data_type TEXT,
                    timestamp TEXT,
                    expiry TEXT,
                    level INTEGER
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_logs (
                    query_id TEXT PRIMARY KEY,
                    query TEXT,
                    query_kind TEXT,
                    user TEXT,
                    query_start_time TEXT,
                    query_duration_ms REAL,
                    read_rows INTEGER,
                    read_bytes INTEGER,
                    result_rows INTEGER,
                    result_bytes INTEGER,
                    memory_usage INTEGER,
                    normalized_query_hash TEXT,
                    current_database TEXT,
                    databases TEXT,  -- JSON array
                    tables TEXT,     -- JSON array
                    columns TEXT,    -- JSON array
                    cache_key TEXT,
                    timestamp REAL
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS query_patterns (
                    pattern_id TEXT PRIMARY KEY,
                    sql_pattern TEXT NOT NULL,
                    model_name TEXT,
                    frequency INTEGER NOT NULL DEFAULT 0,
                    total_duration_ms REAL NOT NULL DEFAULT 0,
                    avg_duration_ms REAL NOT NULL DEFAULT 0,
                    first_seen TIMESTAMP,
                    last_seen TIMESTAMP,
                    memory_usage INTEGER NOT NULL DEFAULT 0,
                    total_read_rows INTEGER NOT NULL DEFAULT 0,
                    total_read_bytes INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    cache_key TEXT
                )
            """)
            conn.commit()
            
            # Create indexes for core tables
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(query_start_time)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_user ON query_logs(user)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_query_kind ON query_logs(query_kind)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_cache ON query_logs(cache_key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_timestamp ON query_logs(timestamp)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_patterns_model ON query_patterns(model_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_patterns_cache ON query_patterns(cache_key)")
            conn.commit()
            
            # Pattern relationship tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_users (
                    pattern_id TEXT,
                    user TEXT,
                    PRIMARY KEY (pattern_id, user),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_tables (
                    pattern_id TEXT,
                    table_name TEXT,
                    PRIMARY KEY (pattern_id, table_name),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_dbt_models (
                    pattern_id TEXT,
                    model_name TEXT,
                    PRIMARY KEY (pattern_id, model_name),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_relationships (
                    source_pattern_id TEXT,
                    target_pattern_id TEXT,
                    relationship_type TEXT,
                    confidence REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (source_pattern_id, target_pattern_id, relationship_type),
                    FOREIGN KEY (source_pattern_id) REFERENCES query_patterns(pattern_id),
                    FOREIGN KEY (target_pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            conn.commit()
            
            # Create indexes for pattern relationships
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pattern_tables_table ON pattern_tables(table_name)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_pattern_dbt_models_model ON pattern_dbt_models(model_name)")
            conn.commit()
            
            # DBT-related tables
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dbt_models (
                    name TEXT PRIMARY KEY,
                    path TEXT,
                    materialization TEXT,
                    freshness_hours INTEGER
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_columns (
                    model_name TEXT,
                    column_name TEXT,
                    column_type TEXT,
                    PRIMARY KEY (model_name, column_name),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_tests (
                    model_name TEXT,
                    test_name TEXT,
                    PRIMARY KEY (model_name, test_name),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_dependencies (
                    model_name TEXT,
                    depends_on TEXT,
                    PRIMARY KEY (model_name, depends_on),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_references (
                    model_name TEXT,
                    referenced_by TEXT,
                    PRIMARY KEY (model_name, referenced_by),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT,
                    timestamp REAL,
                    expiry REAL,
                    level INTEGER
                )
            """)
            
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_results (
                    result_id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    query_patterns TEXT,  -- JSON array of pattern IDs
                    dbt_models TEXT,      -- JSON array of model names
                    uncovered_tables TEXT, -- JSON array of table names
                    model_coverage TEXT,   -- JSON object with coverage metrics
                    cache_key TEXT,
                    FOREIGN KEY (cache_key) REFERENCES cache_metadata(cache_key)
                )
            """)
            conn.commit()

            # Create indexes for analysis results
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_results_cache ON analysis_results(cache_key)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_analysis_results_timestamp ON analysis_results(timestamp)")
            conn.commit()

    def _serialize_query_log(self, log: QueryLog) -> Dict:
        """Serialize QueryLog for database storage"""
        data = log.to_dict()
        # Convert lists to JSON strings
        data['databases'] = json.dumps(data['databases'])
        data['tables'] = json.dumps(data['tables'])
        data['columns'] = json.dumps(data['columns'])
        # Convert datetime to string if it's not already a string
        if not isinstance(data['query_start_time'], str):
            data['query_start_time'] = data['query_start_time'].isoformat()
        return data

    def _deserialize_query_log(self, data: Dict) -> QueryLog:
        """Deserialize QueryLog from database storage"""
        # Convert JSON strings back to lists
        data['databases'] = json.loads(data['databases']) if data['databases'] else []
        data['tables'] = json.loads(data['tables']) if data['tables'] else []
        data['columns'] = json.loads(data['columns']) if data['columns'] else []
        return QueryLog.from_dict(data)
    
    def cache_query_logs(self, logs: List[QueryLog], cache_key: str, expiry: Optional[datetime] = None):
        """Cache query logs using direct SQL inserts"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            # Insert logs
            for log in logs:
                serialized = self._serialize_query_log(log)
                cursor.execute("""
                    INSERT OR REPLACE INTO query_logs (
                        query_id, query, query_kind, user, query_start_time,
                        query_duration_ms, read_rows, read_bytes, result_rows,
                        result_bytes, memory_usage, normalized_query_hash,
                        current_database, databases, tables, columns,
                        cache_key, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    serialized['query_id'], serialized['query'], serialized['query_kind'], serialized['user'],
                    serialized['query_start_time'], serialized['query_duration_ms'],
                    serialized['read_rows'], serialized['read_bytes'],
                    serialized['result_rows'], serialized['result_bytes'],
                    serialized['memory_usage'], serialized['normalized_query_hash'],
                    serialized['current_database'], serialized['databases'],
                    serialized['tables'], serialized['columns'],
                    cache_key, datetime.now().timestamp()
                ))
            
            # Update cache metadata
            cursor.execute("""
                INSERT OR REPLACE INTO cache_metadata (cache_key, data_type, timestamp, expiry, level)
                VALUES (?, ?, ?, ?, ?)
            """, (
                cache_key,
                'query_logs',
                datetime.now().isoformat(),
                expiry.isoformat() if expiry else None,
                1
            ))
            conn.commit()

    def get_cached_query_logs(self, cache_key: str) -> Optional[List[QueryLog]]:
        """Retrieve cached query logs"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check cache validity
            cursor.execute("""
                SELECT * FROM cache_metadata 
                WHERE cache_key = ? AND (expiry IS NULL OR expiry > ?)
            """, (cache_key, datetime.now().isoformat()))
            
            if not cursor.fetchone():
                return None
            
            # Retrieve logs
            cursor.execute("SELECT * FROM query_logs")
            rows = cursor.fetchall()
            
            return [QueryLog(
                query_id=row['query_id'],
                query=row['query'],
                query_kind=row['query_kind'],
                user=row['user'],
                query_start_time=datetime.fromisoformat(row['query_start_time']),
                query_duration_ms=row['query_duration_ms'],
                read_rows=row['read_rows'],
                read_bytes=row['read_bytes'],
                result_rows=row['result_rows'],
                result_bytes=row['result_bytes'],
                memory_usage=row['memory_usage'],
                normalized_query_hash=row['normalized_query_hash'],
                current_database=row['current_database'],
                databases=json.loads(row['databases']) if row['databases'] else [],
                tables=json.loads(row['tables']) if row['tables'] else [],
                columns=json.loads(row['columns']) if row['columns'] else []
            ) for row in rows]

    def has_valid_cache(self, cache_key: str) -> bool:
        """Check if there is valid cache for the given key"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM cache_metadata 
                WHERE cache_key = ? AND (expiry IS NULL OR expiry > ?)
            """, (cache_key, datetime.now().isoformat()))
            return cursor.fetchone()[0] > 0

    def get_cached_data(self, cache_key: str) -> Any:
        """Retrieve cached data for the given key"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT data_type FROM cache_metadata WHERE cache_key = ?
            """, (cache_key,))
            row = cursor.fetchone()
            
            if not row:
                return None
                
            data_type = row[0]
            if data_type == 'query_logs':
                return self.get_cached_query_logs(cache_key)
            elif data_type == 'dbt_analysis':
                return self.get_cached_dbt_analysis(cache_key)
            elif data_type == 'pattern_analysis':
                return self.get_cached_patterns(cache_key)
            else:
                # For backward compatibility, try the old way
                return self._get_legacy_cached_data(cache_key)

    def _get_legacy_cached_data(self, cache_key: str) -> Any:
        """Fallback method for old cache format"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT data FROM analysis_cache WHERE cache_key = ?
            """, (cache_key,))
            row = cursor.fetchone()
            return json.loads(row[0]) if row else None

    def cache_data(self, cache_key: str, data: Any):
        """Cache data with the given key"""
        if isinstance(data, list) and all(isinstance(x, QueryLog) for x in data):
            self.cache_query_logs(data, cache_key)
        elif isinstance(data, list) and len(data) > 0 and hasattr(data[0], 'pattern_id'):
            self.cache_patterns(data, cache_key)
        elif isinstance(data, AnalysisResult):
            self.cache_dbt_analysis(data, cache_key)
        else:
            # For truly legacy data that doesn't fit our schema
            self._cache_legacy_data(cache_key, data)
            
    def cache_patterns(self, patterns: List[Any], cache_key: str):
        """Cache pattern analysis results using direct SQL inserts"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            for pattern in patterns:
                serialized = pattern.to_dict()
                cursor.execute("""
                    INSERT OR REPLACE INTO query_patterns (
                        pattern_id, sql_pattern, model_name, frequency,
                        total_duration_ms, avg_duration_ms, first_seen,
                        last_seen, memory_usage, total_read_rows,
                        total_read_bytes, cache_key
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    serialized['pattern_id'], serialized['sql_pattern'],
                    serialized.get('model_name', ''), serialized['frequency'],
                    serialized['total_duration_ms'], serialized['avg_duration_ms'],
                    serialized['first_seen'], serialized['last_seen'],
                    serialized['memory_usage'], serialized['total_read_rows'],
                    serialized['total_read_bytes'], cache_key
                ))
                
                # Insert user relationships
                for user in serialized['users']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO pattern_users (pattern_id, user)
                        VALUES (?, ?)
                    """, (serialized['pattern_id'], user))
                
                # Insert table relationships
                for table in serialized['tables_accessed']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO pattern_tables (pattern_id, table_name)
                        VALUES (?, ?)
                    """, (serialized['pattern_id'], table))
                
                # Insert DBT model relationships
                for model in serialized['dbt_models_used']:
                    cursor.execute("""
                        INSERT OR REPLACE INTO pattern_dbt_models (pattern_id, model_name)
                        VALUES (?, ?)
                    """, (serialized['pattern_id'], model))
            
            conn.commit()

    def get_cached_patterns(self, cache_key: str) -> List[Any]:
        """Retrieve cached patterns"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT pattern_id, sql_pattern, frequency, total_duration_ms,
                       users, tables_accessed, first_seen, last_seen
                FROM query_patterns
                WHERE cache_key = ?
                ORDER BY frequency DESC
            """, (cache_key,))
            
            patterns = []
            for row in cursor.fetchall():
                pattern = QueryPattern(
                    pattern_id=row[0],
                    sql_pattern=row[1],
                    model_name='',  
                    frequency=row[2],
                    total_duration_ms=row[3],
                    users=set(row[4].split(',')) if row[4] else set(),
                    tables_accessed=set(row[5].split(',')) if row[5] else set(),
                    first_seen=datetime.fromtimestamp(row[6]) if row[6] else None,
                    last_seen=datetime.fromtimestamp(row[7]) if row[7] else None
                )
                patterns.append(pattern)
                
            return patterns

    def get_or_create_pattern(self, pattern_id: str) -> Optional[QueryPattern]:
        """Get existing pattern or return None if not found"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT pattern_id, sql_pattern, model_name, frequency,
                       total_duration_ms, avg_duration_ms, first_seen,
                       last_seen, memory_usage, total_read_rows,
                       total_read_bytes
                FROM query_patterns 
                WHERE pattern_id = ?
                ORDER BY updated_at DESC 
                LIMIT 1
            """, (pattern_id,))
            row = cursor.fetchone()
            if row:
                return QueryPattern(
                    pattern_id=row[0],
                    sql_pattern=row[1],
                    model_name=row[2],
                    frequency=row[3],
                    total_duration_ms=row[4],
                    avg_duration_ms=row[5],
                    first_seen=datetime.fromisoformat(row[6]) if row[6] else None,
                    last_seen=datetime.fromisoformat(row[7]) if row[7] else None,
                    memory_usage=row[8],
                    total_read_rows=row[9],
                    total_read_bytes=row[10],
                    users=self._get_pattern_users(pattern_id),
                    tables_accessed=self._get_pattern_tables(pattern_id),
                    dbt_models_used=self._get_pattern_dbt_models(pattern_id)
                )
        return None

    def _get_pattern_users(self, pattern_id: str) -> Set[str]:
        """Get users for a pattern"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT user FROM pattern_users WHERE pattern_id = ?", (pattern_id,))
            return {row[0] for row in cursor.fetchall()}

    def _get_pattern_tables(self, pattern_id: str) -> Set[str]:
        """Get tables for a pattern"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT table_name FROM pattern_tables WHERE pattern_id = ?", (pattern_id,))
            return {row[0] for row in cursor.fetchall()}

    def _get_pattern_dbt_models(self, pattern_id: str) -> Set[str]:
        """Get DBT models for a pattern"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT model_name FROM pattern_dbt_models WHERE pattern_id = ?", (pattern_id,))
            return {row[0] for row in cursor.fetchall()}

    def cache_pattern(self, pattern: QueryPattern, cache_key: str) -> None:
        """Cache a single pattern with its relationships"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            
            # Insert/update pattern
            cursor.execute("""
                INSERT OR REPLACE INTO query_patterns (
                    pattern_id, sql_pattern, model_name, frequency,
                    total_duration_ms, avg_duration_ms, first_seen,
                    last_seen, memory_usage, total_read_rows,
                    total_read_bytes, updated_at, cache_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                pattern.pattern_id,
                pattern.sql_pattern,
                pattern.model_name,
                pattern.frequency,
                pattern.total_duration_ms,
                pattern.avg_duration_ms,
                pattern.first_seen.isoformat() if pattern.first_seen else None,
                pattern.last_seen.isoformat() if pattern.last_seen else None,
                pattern.memory_usage,
                pattern.total_read_rows,
                pattern.total_read_bytes,
                datetime.now().isoformat(),
                cache_key
            ))
            
            # Update users
            cursor.execute("DELETE FROM pattern_users WHERE pattern_id = ?", (pattern.pattern_id,))
            cursor.executemany(
                "INSERT INTO pattern_users (pattern_id, user) VALUES (?, ?)",
                [(pattern.pattern_id, user) for user in pattern.users]
            )
            
            # Update tables
            cursor.execute("DELETE FROM pattern_tables WHERE pattern_id = ?", (pattern.pattern_id,))
            cursor.executemany(
                "INSERT INTO pattern_tables (pattern_id, table_name) VALUES (?, ?)",
                [(pattern.pattern_id, table) for table in pattern.tables_accessed]
            )
            
            # Update DBT models
            cursor.execute("DELETE FROM pattern_dbt_models WHERE pattern_id = ?", (pattern.pattern_id,))
            cursor.executemany(
                "INSERT INTO pattern_dbt_models (pattern_id, model_name) VALUES (?, ?)",
                [(pattern.pattern_id, model) for model in pattern.dbt_models_used]
            )
            
            conn.commit()

    def enrich_patterns(self, new_patterns: List[QueryPattern], cache_key: str) -> List[QueryPattern]:
        """Enrich new patterns with historical data and maintain version history"""
        enriched_patterns = []
        
        for pattern in new_patterns:
            # Try to get existing pattern
            existing = self.get_or_create_pattern(pattern.pattern_id)
            if existing:
                # Update with new data but keep historical data
                existing.update_from_pattern(pattern)
                pattern = existing
            
            # Cache the enriched pattern
            self.cache_pattern(pattern, cache_key)
            enriched_patterns.append(pattern)
        
        return enriched_patterns

    def get_pattern_history(self, pattern_id: str) -> Optional[Dict]:
        """Get historical data for a specific pattern"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT * FROM query_patterns 
            WHERE pattern_id = ?
            ''', (pattern_id,))
            rows = cursor.fetchall()
            
            if not rows:
                return None
            
            return [
                {
                    'pattern_id': row[0],
                    'version': row[1],
                    'sql_pattern': row[2],
                    'model_name': row[3],
                    'frequency': row[4],
                    'total_duration_ms': row[5],
                    'avg_duration_ms': row[6],
                    'first_seen': row[7],
                    'last_seen': row[8],
                    'users': json.loads(row[9]),
                    'tables_accessed': json.loads(row[10]),
                    'created_at': row[11],
                    'updated_at': row[12]
                }
                for row in rows
            ]

    def get_latest_result(self) -> Optional[AnalysisResult]:
        """Get the latest analysis result from cache"""
        if not self.cache_enabled:
            return None
            
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute(
                '''
                SELECT data 
                FROM analysis_cache 
                WHERE cache_key LIKE 'analysis_result_%'
                ORDER BY timestamp DESC 
                LIMIT 1
                '''
            )
            result = cursor.fetchone()
            
            if not result:
                return None
                
            try:
                data = json.loads(result[0])
                return self._deserialize_data(data)
            except Exception as e:
                logger.error(f"Error deserializing latest result: {str(e)}")
                return None

    def _serialize_data(self, data: Any) -> Dict:
        """Serialize data for caching"""
        if isinstance(data, list):
            return {'type': 'list', 'items': [self._serialize_data(item) for item in data]}
        elif isinstance(data, QueryPattern):
            return {
                'type': 'QueryPattern',
                'data': {
                    'pattern_id': data.pattern_id,
                    'sql_pattern': data.sql_pattern,
                    'frequency': data.frequency,
                    'avg_duration': data.avg_duration,
                    'model_name': data.model_name,
                    'version': data.version
                }
            }
        elif isinstance(data, AnalysisResult):
            return {
                'type': 'AnalysisResult',
                'data': {
                    'timestamp': data.timestamp.isoformat() if data.timestamp else None,
                    'query_patterns': [self._serialize_data(pattern) for pattern in data.query_patterns] if data.query_patterns else [],
                    'model_coverage': data.model_coverage,
                    'uncovered_tables': list(data.uncovered_tables) if data.uncovered_tables else []
                }
            }
        elif isinstance(data, pd.DataFrame):
            return {'type': 'DataFrame', 'data': data.to_dict('records')}
        elif isinstance(data, datetime):
            return {'type': 'datetime', 'data': data.isoformat()}
        elif isinstance(data, (int, float, str, bool, type(None))):
            return {'type': 'primitive', 'data': data}
        else:
            raise ValueError(f"Cannot serialize object of type {type(data)}")

    def _deserialize_data(self, data: Dict) -> Any:
        """Deserialize cached data"""
        if not isinstance(data, dict) or 'type' not in data:
            return data
            
        if data['type'] == 'list':
            return [self._deserialize_data(item) for item in data['items']]
        elif data['type'] == 'QueryPattern':
            return QueryPattern(
                pattern_id=data['data']['pattern_id'],
                sql_pattern=data['data']['sql_pattern'],
                frequency=data['data']['frequency'],
                avg_duration=data['data']['avg_duration'],
                model_name=data['data']['model_name'],
                version=data['data']['version']
            )
        elif data['type'] == 'AnalysisResult':
            return AnalysisResult(
                timestamp=datetime.fromisoformat(data['data']['timestamp']) if data['data']['timestamp'] else None,
                query_patterns=[self._deserialize_data(pattern) for pattern in data['data']['query_patterns']],
                model_coverage=data['data']['model_coverage'],
                uncovered_tables=set(data['data']['uncovered_tables'])
            )
        elif data['type'] == 'DataFrame':
            return pd.DataFrame(data['data'])
        elif data['type'] == 'datetime':
            return datetime.fromisoformat(data['data'])
        elif data['type'] == 'primitive':
            return data['data']
        else:
            raise ValueError(f"Cannot deserialize object of type {data['type']}")

    def clear_cache(self) -> None:
        """Clear all cached data"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM analysis_cache')
            conn.commit()

    def cache_dbt_analysis(self, analysis_result: AnalysisResult, cache_key: str, expiry: Optional[datetime] = None):
        """Cache DBT analysis results using direct SQL inserts"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            
            # Store DBT models
            for model_name, model in analysis_result.dbt_models.items():
                cursor.execute("""
                    INSERT OR REPLACE INTO dbt_models (
                        name, path, materialization, freshness_hours
                    ) VALUES (?, ?, ?, ?)
                """, (
                    model_name,
                    model.path,
                    model.materialization,
                    int(model.freshness.total_seconds() / 3600) if model.freshness else None
                ))
                
                # Store model columns
                for col_name, col_type in model.columns.items():
                    cursor.execute("""
                        INSERT OR REPLACE INTO model_columns (
                            model_name, column_name, column_type
                        ) VALUES (?, ?, ?)
                    """, (model_name, col_name, col_type))
                
                # Store model tests
                for test in model.tests:
                    cursor.execute("""
                        INSERT OR REPLACE INTO model_tests (
                            model_name, test_name
                        ) VALUES (?, ?)
                    """, (model_name, test))
                
                # Store model dependencies
                for dep in model.depends_on:
                    cursor.execute("""
                        INSERT OR REPLACE INTO model_dependencies (
                            model_name, depends_on
                        ) VALUES (?, ?)
                    """, (model_name, dep))
                
                # Store model references
                for ref in model.referenced_by:
                    cursor.execute("""
                        INSERT OR REPLACE INTO model_references (
                            model_name, referenced_by
                        ) VALUES (?, ?)
                    """, (model_name, ref))
            
            # Store uncovered tables
            uncovered_tables = json.dumps(list(analysis_result.uncovered_tables))
            model_coverage = json.dumps(analysis_result.model_coverage)
            
            # Store analysis result metadata
            cursor.execute("""
                INSERT OR REPLACE INTO analysis_results (
                    result_id, timestamp, query_patterns, dbt_models, uncovered_tables, model_coverage, cache_key
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                cache_key,
                analysis_result.timestamp.isoformat(),
                json.dumps([pattern.pattern_id for pattern in analysis_result.query_patterns]),
                json.dumps(list(analysis_result.dbt_models.keys())),
                uncovered_tables,
                model_coverage,
                cache_key
            ))
            
            # Update cache metadata
            cursor.execute("""
                INSERT OR REPLACE INTO cache_metadata (
                    cache_key, data_type, timestamp, expiry, level
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                cache_key,
                'dbt_analysis',
                datetime.now().isoformat(),
                expiry.isoformat() if expiry else None,
                3  # DBT integration level
            ))

    def get_cached_dbt_analysis(self, cache_key: str) -> Optional[AnalysisResult]:
        """Retrieve cached DBT analysis"""
        with sqlite3.connect(str(self.db_path)) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            # Check cache validity
            cursor.execute("""
                SELECT * FROM cache_metadata 
                WHERE cache_key = ? AND data_type = 'dbt_analysis'
                AND (expiry IS NULL OR expiry > ?)
            """, (cache_key, datetime.now().isoformat()))
            
            if not cursor.fetchone():
                return None
            
            # Get analysis result metadata
            cursor.execute("""
                SELECT * FROM analysis_results WHERE result_id = ?
            """, (cache_key,))
            result_row = cursor.fetchone()
            if not result_row:
                return None
            
            # Get all DBT models
            dbt_models = {}
            cursor.execute("SELECT * FROM dbt_models")
            for model_row in cursor.fetchall():
                model = DBTModel(
                    name=model_row['name'],
                    path=model_row['path'],
                    materialization=model_row['materialization']
                )
                
                if model_row['freshness_hours']:
                    model.freshness = timedelta(hours=model_row['freshness_hours'])
                
                # Get model columns
                cursor.execute("""
                    SELECT column_name, column_type 
                    FROM model_columns 
                    WHERE model_name = ?
                """, (model.name,))
                model.columns = {row['column_name']: row['column_type'] 
                               for row in cursor.fetchall()}
                
                # Get model tests
                cursor.execute("""
                    SELECT test_name 
                    FROM model_tests 
                    WHERE model_name = ?
                """, (model.name,))
                model.tests = [row['test_name'] for row in cursor.fetchall()]
                
                # Get model dependencies
                cursor.execute("""
                    SELECT depends_on 
                    FROM model_dependencies 
                    WHERE model_name = ?
                """, (model.name,))
                model.depends_on = {row['depends_on'] for row in cursor.fetchall()}
                
                # Get model references
                cursor.execute("""
                    SELECT referenced_by 
                    FROM model_references 
                    WHERE model_name = ?
                """, (model.name,))
                model.referenced_by = {row['referenced_by'] for row in cursor.fetchall()}
                
                dbt_models[model.name] = model
            
            # Get query patterns
            pattern_ids = json.loads(result_row['query_patterns'])
            query_patterns = []
            if pattern_ids:
                cursor.execute("""
                    SELECT * FROM query_patterns 
                    WHERE pattern_id IN ({})
                """.format(','.join(['?'] * len(pattern_ids))), pattern_ids)
                
                for pattern_row in cursor.fetchall():
                    pattern = QueryPattern(
                        pattern_id=pattern_row['pattern_id'],
                        sql_pattern=pattern_row['sql_pattern'],
                        model_name='',  # Will be set during coverage calculation
                        frequency=pattern_row['frequency'],
                        total_duration_ms=pattern_row['total_duration_ms'],
                        users=set(pattern_row['users'].split(',')) if pattern_row['users'] else set(),
                        tables_accessed=set(pattern_row['tables_accessed'].split(',')) if pattern_row['tables_accessed'] else set(),
                        first_seen=datetime.fromtimestamp(pattern_row['first_seen']) if pattern_row['first_seen'] else None,
                        last_seen=datetime.fromtimestamp(pattern_row['last_seen']) if pattern_row['last_seen'] else None
                    )
                    query_patterns.append(pattern)
            
            # Create AnalysisResult
            result = AnalysisResult(
                timestamp=datetime.fromisoformat(result_row['timestamp']),
                query_patterns=query_patterns,
                dbt_models=dbt_models,
                uncovered_tables=set(json.loads(result_row['uncovered_tables'])),
                model_coverage=json.loads(result_row['model_coverage']),
                dbt_mapper=None  # Will be set by the caller
            )
            
            return result

    def _cache_legacy_data(self, cache_key: str, data: Any):
        """Fallback method for old cache format"""
        with sqlite3.connect(str(self.db_path)) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO analysis_cache (cache_key, data, timestamp)
                VALUES (?, ?, ?)
            """, (cache_key, json.dumps(data), datetime.now().timestamp()))