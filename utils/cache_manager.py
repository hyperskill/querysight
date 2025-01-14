import os
import json
import sqlite3
from datetime import datetime, timedelta
import pandas as pd
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from .models import QueryLog, QueryPattern, AnalysisResult, AIRecommendation, DBTModel
from .logger import setup_logger
from pathlib import Path

logger = setup_logger(__name__)

class QueryLogsCacheManager:
    """Manages caching of query logs and analysis results"""
    
    def __init__(self, cache_dir: str = ".cache", force_reset: bool = False):
        """Initialize cache manager with SQLite backend"""
        self.cache_dir = Path(os.getcwd())
        self.db_path = self.cache_dir / "cache.db"
        self.force_reset = force_reset
        
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
        if self.force_reset and self.db_path.exists():
            os.remove(self.db_path)
            logger.info("Cache database reset forced")
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Cache metadata table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS cache_metadata (
                    cache_key TEXT PRIMARY KEY,
                    data_type TEXT,
                    timestamp TEXT,
                    expiry TEXT,
                    level INTEGER
                )
            """)
            
            # Query logs table with new fields
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
            
            # Migrate existing data from type to query_kind if needed
            try:
                cursor.execute("SELECT * FROM query_logs LIMIT 1")
                columns = [description[0] for description in cursor.description]
                if 'type' in columns and 'query_kind' not in columns:
                    logger.info("Migrating query_logs table from 'type' to 'query_kind'")
                    # Create new column
                    cursor.execute("ALTER TABLE query_logs ADD COLUMN query_kind TEXT")
                    # Copy data
                    cursor.execute("UPDATE query_logs SET query_kind = type")
                    # Drop old column (SQLite doesn't support DROP COLUMN directly)
                    cursor.execute("""
                        CREATE TABLE query_logs_new (
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
                            cache_key TEXT,
                            timestamp REAL
                        )
                    """)
                    cursor.execute("""
                        INSERT INTO query_logs_new 
                        SELECT query_id, query, query_kind, user, query_start_time,
                               query_duration_ms, read_rows, read_bytes, result_rows,
                               result_bytes, memory_usage, normalized_query_hash,
                               cache_key, timestamp
                        FROM query_logs
                    """)
                    cursor.execute("DROP TABLE query_logs")
                    cursor.execute("ALTER TABLE query_logs_new RENAME TO query_logs")
            except sqlite3.OperationalError:
                # Table doesn't exist yet, no migration needed
                pass
            
            # Create index on query_start_time for efficient filtering
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(query_start_time)")
            
            # Drop and recreate patterns table with new schema
            cursor.execute("DROP TABLE IF EXISTS patterns")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS patterns (
                    pattern_id TEXT PRIMARY KEY,
                    sql_pattern TEXT,
                    frequency INTEGER,
                    total_duration_ms INTEGER,
                    users TEXT,
                    tables_accessed TEXT,
                    first_seen REAL,
                    last_seen REAL,
                    cache_key TEXT,
                    timestamp REAL
                )
            """)
            
            # Drop and recreate analysis_results table with new schema
            cursor.execute("DROP TABLE IF EXISTS analysis_results")
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_results (
                    result_id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    query_patterns TEXT,
                    dbt_models TEXT,
                    uncovered_tables TEXT,
                    model_coverage TEXT,
                    cache_key TEXT
                )
            """)
            
            # DBT Models table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS dbt_models (
                    name TEXT PRIMARY KEY,
                    path TEXT,
                    materialization TEXT,
                    freshness_hours INTEGER
                )
            """)
            
            # Model columns table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_columns (
                    model_name TEXT,
                    column_name TEXT,
                    column_type TEXT,
                    PRIMARY KEY (model_name, column_name),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Model tests table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_tests (
                    model_name TEXT,
                    test_name TEXT,
                    PRIMARY KEY (model_name, test_name),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Model dependencies table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_dependencies (
                    model_name TEXT,
                    depends_on TEXT,
                    PRIMARY KEY (model_name, depends_on),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Model references table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS model_references (
                    model_name TEXT,
                    referenced_by TEXT,
                    PRIMARY KEY (model_name, referenced_by),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Legacy cache table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT,
                    timestamp REAL
                )
            """)
            
            # Query Patterns Table - Direct mapping of QueryPattern class
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
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Pattern Users - Many-to-many relationship
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_users (
                    pattern_id TEXT,
                    user TEXT,
                    PRIMARY KEY (pattern_id, user),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            # Pattern Tables - Many-to-many relationship
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_tables (
                    pattern_id TEXT,
                    table_name TEXT,
                    PRIMARY KEY (pattern_id, table_name),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            # Pattern DBT Models - Many-to-many relationship
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_dbt_models (
                    pattern_id TEXT,
                    model_name TEXT,
                    PRIMARY KEY (pattern_id, model_name),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            # Create indexes for better query performance
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_user ON query_logs(user)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_query_kind ON query_logs(query_kind)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(query_start_time)")
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_patterns_model ON query_patterns(model_name)")
            
            # Recommendations Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    description TEXT,
                    impact TEXT,
                    suggested_sql TEXT
                )
            """)
            
            # Analysis Cache Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analysis_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT,
                    timestamp REAL,
                    expiry REAL,
                    level INTEGER
                )
            """)
            
            # Pattern Relationships Table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS pattern_relationships (
                    source_pattern_id TEXT,
                    target_pattern_id TEXT,
                    relationship_type TEXT,
                    confidence REAL,
                    created_at TEXT,
                    PRIMARY KEY (source_pattern_id, target_pattern_id, relationship_type)
                )
            """)
            
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Insert logs
            for log in logs:
                serialized = self._serialize_query_log(log)
                cursor.execute("""
                    INSERT OR REPLACE INTO query_logs (
                        query_id, query, query_kind, user, query_start_time,
                        query_duration_ms, read_rows, read_bytes,
                        result_rows, result_bytes, memory_usage,
                        normalized_query_hash, current_database, databases, tables, columns,
                        cache_key, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    serialized['query_id'], serialized['query'], serialized['query_kind'], serialized['user'],
                    serialized['query_start_time'], serialized['query_duration_ms'],
                    serialized['read_rows'], serialized['read_bytes'], serialized['result_rows'],
                    serialized['result_bytes'], serialized['memory_usage'], serialized['normalized_query_hash'],
                    serialized['current_database'], serialized['databases'], serialized['tables'], serialized['columns'],
                    cache_key, datetime.now().timestamp()
                ))
            
            # Update cache metadata
            cursor.execute("""
                INSERT OR REPLACE INTO cache_metadata (
                    cache_key, data_type, timestamp, expiry, level
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                cache_key, 'query_logs', datetime.now().isoformat(),
                expiry.isoformat() if expiry else None, 1
            ))

    def get_cached_query_logs(self, cache_key: str) -> Optional[List[QueryLog]]:
        """Retrieve cached query logs"""
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT COUNT(*) FROM cache_metadata 
                WHERE cache_key = ? AND (expiry IS NULL OR expiry > ?)
            """, (cache_key, datetime.now().isoformat()))
            return cursor.fetchone()[0] > 0

    def get_cached_data(self, cache_key: str) -> Any:
        """Retrieve cached data for the given key"""
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            timestamp = datetime.now().timestamp()
            
            for pattern in patterns:
                cursor.execute("""
                    INSERT OR REPLACE INTO patterns (
                        pattern_id, sql_pattern, frequency, total_duration_ms,
                        users, tables_accessed, first_seen, last_seen,
                        cache_key, timestamp
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    pattern.pattern_id,
                    pattern.sql_pattern,
                    pattern.frequency,
                    pattern.total_duration_ms,
                    ','.join(pattern.users),
                    ','.join(pattern.tables_accessed),
                    pattern.first_seen.timestamp() if pattern.first_seen else None,
                    pattern.last_seen.timestamp() if pattern.last_seen else None,
                    cache_key,
                    timestamp
                ))
                
            # Update cache metadata
            cursor.execute("""
                INSERT OR REPLACE INTO cache_metadata (
                    cache_key, data_type, timestamp, expiry, level
                ) VALUES (?, ?, ?, ?, ?)
            """, (
                cache_key, 'pattern_analysis', datetime.now().isoformat(),
                (datetime.now() + self.cache_durations[2]).isoformat(), 2
            ))

    def get_cached_patterns(self, cache_key: str) -> List[Any]:
        """Retrieve cached patterns"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT pattern_id, sql_pattern, frequency, total_duration_ms,
                       users, tables_accessed, first_seen, last_seen
                FROM patterns
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

    def enrich_patterns(self, new_patterns: List[QueryPattern]) -> List[QueryPattern]:
        """Enrich new patterns with historical data and maintain version history"""
        enriched_patterns = []
        
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for pattern in new_patterns:
                # Get the latest version for this pattern
                cursor.execute('''
                SELECT version, frequency, total_duration_ms, users, tables_accessed, first_seen
                FROM query_patterns 
                WHERE pattern_id = ?
                ORDER BY version DESC
                LIMIT 1
                ''', (pattern.pattern_id,))
                existing = cursor.fetchone()
                
                new_version = 1
                if existing:
                    new_version = existing[0] + 1
                    # Check if pattern has significantly changed
                    if (pattern.frequency > existing[1] * 1.5 or  # 50% increase in frequency
                        pattern.total_duration_ms > existing[2] * 1.2):  # 20% increase in duration
                        # Create relationship between versions
                        cursor.execute('''
                        INSERT INTO pattern_relationships
                        (source_pattern_id, target_pattern_id, relationship_type, confidence, created_at)
                        VALUES (?, ?, ?, ?, ?)
                        ''', (
                            pattern.pattern_id,
                            f"{pattern.pattern_id}_v{existing[0]}",
                            'version_evolution',
                            0.9,
                            datetime.now().isoformat()
                        ))
                    
                    # Enrich with historical data
                    historical_users = set(json.loads(existing[3]))
                    historical_tables = set(json.loads(existing[4]))
                    pattern.users.update(historical_users)
                    pattern.tables_accessed.update(historical_tables)
                    
                    if existing[5]:  # first_seen might be NULL
                        historical_first_seen = datetime.fromisoformat(existing[5])
                        if not pattern.first_seen or historical_first_seen < pattern.first_seen:
                            pattern.first_seen = historical_first_seen
                
                # Insert new version
                cursor.execute('''
                INSERT INTO query_patterns (
                    pattern_id, version, sql_pattern, model_name, frequency,
                    total_duration_ms, avg_duration_ms, first_seen, last_seen,
                    users, tables_accessed, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    pattern.pattern_id,
                    new_version,
                    pattern.sql_pattern,
                    pattern.model_name,
                    pattern.frequency,
                    pattern.total_duration_ms,
                    pattern.avg_duration_ms,
                    pattern.first_seen.isoformat() if pattern.first_seen else None,
                    pattern.last_seen.isoformat() if pattern.last_seen else None,
                    json.dumps(list(pattern.users)),
                    json.dumps(list(pattern.tables_accessed)),
                    datetime.now().isoformat(),
                    datetime.now().isoformat()
                ))
                
                enriched_patterns.append(pattern)
            
            conn.commit()
        
        return enriched_patterns

    def get_pattern_history(self, pattern_id: str) -> Optional[Dict]:
        """Get historical data for a specific pattern"""
        with sqlite3.connect(self.db_path) as conn:
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
            
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('DELETE FROM analysis_cache')
            conn.commit()

    def cache_dbt_analysis(self, analysis_result: AnalysisResult, cache_key: str, expiry: Optional[datetime] = None):
        """Cache DBT analysis results using direct SQL inserts"""
        with sqlite3.connect(self.db_path) as conn:
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
        with sqlite3.connect(self.db_path) as conn:
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
                    SELECT * FROM patterns 
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
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO analysis_cache (cache_key, data, timestamp)
                VALUES (?, ?, ?)
            """, (cache_key, json.dumps(data), datetime.now().timestamp()))
    def _cache_legacy_data(self, cache_key: str, data: Any):
        """Fallback method for old cache format"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO analysis_cache (cache_key, data, timestamp)
                VALUES (?, ?, ?)
            """, (cache_key, json.dumps(data), datetime.now().timestamp()))