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
    
    def __init__(self, cache_dir: str = ".cache"):
        """Initialize cache manager with SQLite backend"""
        self.cache_dir = Path(os.getcwd())
        self.db_path = self.cache_dir / "cache.db"
        
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
        with sqlite3.connect(self.db_path) as conn:
            # Query Logs Table - Direct mapping of QueryLog class
            conn.execute("""
                CREATE TABLE IF NOT EXISTS query_logs (
                    query_id TEXT PRIMARY KEY,
                    query TEXT NOT NULL,
                    type TEXT NOT NULL,
                    user TEXT NOT NULL,
                    query_start_time TIMESTAMP NOT NULL,
                    query_duration_ms REAL NOT NULL,
                    read_rows INTEGER NOT NULL,
                    read_bytes INTEGER NOT NULL,
                    result_rows INTEGER NOT NULL,
                    result_bytes INTEGER NOT NULL,
                    memory_usage INTEGER NOT NULL,
                    normalized_query_hash TEXT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Query Patterns Table - Direct mapping of QueryPattern class
            conn.execute("""
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
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pattern_users (
                    pattern_id TEXT,
                    user TEXT,
                    PRIMARY KEY (pattern_id, user),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            # Pattern Tables - Many-to-many relationship
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pattern_tables (
                    pattern_id TEXT,
                    table_name TEXT,
                    PRIMARY KEY (pattern_id, table_name),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            # Pattern DBT Models - Many-to-many relationship
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pattern_dbt_models (
                    pattern_id TEXT,
                    model_name TEXT,
                    PRIMARY KEY (pattern_id, model_name),
                    FOREIGN KEY (pattern_id) REFERENCES query_patterns(pattern_id)
                )
            """)
            
            # Cache metadata table for backward compatibility
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_metadata (
                    cache_key TEXT PRIMARY KEY,
                    data_type TEXT NOT NULL,
                    timestamp TIMESTAMP NOT NULL,
                    expiry TIMESTAMP,
                    level INTEGER
                )
            """)
            
            # Create indexes for better query performance
            conn.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_user ON query_logs(user)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_type ON query_logs(type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_query_logs_start_time ON query_logs(query_start_time)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_patterns_model ON query_patterns(model_name)")
            
            # Analysis Results Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS analysis_results (
                    id TEXT PRIMARY KEY,
                    timestamp TEXT,
                    uncovered_tables TEXT,
                    model_coverage TEXT
                )
            """)
            
            # Recommendations Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS recommendations (
                    id TEXT PRIMARY KEY,
                    type TEXT,
                    description TEXT,
                    impact TEXT,
                    suggested_sql TEXT
                )
            """)
            
            # Analysis Cache Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS analysis_cache (
                    cache_key TEXT PRIMARY KEY,
                    data TEXT,
                    timestamp REAL,
                    expiry REAL,
                    level INTEGER
                )
            """)
            
            # Pattern Relationships Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS pattern_relationships (
                    source_pattern_id TEXT,
                    target_pattern_id TEXT,
                    relationship_type TEXT,
                    confidence REAL,
                    created_at TEXT,
                    PRIMARY KEY (source_pattern_id, target_pattern_id, relationship_type)
                )
            """)
            
            # DBT Models Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS dbt_models (
                    name TEXT PRIMARY KEY,
                    path TEXT,
                    materialization TEXT,
                    freshness_hours REAL
                )
            """)
            
            # Model Columns Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS model_columns (
                    model_name TEXT,
                    column_name TEXT,
                    column_type TEXT,
                    PRIMARY KEY (model_name, column_name),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Model Tests Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS model_tests (
                    model_name TEXT,
                    test_name TEXT,
                    PRIMARY KEY (model_name, test_name),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Model Dependencies Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS model_dependencies (
                    model_name TEXT,
                    depends_on TEXT,
                    PRIMARY KEY (model_name, depends_on),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
            # Model References Table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS model_references (
                    model_name TEXT,
                    referenced_by TEXT,
                    PRIMARY KEY (model_name, referenced_by),
                    FOREIGN KEY (model_name) REFERENCES dbt_models(name)
                )
            """)
            
    def _serialize_query_log(self, log: QueryLog) -> Dict:
        """Serialize QueryLog to dictionary"""
        return {
            'query_id': log.query_id,
            'query': log.query,
            'type': log.type,
            'user': log.user,
            'query_start_time': log.query_start_time.isoformat() if log.query_start_time else None,
            'query_duration_ms': log.query_duration_ms,
            'read_rows': log.read_rows,
            'read_bytes': log.read_bytes,
            'result_rows': log.result_rows,
            'result_bytes': log.result_bytes,
            'memory_usage': log.memory_usage,
            'normalized_query_hash': log.normalized_query_hash
        }
    
    def _deserialize_query_log(self, data: Dict) -> QueryLog:
        """Deserialize dictionary to QueryLog"""
        return QueryLog(
            query_id=data['query_id'],
            query=data['query'],
            type=data['type'],
            user=data['user'],
            query_start_time=datetime.fromisoformat(data['query_start_time']) if data['query_start_time'] else None,
            query_duration_ms=data['query_duration_ms'],
            read_rows=data['read_rows'],
            read_bytes=data['read_bytes'],
            result_rows=data['result_rows'],
            result_bytes=data['result_bytes'],
            memory_usage=data['memory_usage'],
            normalized_query_hash=data['normalized_query_hash']
        )
    
    def cache_query_logs(self, logs: List[QueryLog], cache_key: str, expiry: Optional[datetime] = None):
        """Cache query logs using direct SQL inserts"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Insert logs
            for log in logs:
                cursor.execute("""
                    INSERT OR REPLACE INTO query_logs (
                        query_id, query, type, user, query_start_time,
                        query_duration_ms, read_rows, read_bytes,
                        result_rows, result_bytes, memory_usage,
                        normalized_query_hash
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    log.query_id, log.query, log.type, log.user,
                    log.query_start_time.isoformat(), log.query_duration_ms,
                    log.read_rows, log.read_bytes, log.result_rows,
                    log.result_bytes, log.memory_usage, log.normalized_query_hash
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
                type=row['type'],
                user=row['user'],
                query_start_time=datetime.fromisoformat(row['query_start_time']),
                query_duration_ms=row['query_duration_ms'],
                read_rows=row['read_rows'],
                read_bytes=row['read_bytes'],
                result_rows=row['result_rows'],
                result_bytes=row['result_bytes'],
                memory_usage=row['memory_usage'],
                normalized_query_hash=row['normalized_query_hash']
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
        elif isinstance(data, AnalysisResult):
            self.cache_dbt_analysis(data, cache_key)
        else:
            # For backward compatibility, use the old way
            self._cache_legacy_data(cache_key, data)

    def _cache_legacy_data(self, cache_key: str, data: Any):
        """Fallback method for old cache format"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT OR REPLACE INTO analysis_cache (cache_key, data, timestamp)
                VALUES (?, ?, ?)
            """, (cache_key, json.dumps(data), datetime.now().timestamp()))

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
                    id, timestamp, uncovered_tables, model_coverage
                ) VALUES (?, ?, ?, ?)
            """, (
                cache_key,
                analysis_result.timestamp.isoformat(),
                uncovered_tables,
                model_coverage
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
                SELECT * FROM analysis_results WHERE id = ?
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
            
            # Create AnalysisResult
            return AnalysisResult(
                timestamp=datetime.fromisoformat(result_row['timestamp']),
                query_patterns=[],  # Will be populated by pattern analysis
                dbt_models=dbt_models,
                uncovered_tables=set(json.loads(result_row['uncovered_tables'])),
                model_coverage=json.loads(result_row['model_coverage'])
            )