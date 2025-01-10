import sqlite3
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any
from .models import QueryLog, QueryPattern, AnalysisResult

class QueryLogsCacheManager:
    def __init__(self, db_path: str = 'cache.db'):
        self.db_path = db_path
        self.cache_enabled = True
        self.cache_duration = timedelta(hours=24)
        
        # Initialize the database
        self._initialize_db()
        
    def _initialize_db(self):
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            # Create tables if they do not exist
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS query_logs (
                id INTEGER PRIMARY KEY,
                query TEXT,
                execution_time REAL,
                execution_time_category TEXT,
                query_length INTEGER,
                start_date TEXT,
                end_date TEXT
            )
            ''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS analysis_results (
                cache_key TEXT PRIMARY KEY,
                result TEXT,
                timestamp TEXT
            )
            ''')
            conn.commit()

    def get_cached_data(
        self,
        start_date: datetime,
        end_date: datetime
    ) -> Optional[List[QueryLog]]:
        """
        Retrieve cached query logs if available and not expired
        """
        if not self.cache_enabled:
            return None
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT * FROM query_logs
            WHERE start_date >= ? AND end_date <= ?
            ''', (start_date.isoformat(), end_date.isoformat()))
            rows = cursor.fetchall()
            
            if not rows:
                return None
            
            return [QueryLog.from_dict(dict(row)) for row in rows]

    def update_cache(
        self,
        logs: List[QueryLog],
        start_date: datetime,
        end_date: datetime
    ) -> None:
        """
        Update cache with new query logs
        """
        if not self.cache_enabled:
            return
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            for log in logs:
                cursor.execute('''
                INSERT INTO query_logs (query, execution_time, execution_time_category, query_length, start_date, end_date)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (log.query, log.execution_time, log.execution_time_category, log.query_length, start_date.isoformat(), end_date.isoformat()))
            conn.commit()
        
    def cache_analysis_result(
        self,
        result: AnalysisResult,
        cache_key: str
    ) -> None:
        """
        Cache analysis result for future reference
        """
        if not self.cache_enabled:
            return
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            serialized = {
                "timestamp": result.timestamp.isoformat(),
                "query_patterns": [
                    {
                        "pattern_id": p.pattern_id,
                        "sql_pattern": p.sql_pattern,
                        "model_name": p.model_name,
                        "frequency": p.frequency,
                        "total_duration_ms": p.total_duration_ms,
                        "avg_duration_ms": p.avg_duration_ms,
                        "first_seen": p.first_seen.isoformat() if p.first_seen else None,
                        "last_seen": p.last_seen.isoformat() if p.last_seen else None,
                        "users": list(p.users),
                        "tables_accessed": list(p.tables_accessed)
                    }
                    for p in result.query_patterns
                ],
                "model_coverage": result.model_coverage,
                "uncovered_tables": list(result.uncovered_tables)
            }
            
            cursor.execute('''
            INSERT OR REPLACE INTO analysis_results (cache_key, result, timestamp)
            VALUES (?, ?, ?)
            ''', (cache_key, json.dumps(serialized), result.timestamp.isoformat()))
            conn.commit()

    def get_cached_analysis(
        self,
        cache_key: str,
        max_age: timedelta = timedelta(hours=24)
    ) -> Optional[AnalysisResult]:
        """
        Retrieve cached analysis result if available and not expired
        """
        if not self.cache_enabled:
            return None
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT result, timestamp FROM analysis_results
            WHERE cache_key = ?
            ''', (cache_key,))
            row = cursor.fetchone()
            
            if not row:
                return None
            
            result_data, timestamp = row
            if datetime.fromisoformat(timestamp) < datetime.now() - max_age:
                return None
            
            data = json.loads(result_data)
            patterns = [
                QueryPattern(
                    pattern_id=p['pattern_id'],
                    sql_pattern=p['sql_pattern'],
                    model_name=p['model_name'],
                    frequency=p['frequency'],
                    total_duration_ms=p['total_duration_ms'],
                    avg_duration_ms=p['avg_duration_ms'],
                    first_seen=datetime.fromisoformat(p['first_seen']) if p['first_seen'] else None,
                    last_seen=datetime.fromisoformat(p['last_seen']) if p['last_seen'] else None,
                    users=set(p['users']),
                    tables_accessed=set(p['tables_accessed'])
                )
                for p in data['query_patterns']
            ]
            
            return AnalysisResult(
                timestamp=datetime.fromisoformat(data['timestamp']),
                query_patterns=patterns,
                dbt_models={},  # DBT models are loaded separately
                uncovered_tables=set(data['uncovered_tables']),
                model_coverage=data['model_coverage']
            )
    
    def has_valid_cache(
        self,
        start_date: datetime,
        end_date: datetime,
        query_focus: str
    ) -> bool:
        """Check if valid cache exists for the given parameters"""
        if not self.cache_enabled:
            return False
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT COUNT(*) FROM query_logs
            WHERE start_date >= ? AND end_date <= ?
            ''', (start_date.isoformat(), end_date.isoformat()))
            count = cursor.fetchone()[0]
            
            return count > 0

    def get_cached_logs(self) -> Dict[str, Any]:
        """Get cached logs with metadata"""
        if not self.cache_enabled:
            return {'status': 'error', 'error': 'Cache is disabled'}
            
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
            SELECT * FROM query_logs
            ORDER BY start_date DESC
            LIMIT 1
            ''')
            log = cursor.fetchone()
            if log:
                return {'status': 'success', 'data': log}
            else:
                return {'status': 'error', 'error': 'No cached logs found'}