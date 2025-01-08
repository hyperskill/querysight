from datetime import datetime, timedelta
import pandas as pd
from typing import Optional, Dict, Any, List
import json
import os
from pathlib import Path
from .models import QueryLog, QueryPattern, AnalysisResult

class QueryLogsCacheManager:
    def __init__(self, cache_dir: str = '.cache'):
        try:
            self.cache_dir = Path(cache_dir)
            self.cache_dir.mkdir(parents=True, exist_ok=True)
            self.cache_enabled = True
        except (OSError, PermissionError):
            # If we can't create/access cache directory, disable caching
            self.cache_enabled = False
        self.cache_duration = timedelta(hours=24)
        
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
            
        cache_file = self._get_cache_file(start_date, end_date)
        if not cache_file.exists():
            return None
            
        # Check if cache is expired
        if self._is_cache_expired(cache_file):
            return None
            
        try:
            df = pd.read_parquet(cache_file)
            return [QueryLog.from_dict(row) for row in df.to_dict('records')]
        except Exception as e:
            return None
            
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
            
        try:
            cache_file = self._get_cache_file(start_date, end_date)
            
            # Convert logs to DataFrame
            df = pd.DataFrame([log.__dict__ for log in logs])
            
            # Save to parquet with compression
            df.to_parquet(
                cache_file,
                compression='snappy',
                index=False
            )
        except:
            # If caching fails, just continue without it
            pass
        
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
            
        try:
            cache_file = self.cache_dir / f"analysis_{cache_key}.json"
            
            # Convert to serializable format
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
            
            with open(cache_file, 'w') as f:
                json.dump(serialized, f, indent=2)
        except:
            # If caching fails, just continue without it
            pass
            
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
            
        cache_file = self.cache_dir / f"analysis_{cache_key}.json"
        
        if not cache_file.exists():
            return None
            
        # Check if cache is expired
        if self._is_cache_expired(cache_file, max_age):
            return None
            
        try:
            with open(cache_file, 'r') as f:
                data = json.load(f)
                
            # Reconstruct AnalysisResult
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
            
        except Exception as e:
            return None
    
    def has_valid_cache(
        self,
        start_date: datetime,
        end_date: datetime,
        query_focus: str
    ) -> bool:
        """Check if valid cache exists for the given parameters"""
        if not self.cache_enabled:
            return False
            
        cache_file = self._get_cache_file(start_date, end_date)
        if not cache_file.exists():
            return False
            
        # Check if cache is expired
        if self._is_cache_expired(cache_file):
            return False
            
        # Check if query focus matches
        try:
            with open(cache_file.with_suffix('.meta'), 'r') as f:
                meta = json.load(f)
                return meta.get('query_focus') == query_focus
        except:
            return False

    def get_cached_logs(self) -> Dict[str, Any]:
        """Get cached logs with metadata"""
        if not self.cache_enabled:
            return {'status': 'error', 'error': 'Cache is disabled'}
            
        # Find the most recent cache file
        try:
            cache_files = list(self.cache_dir.glob('*.parquet'))
            if not cache_files:
                return {'status': 'error', 'error': 'No cache found'}
                
            latest_cache = max(cache_files, key=lambda x: x.stat().st_mtime)
            
            df = pd.read_parquet(latest_cache)
            logs = [QueryLog.from_dict(row) for row in df.to_dict('records')]
            return {
                'status': 'success',
                'data': logs
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e)
            }

    def get_latest_result(self) -> Optional[AnalysisResult]:
        """Get the latest analysis result from cache"""
        if not self.cache_enabled:
            return None
            
        result_file = self.cache_dir / 'latest_result.json'
        if not result_file.exists():
            return None
            
        try:
            with open(result_file, 'r') as f:
                data = json.load(f)
                return AnalysisResult.from_dict(data)
        except:
            return None

    def cache_logs(
        self,
        logs: List[QueryLog],
        start_date: datetime,
        end_date: datetime,
        query_focus: str
    ) -> None:
        """Cache query logs with metadata"""
        if not self.cache_enabled:
            return
            
        try:
            cache_file = self._get_cache_file(start_date, end_date)
            
            # Save logs
            df = pd.DataFrame([log.__dict__ for log in logs])
            df.to_parquet(cache_file, compression='snappy')
            
            # Save metadata
            meta = {
                'query_focus': query_focus,
                'cached_at': datetime.now().isoformat(),
                'start_date': start_date.isoformat(),
                'end_date': end_date.isoformat()
            }
            with open(cache_file.with_suffix('.meta'), 'w') as f:
                json.dump(meta, f)
        except:
            # If caching fails, just continue without it
            pass

    def _get_cache_file(self, start_date: datetime, end_date: datetime) -> Path:
        """Generate cache file path based on date range"""
        cache_key = f"{start_date.date()}_{end_date.date()}"
        return self.cache_dir / f"query_logs_{cache_key}.parquet"
        
    def _is_cache_expired(
        self,
        cache_file: Path,
        max_age: timedelta = None
    ) -> bool:
        """Check if cache file is expired"""
        if max_age is None:
            max_age = self.cache_duration
            
        cache_time = datetime.fromtimestamp(cache_file.stat().st_mtime)
        return datetime.now() - cache_time > max_age
