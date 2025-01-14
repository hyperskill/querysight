from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from enum import Enum
from utils.logger import setup_logger
from .sql_parser import extract_tables_from_query
from .dbt_mapper import DBTModelMapper

logger = setup_logger(__name__)

class QueryKind(Enum):
    """Type of SQL query based on its operation.
    Used to categorize and filter queries for analysis."""
    SELECT = "SELECT"
    INSERT = "INSERT"
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    OTHER = "OTHER"
    SHOW = "SHOW"

class QueryFocus(Enum):
    """Focus area for query analysis.
    Determines which subset of queries to analyze based on specific criteria."""
    SLOW = "Slow Queries"
    FREQUENT = "Frequent Queries"
    ALL = "All Queries"

@dataclass
class QueryLog:
    """Raw query log entry from ClickHouse"""
    query_id: str
    query: str
    query_kind: str
    user: str
    query_start_time: datetime
    query_duration_ms: float
    read_rows: int
    read_bytes: int
    result_rows: int
    result_bytes: int
    memory_usage: int
    normalized_query_hash: str
    current_database: str = ""  # Default empty string for backward compatibility
    databases: List[str] = field(default_factory=list)  # Default empty list
    tables: List[str] = field(default_factory=list)  # Default empty list
    columns: List[str] = field(default_factory=list)  # Default empty list


    @classmethod
    def from_dict(cls, data: Dict) -> 'QueryLog':
        """Create QueryLog from dictionary, handling datetime conversion"""
        if isinstance(data.get('query_start_time'), str):
            data['query_start_time'] = datetime.fromisoformat(data['query_start_time'])
        return cls(
            query_id=data['query_id'],
            query=data['query'],
            query_kind=data['query_kind'],
            user=data['user'],
            query_start_time=data['query_start_time'],
            query_duration_ms=data['query_duration_ms'],
            read_rows=data['read_rows'],
            read_bytes=data['read_bytes'],
            result_rows=data['result_rows'],
            result_bytes=data['result_bytes'],
            memory_usage=data['memory_usage'],
            normalized_query_hash=data['normalized_query_hash'],
            current_database=data.get('current_database', ''),  # Use get() with default
            databases=data.get('databases', []),  # Use get() with default
            tables=data.get('tables', []),  # Use get() with default
            columns=data.get('columns', [])  # Use get() with default
        )
        
    def to_dict(self) -> Dict:
        """Convert QueryLog to dictionary for caching"""
        return {
            'query_id': self.query_id,
            'query': self.query,
            'query_kind': self.query_kind,
            'user': self.user,
            'query_start_time': self.query_start_time.isoformat(),
            'query_duration_ms': self.query_duration_ms,
            'read_rows': self.read_rows,
            'read_bytes': self.read_bytes,
            'result_rows': self.result_rows,
            'result_bytes': self.result_bytes,
            'memory_usage': self.memory_usage,
            'normalized_query_hash': self.normalized_query_hash,
            'current_database': self.current_database,
            'databases': self.databases,
            'tables': self.tables,
            'columns': self.columns
        }

@dataclass
class QueryPattern:
    """Analyzed query pattern with metrics"""
    pattern_id: str
    sql_pattern: str
    model_name: str
    frequency: int = 0
    total_duration_ms: float = 0
    avg_duration_ms: float = 0
    first_seen: Optional[datetime] = None
    last_seen: Optional[datetime] = None
    users: Set[str] = field(default_factory=set)
    tables_accessed: Set[str] = field(default_factory=set)
    dbt_models_used: Set[str] = field(default_factory=set)
    memory_usage: int = 0  # Total memory usage in bytes
    total_read_rows: int = 0
    total_read_bytes: int = 0

    def update_from_log(self, log: QueryLog) -> None:
        """Update pattern metrics from a new log entry"""
        self.frequency += 1
        self.total_duration_ms += log.query_duration_ms
        self.avg_duration_ms = self.total_duration_ms / self.frequency
        self.users.add(log.user)
        self.memory_usage += log.memory_usage
        self.total_read_rows += log.read_rows
        self.total_read_bytes += log.read_bytes

        # Update timestamps
        if not self.first_seen or log.query_start_time < self.first_seen:
            self.first_seen = log.query_start_time
        if not self.last_seen or log.query_start_time > self.last_seen:
            self.last_seen = log.query_start_time

        # Update tables from both sources
        if log.tables:  # From system.query_log
            self.tables_accessed.update(log.tables)
        if hasattr(log, 'extracted_tables'):  # From SQL parser
            self.tables_accessed.update(log.extracted_tables)

    def update_from_pattern(self, other: 'QueryPattern') -> None:
        """Update pattern with data from another pattern while preserving history"""
        if self.pattern_id != other.pattern_id:
            raise ValueError("Cannot update from a different pattern ID")
            
        self.frequency += other.frequency
        self.total_duration_ms += other.total_duration_ms
        self.avg_duration_ms = self.total_duration_ms / self.frequency
        self.users.update(other.users)
        self.tables_accessed.update(other.tables_accessed)
        self.dbt_models_used.update(other.dbt_models_used)
        self.memory_usage += other.memory_usage
        self.total_read_rows += other.total_read_rows
        self.total_read_bytes += other.total_read_bytes

        # Update timestamps
        if not self.first_seen or (other.first_seen and other.first_seen < self.first_seen):
            self.first_seen = other.first_seen
        if not self.last_seen or (other.last_seen and other.last_seen > self.last_seen):
            self.last_seen = other.last_seen

    @property
    def complexity_score(self) -> float:
        """Calculate complexity score based on pattern metrics"""
        duration_weight = min(self.avg_duration_ms / 1000, 1.0)  # Cap at 1.0
        frequency_weight = min(self.frequency / 100, 1.0)  # Cap at 1.0
        table_weight = min(len(self.tables_accessed) / 5, 1.0)  # Cap at 1.0
        
        return (duration_weight * 0.4 + 
                frequency_weight * 0.4 + 
                table_weight * 0.2)

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'pattern_id': self.pattern_id,
            'sql_pattern': self.sql_pattern,
            'model_name': self.model_name,
            'frequency': self.frequency,
            'total_duration_ms': self.total_duration_ms,
            'avg_duration_ms': self.avg_duration_ms,
            'first_seen': self.first_seen.isoformat() if self.first_seen else None,
            'last_seen': self.last_seen.isoformat() if self.last_seen else None,
            'users': list(self.users),
            'tables_accessed': list(self.tables_accessed),
            'dbt_models_used': list(self.dbt_models_used),
            'memory_usage': self.memory_usage,
            'total_read_rows': self.total_read_rows,
            'total_read_bytes': self.total_read_bytes
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'QueryPattern':
        """Create from dictionary"""
        return cls(
            pattern_id=data['pattern_id'],
            sql_pattern=data['sql_pattern'],
            model_name=data['model_name'],
            frequency=data['frequency'],
            total_duration_ms=data['total_duration_ms'],
            avg_duration_ms=data['avg_duration_ms'],
            first_seen=datetime.fromisoformat(data['first_seen']) if data.get('first_seen') else None,
            last_seen=datetime.fromisoformat(data['last_seen']) if data.get('last_seen') else None,
            users=set(data['users']),
            tables_accessed=set(data['tables_accessed']),
            dbt_models_used=set(data['dbt_models_used']),
            memory_usage=data['memory_usage'],
            total_read_rows=data['total_read_rows'],
            total_read_bytes=data['total_read_bytes']
        )

@dataclass
class DBTModel:
    """Representation of a dbt model"""
    name: str
    path: str
    depends_on: Set[str] = field(default_factory=set)
    referenced_by: Set[str] = field(default_factory=set)
    columns: Dict[str, str] = field(default_factory=dict)
    tests: List[str] = field(default_factory=list)
    materialization: str = "view"
    freshness: Optional[timedelta] = None
    
    def add_dependency(self, model_name: str) -> None:
        self.depends_on.add(model_name)
    
    def add_reference(self, model_name: str) -> None:
        self.referenced_by.add(model_name)

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'name': self.name,
            'path': self.path,
            'depends_on': list(self.depends_on),
            'referenced_by': list(self.referenced_by),
            'columns': self.columns,
            'tests': self.tests,
            'materialization': self.materialization,
            'freshness': self.freshness.total_seconds() if self.freshness else None
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'DBTModel':
        """Create from dictionary"""
        return cls(
            name=data['name'],
            path=data['path'],
            depends_on=set(data['depends_on']),
            referenced_by=set(data['referenced_by']),
            columns=data['columns'],
            tests=data['tests'],
            materialization=data['materialization'],
            freshness=timedelta(seconds=data['freshness']) if data.get('freshness') else None
        )

@dataclass
class SamplingConfig:
    """Configuration for query log sampling."""
    sample_size: float
    start_date: datetime
    end_date: datetime
    user_include: Optional[List[str]] = None
    user_exclude: Optional[List[str]] = None
    db_include: Optional[List[str]] = None  # Added database filtering
    db_exclude: Optional[List[str]] = None  # Added database filtering
    query_focus: List[str] = field(default_factory=list)
    query_types: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict:
        """Convert to dictionary representation."""
        return {
            'sample_size': self.sample_size,
            'start_date': self.start_date.isoformat(),
            'end_date': self.end_date.isoformat(),
            'user_include': self.user_include,
            'user_exclude': self.user_exclude,
            'db_include': self.db_include,
            'db_exclude': self.db_exclude,
            'query_focus': self.query_focus,
            'query_types': self.query_types
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'SamplingConfig':
        """Create from dictionary representation."""
        return cls(
            sample_size=data['sample_size'],
            start_date=datetime.fromisoformat(data['start_date']),
            end_date=datetime.fromisoformat(data['end_date']),
            user_include=data.get('user_include'),
            user_exclude=data.get('user_exclude'),
            db_include=data.get('db_include'),
            db_exclude=data.get('db_exclude'),
            query_focus=data.get('query_focus', []),
            query_types=data.get('query_types', [])
        )

@dataclass
class AnalysisResult:
    """Comprehensive analysis result combining all data sources"""
    timestamp: datetime
    query_patterns: List[QueryPattern]
    dbt_models: Dict[str, DBTModel]
    uncovered_tables: Set[str] = field(default_factory=set)
    model_coverage: Dict[str, float] = field(default_factory=dict)
    dbt_mapper: Optional[DBTModelMapper] = None
    
    def calculate_coverage(self) -> None:
        """Calculate coverage metrics with improved table matching."""
        if not self.dbt_mapper:
            logger.warning("No dbt mapper available, coverage calculation may be incomplete")
            return
            
        # Get all dbt models
        all_dbt_models = set(self.dbt_models.keys())
        if not all_dbt_models:
            logger.warning("No dbt models found")
            self.model_coverage = {
                "covered": 0.0,
                "uncovered": 0.0,
                "total_models": 0,
                "used_models": [],
                "unused_models": []
            }
            return
            
        # Track which models are used
        used_models = set()
        self.uncovered_tables = set()  # Reset uncovered tables
        
        # Process each query pattern
        for pattern in self.query_patterns:
            # Extract tables from the SQL pattern
            tables = extract_tables_from_query(pattern.sql_pattern)
            pattern.tables_accessed = tables
            
            # Try to map each table to a dbt model or source
            for table in tables:
                # Try different variations of the table name
                model_name = self.dbt_mapper.get_model_name(table)
                if model_name:
                    used_models.add(model_name)
                    pattern.dbt_models_used.add(model_name)
                    
                    # Also add any upstream models that this model depends on
                    model = self.dbt_models.get(model_name)
                    if model:
                        used_models.update(model.depends_on)
                else:
                    # Check if it's a source reference
                    for source_ref, physical_table in self.dbt_mapper.source_refs.items():
                        if (physical_table.lower() == table.lower() or 
                            physical_table.lower().endswith('.' + table.lower())):
                            # Found a source match
                            pattern.dbt_models_used.add(f"source:{source_ref}")
                            break
                    else:
                        # No model or source found
                        self.uncovered_tables.add(table)
        
        # Calculate coverage
        total_models = len(all_dbt_models)
        covered_models = len(used_models)
        uncovered_models = all_dbt_models - used_models
        
        self.model_coverage = {
            "covered": (covered_models / total_models * 100) if total_models > 0 else 0.0,
            "uncovered": (len(uncovered_models) / total_models * 100) if total_models > 0 else 0.0,
            "total_models": total_models,
            "used_models": sorted(list(used_models)),  # Sort for consistent output
            "unused_models": sorted(list(uncovered_models)),  # Sort for consistent output,
            "source_refs": sorted(list(self.dbt_mapper.source_refs.keys())) if self.dbt_mapper else []
        }
        
        logger.info(f"Coverage calculation complete: {covered_models}/{total_models} models used")
        if self.uncovered_tables:
            logger.info(f"Found {len(self.uncovered_tables)} uncovered tables")

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'timestamp': self.timestamp.isoformat(),
            'query_patterns': [pattern.to_dict() for pattern in self.query_patterns],
            'dbt_models': {name: model.to_dict() for name, model in self.dbt_models.items()},
            'uncovered_tables': list(self.uncovered_tables),
            'model_coverage': self.model_coverage,
            'dbt_mapper': self.dbt_mapper.to_dict() if self.dbt_mapper else None
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'AnalysisResult':
        """Create from dictionary"""
        result = cls(
            timestamp=datetime.fromisoformat(data['timestamp']),
            query_patterns=[QueryPattern.from_dict(pattern) for pattern in data['query_patterns']],
            dbt_models={name: DBTModel.from_dict(model) for name, model in data['dbt_models'].items()},
            uncovered_tables=set(data['uncovered_tables']),
            model_coverage=data['model_coverage']
        )
        
        # Handle dbt_mapper separately to avoid circular imports
        if data.get('dbt_mapper'):
            result.dbt_mapper = DBTModelMapper.from_dict(data['dbt_mapper'])
            
        return result

@dataclass
class AIRecommendation:
    """AI-generated recommendation for query optimization"""
    type: str
    description: str
    impact: str
    suggested_sql: Optional[str] = None
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'type': self.type,
            'description': self.description,
            'impact': self.impact,
            'suggested_sql': self.suggested_sql
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'AIRecommendation':
        """Create from dictionary"""
        return cls(
            type=data['type'],
            description=data['description'],
            impact=data['impact'],
            suggested_sql=data.get('suggested_sql')
        )
