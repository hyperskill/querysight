from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from enum import Enum
import hashlib
import json
from utils.logger import setup_logger
from .sql_parser import extract_tables_from_query
from .dbt_mapper import DBTModelMapper

logger = setup_logger(__name__)

class QueryType(Enum):
    SELECT = "SELECT"
    INSERT = "INSERT"
    CREATE = "CREATE"
    ALTER = "ALTER"
    DROP = "DROP"
    OTHER = "OTHER"

class QueryFocus(Enum):
    SLOW = "Slow Queries"
    FREQUENT = "Frequent Queries"
    ALL = "All Queries"

@dataclass
class QueryLog:
    """Raw query log entry from ClickHouse"""
    query_id: str
    query: str
    type: str
    user: str
    query_start_time: datetime
    query_duration_ms: float
    read_rows: int
    read_bytes: int
    result_rows: int
    result_bytes: int
    memory_usage: int
    normalized_query_hash: str

    @classmethod
    def from_dict(cls, data: Dict) -> 'QueryLog':
        """Create QueryLog from dictionary, handling datetime conversion"""
        if isinstance(data.get('query_start_time'), str):
            data['query_start_time'] = datetime.fromisoformat(data['query_start_time'])
        return cls(**data)
        
    def to_dict(self) -> Dict:
        """Convert QueryLog to dictionary for caching"""
        return {
            'query_id': self.query_id,
            'query': self.query,
            'type': self.type,
            'user': self.user,
            'query_start_time': self.query_start_time.isoformat(),  # Convert datetime to string
            'query_duration_ms': self.query_duration_ms,
            'read_rows': self.read_rows,
            'read_bytes': self.read_bytes,
            'result_rows': self.result_rows,
            'result_bytes': self.result_bytes,
            'memory_usage': self.memory_usage,
            'normalized_query_hash': self.normalized_query_hash
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
    dbt_models_used: Set[str] = field(default_factory=set)  # New field for dbt models
    
    @property
    def complexity_score(self) -> float:
        """Calculate complexity score based on pattern metrics"""
        duration_weight = min(self.avg_duration_ms / 1000, 1.0)  # Cap at 1.0
        frequency_weight = min(self.frequency / 100, 1.0)  # Cap at 1.0
        table_weight = min(len(self.tables_accessed) / 5, 1.0)  # Cap at 1.0
        
        return (duration_weight * 0.4 + 
                frequency_weight * 0.4 + 
                table_weight * 0.2)

    def update_from_log(self, log: QueryLog) -> None:
        """Update pattern metrics from a new log entry"""
        self.frequency += 1
        self.total_duration_ms += log.query_duration_ms
        self.avg_duration_ms = self.total_duration_ms / self.frequency
        
        if not self.first_seen or log.query_start_time < self.first_seen:
            self.first_seen = log.query_start_time
        if not self.last_seen or log.query_start_time > self.last_seen:
            self.last_seen = log.query_start_time
            
        self.users.add(log.user)

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
            'dbt_models_used': list(self.dbt_models_used)
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
            dbt_models_used=set(data['dbt_models_used'])
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
                "total_models": 0
            }
            return
            
        # Track which models are used
        used_models = set()
        
        # Process each query pattern
        for pattern in self.query_patterns:
            # Extract tables from the SQL pattern
            tables = extract_tables_from_query(pattern.sql_pattern)
            
            # Update tables accessed
            pattern.tables_accessed = tables
            
            # Try to map each table to a dbt model
            for table in tables:
                # Try different variations of the table name
                model_name = self.dbt_mapper.get_model_name(table)
                if model_name:
                    used_models.add(model_name)
                    pattern.dbt_models_used.add(model_name)
        
        # Calculate coverage
        total_models = len(all_dbt_models)
        covered_models = len(used_models)
        self.uncovered_tables = all_dbt_models - used_models
        
        self.model_coverage = {
            "covered": (covered_models / total_models * 100) if total_models > 0 else 0.0,
            "uncovered": (len(self.uncovered_tables) / total_models * 100) if total_models > 0 else 0.0,
            "total_models": total_models,
            "used_models": list(used_models),  # Add list of actually used models
            "unused_models": list(self.uncovered_tables)  # Add list of unused models
        }
        
        logger.info(f"Coverage calculation complete: {covered_models}/{total_models} models used")

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
        return cls(
            timestamp=datetime.fromisoformat(data['timestamp']),
            query_patterns=[QueryPattern.from_dict(pattern) for pattern in data['query_patterns']],
            dbt_models={name: DBTModel.from_dict(model) for name, model in data['dbt_models'].items()},
            uncovered_tables=set(data['uncovered_tables']),
            model_coverage=data['model_coverage'],
            dbt_mapper=DBTModelMapper.from_dict(data['dbt_mapper']) if data.get('dbt_mapper') else None
        )

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
