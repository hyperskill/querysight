from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from enum import Enum
import hashlib
import json
from utils.logger import setup_logger

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

@dataclass
class AnalysisResult:
    """Comprehensive analysis result combining all data sources"""
    timestamp: datetime
    query_patterns: List[QueryPattern]
    dbt_models: Dict[str, DBTModel]
    uncovered_tables: Set[str] = field(default_factory=set)
    model_coverage: Dict[str, float] = field(default_factory=dict)
    
    def calculate_coverage(self) -> None:
        """Calculate coverage metrics"""
        all_tables = set()
        covered_tables = set()
        
        # Collect all tables from query patterns
        for pattern in self.query_patterns:
            all_tables.update(pattern.tables_accessed)
        
        # Check which tables are covered by dbt models
        for model in self.dbt_models.values():
            covered_tables.add(model.name)
            
        self.uncovered_tables = all_tables - covered_tables
        
        # Calculate coverage percentages
        if not all_tables:
            # No tables found in query patterns
            self.model_coverage = {
                "covered": 0.0,  # No tables to cover
                "uncovered": 0.0
            }
        else:
            total_tables = len(all_tables)
            self.model_coverage = {
                "covered": len(covered_tables) / total_tables * 100,
                "uncovered": len(self.uncovered_tables) / total_tables * 100
            }

@dataclass
class AIRecommendation:
    """AI-generated recommendation for improvement"""
    id: str
    pattern_id: str
    suggestion_type: str
    description: str
    impact_score: float
    implementation_difficulty: float
    suggested_sql: Optional[str] = None
    affected_models: Set[str] = field(default_factory=set)
    estimated_benefits: Dict[str, float] = field(default_factory=dict)
    status: str = "pending"  # pending, approved, implemented, rejected
    
    @property
    def priority_score(self) -> float:
        """Calculate priority score based on impact and difficulty"""
        return self.impact_score * (1 - self.implementation_difficulty * 0.5)

    def to_dict(self) -> Dict:
        """Convert to dictionary for storage"""
        return {
            "id": self.id,
            "pattern_id": self.pattern_id,
            "suggestion_type": self.suggestion_type,
            "description": self.description,
            "impact_score": self.impact_score,
            "implementation_difficulty": self.implementation_difficulty,
            "suggested_sql": self.suggested_sql,
            "affected_models": list(self.affected_models),
            "estimated_benefits": self.estimated_benefits,
            "status": self.status
        }

    @classmethod
    def from_dict(cls, data: Dict) -> 'AIRecommendation':
        """Create instance from dictionary"""
        data = data.copy()
        data['affected_models'] = set(data['affected_models'])
        return cls(**data)
