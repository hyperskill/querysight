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

    def get_all_ancestors(self, dbt_models: Dict[str, 'DBTModel'], max_depth: int = None) -> Set[str]:
        """
        Get all ancestor models (upstream dependencies) recursively.
        
        Args:
            dbt_models: Dictionary of all DBT models
            max_depth: Maximum depth to traverse (None for unlimited)
            
        Returns:
            Set of model names that are ancestors
        """
        if not self.depends_on:
            return set()
            
        all_ancestors = set(self.depends_on)
        if max_depth is not None and max_depth <= 1:
            return all_ancestors
            
        for dep_name in self.depends_on:
            dep_model = dbt_models.get(dep_name)
            if dep_model:
                next_depth = None if max_depth is None else max_depth - 1
                ancestors = dep_model.get_all_ancestors(dbt_models, next_depth)
                all_ancestors.update(ancestors)
                
        return all_ancestors

    def get_all_descendants(self, dbt_models: Dict[str, 'DBTModel'], max_depth: int = None) -> Set[str]:
        """
        Get all descendant models (downstream) recursively.
        
        Args:
            dbt_models: Dictionary of all DBT models
            max_depth: Maximum depth to traverse (None for unlimited)
            
        Returns:
            Set of model names that are descendants
        """
        if not self.referenced_by:
            return set()
            
        all_descendants = set(self.referenced_by)
        if max_depth is not None and max_depth <= 1:
            return all_descendants
            
        for ref_name in self.referenced_by:
            ref_model = dbt_models.get(ref_name)
            if ref_model:
                next_depth = None if max_depth is None else max_depth - 1
                descendants = ref_model.get_all_descendants(dbt_models, next_depth)
                all_descendants.update(descendants)
                
        return all_descendants

    def dependency_depth(self, dbt_models: Dict[str, 'DBTModel']) -> int:
        """
        Calculate dependency depth from source (how many steps from raw data).
        Higher is further from source data.
        
        Args:
            dbt_models: Dictionary of all DBT models
            
        Returns:
            Depth from source (0 for source tables)
        """
        # Source tables (no dependencies or only source dependencies)
        if not self.depends_on or all('.' in dep for dep in self.depends_on):
            return 0
            
        # Calculate max depth of dependencies + 1
        max_dep_depth = 0
        for dep_name in self.depends_on:
            if '.' in dep_name:  # Skip source references
                continue
                
            dep_model = dbt_models.get(dep_name)
            if dep_model:
                dep_depth = dep_model.dependency_depth(dbt_models)
                max_dep_depth = max(max_dep_depth, dep_depth)
                
        return max_dep_depth + 1

    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        result = {
            'name': self.name,
            'path': self.path,
            'depends_on': list(self.depends_on),
            'referenced_by': list(self.referenced_by),
            'columns': self.columns,
            'tests': self.tests,
            'materialization': self.materialization,
            'freshness': self.freshness.total_seconds() if self.freshness else None
        }
        
        # Add new fields only if they're precomputed
        if hasattr(self, '_dependency_depth'):
            result['dependency_depth'] = self._dependency_depth
        if hasattr(self, '_impact_score'):
            result['impact_score'] = self._impact_score
            
        return result

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
    
    def __init__(self, dbt_models: Dict[str, DBTModel], 
                 dbt_mapper: Optional[DBTModelMapper] = None,
                 query_patterns: List[QueryPattern] = None):
        self.dbt_models = dbt_models
        self.dbt_mapper = dbt_mapper
        self.query_patterns = query_patterns or []
        self.model_coverage = None
        self.uncovered_tables = set()
        self.serialization_version = 2  # Increment when changing serialization format

    def calculate_coverage(self) -> None:
        """Calculate coverage metrics with enhanced dependency insights."""
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
        
        # Calculate dependency metrics
        if self.dbt_models:
            # Precompute dependency metrics for each model
            model_metrics = {}
            for name, model in self.dbt_models.items():
                depth = model.dependency_depth(self.dbt_models)
                descendants = model.get_all_descendants(self.dbt_models)
                impact_score = len(descendants) + sum(
                    1 for p in self.query_patterns 
                    if name in p.dbt_models_used
                )
                
                # Store the metrics
                model_metrics[name] = {
                    "depth": depth,
                    "impact_score": impact_score,
                    "descendant_count": len(descendants),
                    "is_used": name in used_models
                }
                
                # Cache on model object for serialization
                model._dependency_depth = depth
                model._impact_score = impact_score
            
            # Find critical models (high impact, used)
            critical_models = [
                {
                    "model": name,
                    "impact_score": metrics["impact_score"],
                    "descendant_count": metrics["descendant_count"],
                    "depth": metrics["depth"]
                }
                for name, metrics in model_metrics.items()
                if metrics["is_used"] and metrics["impact_score"] > 3  # Arbitrary threshold
            ]
            
            # Sort by impact score
            critical_models.sort(key=lambda x: x["impact_score"], reverse=True)
            
            # Find bottleneck models
            bottleneck_models = [
                {
                    "model": name,
                    "upstream_count": len(self.dbt_models[name].depends_on),
                    "downstream_count": len(self.dbt_models[name].referenced_by),
                    "is_used": metrics["is_used"]
                }
                for name, metrics in model_metrics.items()
                if len(self.dbt_models[name].depends_on) > 1 
                and len(self.dbt_models[name].referenced_by) > 1
            ]
            
            # Sort by combined upstream and downstream count
            bottleneck_models.sort(
                key=lambda x: x["upstream_count"] + x["downstream_count"], 
                reverse=True
            )
            
            # Calculate dependency metrics
            dependency_metrics = {
                "max_depth": max(m["depth"] for m in model_metrics.values()) if model_metrics else 0,
                "avg_depth": sum(m["depth"] for m in model_metrics.values()) / len(model_metrics) if model_metrics else 0,
                "critical_models": critical_models[:5],  # Top 5
                "bottleneck_models": bottleneck_models[:5]  # Top 5
            }
            
            # Update coverage dictionary
            self.model_coverage = {
                "covered": (covered_models / total_models * 100) if total_models > 0 else 0.0,
                "uncovered": (len(uncovered_models) / total_models * 100) if total_models > 0 else 0.0,
                "total_models": total_models,
                "used_models": sorted(list(used_models)),  # Sort for consistent output
                "unused_models": sorted(list(uncovered_models)),  # Sort for consistent output,
                "source_refs": sorted(list(self.dbt_mapper.source_refs.keys())) if self.dbt_mapper else [],
                "dependency_metrics": dependency_metrics
            }
        else:
            # Original coverage dictionary without dependency metrics
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

    def get_dependency_context(self, model_name: str) -> Dict[str, any]:
        """
        Get comprehensive dependency context for a specific model.
        
        This information can be used by the AI suggester to provide more
        targeted recommendations regarding model dependencies.
        
        Args:
            model_name: Name of the model to analyze
            
        Returns:
            Dictionary with dependency context
        """
        if model_name not in self.dbt_models:
            return {
                "model": model_name,
                "found": False,
                "error": "Model not found"
            }
            
        model = self.dbt_models[model_name]
        
        # Get direct dependencies
        direct_deps = set(model.depends_on)
        direct_refs = set(model.referenced_by)
        
        # Get all ancestors and descendants
        all_ancestors = model.get_all_ancestors(self.dbt_models)
        all_descendants = model.get_all_descendants(self.dbt_models)
        
        # Get depth metrics
        depth = model.dependency_depth(self.dbt_models)
        
        # Get query patterns that use this model
        patterns_using_model = [
            p.pattern_id for p in self.query_patterns 
            if model_name in p.dbt_models_used
        ]
        
        # Calculate criticality score (higher = more critical)
        criticality = len(all_descendants) + len(patterns_using_model)
        
        return {
            "model": model_name,
            "found": True,
            "depth": depth,
            "direct_dependencies": sorted(list(direct_deps)),
            "direct_dependents": sorted(list(direct_refs)),
            "all_ancestors": sorted(list(all_ancestors)),
            "all_descendants": sorted(list(all_descendants)),
            "patterns_using_model": patterns_using_model,
            "criticality_score": criticality,
            "materialization": model.materialization
        }
        
    def get_critical_path(self, source_model: str, target_model: str) -> List[str]:
        """
        Find the dependency path between source and target models.
        
        Args:
            source_model: Starting model name
            target_model: Ending model name
            
        Returns:
            List of model names forming the path, or empty list if no path exists
        """
        if source_model not in self.dbt_models or target_model not in self.dbt_models:
            return []
            
        # Use breadth-first search to find shortest path
        visited = {source_model}
        queue = [(source_model, [source_model])]
        
        while queue:
            current, path = queue.pop(0)
            
            # Check if we've reached target
            if current == target_model:
                return path
                
            # Add unvisited neighbors
            model = self.dbt_models.get(current)
            if model:
                # Check upstream dependencies
                for dep in model.depends_on:
                    if dep not in visited and dep in self.dbt_models:
                        visited.add(dep)
                        queue.append((dep, path + [dep]))
                        
                # Check downstream dependents
                for ref in model.referenced_by:
                    if ref not in visited and ref in self.dbt_models:
                        visited.add(ref)
                        queue.append((ref, path + [ref]))
                        
        return []  # No path found

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
    pattern_metadata: Optional[Dict] = None  # Added field for pattern metadata
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'type': self.type,
            'description': self.description,
            'impact': self.impact,
            'suggested_sql': self.suggested_sql,
            'pattern_metadata': self.pattern_metadata
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'AIRecommendation':
        """Create from dictionary"""
        return cls(
            type=data['type'],
            description=data['description'],
            impact=data['impact'],
            suggested_sql=data.get('suggested_sql'),
            pattern_metadata=data.get('pattern_metadata')
        )
