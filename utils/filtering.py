"""Utilities for filtering query patterns and analysis results."""

from typing import Dict, List, Set, Optional
from .models import QueryPattern

def filter_patterns(patterns: List[QueryPattern], criteria: Dict) -> List[QueryPattern]:
    """Filter patterns based on various criteria.
    
    Args:
        patterns: List of QueryPattern objects to filter
        criteria: Dictionary of filter criteria, which may include:
            - pattern_ids: List of pattern IDs to include
            - min_duration: Minimum average duration in ms
            - min_frequency: Minimum query frequency
            - tables: Set of table names to filter by
            - dbt_models: Set of DBT model names to filter by
            
    Returns:
        List of QueryPattern objects that match all specified criteria
    """
    filtered = patterns
    
    # Filter by pattern IDs
    if 'pattern_ids' in criteria:
        pattern_ids = set(criteria['pattern_ids'])
        filtered = [p for p in filtered if p.pattern_id in pattern_ids]
    
    # Filter by duration
    if 'min_duration' in criteria:
        filtered = [p for p in filtered if p.avg_duration_ms >= criteria['min_duration']]
    
    # Filter by frequency
    if 'min_frequency' in criteria:
        filtered = [p for p in filtered if p.frequency >= criteria['min_frequency']]
        
    # Filter by tables
    if 'tables' in criteria:
        tables = set(criteria['tables'])
        filtered = [p for p in filtered if tables & p.tables_accessed]
    
    # Filter by DBT models
    if 'dbt_models' in criteria:
        models = set(criteria['dbt_models'])
        filtered = [p for p in filtered if models & p.dbt_models_used]
    
    return filtered
