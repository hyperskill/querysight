from typing import List, Dict, Any, Optional
from openai import OpenAI
import json
import hashlib
from datetime import datetime
from .models import (
    QueryPattern,
    DBTModel,
    AIRecommendation,
    AnalysisResult
)
from .logger import setup_logger

logger = setup_logger(__name__)

class AISuggester:
    """AI-powered query optimization suggester"""
    
    def __init__(self, api_key: str, model: str):
        self.api_key = api_key
        self.client = OpenAI(api_key=api_key)
        self.model = model

    def _create_prompt(self, pattern: QueryPattern, dbt_models: Dict[str, DBTModel]) -> str:
        """Create a concise prompt focusing on model lists in JSON format"""
        # Get unmapped tables
        unmapped_tables = pattern.tables_accessed - pattern.dbt_models_used
        
        # Get model details in a structured format
        mapped_models = []
        for model_name in pattern.dbt_models_used:
            model = dbt_models.get(model_name)
            if model:
                mapped_models.append({
                    "name": model_name,
                    "materialization": model.materialization,
                    "dependencies": list(model.depends_on),
                    "referenced_by": list(model.referenced_by)
                })
        
        # Detect query pattern type
        sql_lower = pattern.sql_pattern.lower()
        pattern_types = []
        if "group by" in sql_lower:
            pattern_types.append("Aggregation")
        if "join" in sql_lower:
            pattern_types.append("Join")
        if "where" in sql_lower:
            pattern_types.append("Filter")
        if not pattern_types and "select" in sql_lower:
            pattern_types.append("Simple Select")
        
        # Create JSON structure for the prompt
        context = {
            "query": {
                "pattern_type": pattern_types,
                "frequency": pattern.frequency,
                "avg_duration_ms": pattern.avg_duration_ms,
                "memory_usage": pattern.memory_usage,
                "sql": pattern.sql_pattern
            },
            "models": {
                "mapped": mapped_models,
                "unmapped": list(unmapped_tables)
            }
        }
        
        # Convert to formatted JSON string
        context_json = json.dumps(context, indent=2)
        
        prompt = (
            f"Analyze this query pattern. Below is a JSON structure containing:\n"
            f"1. Query information including pattern type, metrics, and SQL\n"
            f"2. Lists of mapped DBT models with their properties\n"
            f"3. List of unmapped tables\n\n"
            f"{context_json}\n\n"
            "Suggest ONE specific optimization focusing on performance. Consider:\n"
            "1. For high-frequency queries (>100/day), consider materialization\n"
            "   - Especially if the query involves complex aggregations\n"
            "   - Check if referenced tables are already materialized\n"
            "2. For complex joins or subqueries, consider query rewrites\n"
            "   - Look for opportunities to simplify joins\n"
            "   - Consider pushing down predicates\n"
            "   - Check for redundant subqueries\n"
            "3. For simple lookups or filters, consider indexes\n"
            "   - Particularly on frequently filtered columns\n"
            "   - For join conditions\n\n"
            "Format response as:\n"
            "Type: [INDEX|MATERIALIZATION|REWRITE|NEW_DBT_MODEL|NEW_DBT_MACRO]\n"
            "Description: [1-4 sentences]\n"
            "Impact: [HIGH|MEDIUM|LOW]\n"
            "SQL: [optional improved query]"
        )
        return prompt
        
    def generate_recommendations(
        self, 
        patterns: List[QueryPattern],
        dbt_models: Dict[str, DBTModel]
    ) -> List[AIRecommendation]:
        """Generate optimization recommendations for query patterns"""
        recommendations = []
        
        for pattern in patterns:
            try:
                prompt = self._create_prompt(pattern, dbt_models)
                
                response = self.client.chat.completions.create(
                    model=self.model,
                    messages=[
                        {"role": "system", "content": "You are a SQL optimization expert. Provide brief, actionable recommendations."},
                        {"role": "user", "content": prompt}
                    ],
                    max_tokens=150,
                    temperature=0.7
                )
                
                # Parse response into structured format
                suggestion = response.choices[0].message.content.strip()
                parts = suggestion.split('\n')
                
                rec_type = parts[0].split(': ')[1] if len(parts) > 0 else "UNKNOWN"
                description = parts[1].split(': ')[1] if len(parts) > 1 else "No description provided"
                impact = parts[2].split(': ')[1] if len(parts) > 2 else "UNKNOWN"
                sql = parts[3].split(': ')[1] if len(parts) > 3 and 'SQL: ' in parts[3] else None
                
                # Create pattern metadata dictionary
                pattern_metadata = {
                    'pattern_id': pattern.pattern_id,
                    'sql_pattern': pattern.sql_pattern,
                    'frequency': pattern.frequency,
                    'avg_duration_ms': pattern.avg_duration_ms,
                    'memory_usage': pattern.memory_usage,
                    'total_read_rows': pattern.total_read_rows,
                    'total_read_bytes': pattern.total_read_bytes,
                    'tables_accessed': list(pattern.tables_accessed),
                    'dbt_models_used': list(pattern.dbt_models_used),
                    'first_seen': pattern.first_seen.isoformat() if pattern.first_seen else None,
                    'last_seen': pattern.last_seen.isoformat() if pattern.last_seen else None,
                    'users': list(pattern.users),
                    'complexity_score': pattern.complexity_score
                }
                
                recommendations.append(AIRecommendation(
                    type=rec_type,
                    description=description,
                    impact=impact,
                    suggested_sql=sql,
                    pattern_metadata=pattern_metadata
                ))
                
            except Exception as e:
                logger.error(f"Error generating suggestions: {str(e)}")
                continue
        
        return recommendations