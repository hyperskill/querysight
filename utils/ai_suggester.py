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
    
    def __init__(self, api_key: str):
        self.api_key = api_key
        self.client = OpenAI(api_key=api_key)

    def _create_prompt(self, pattern: QueryPattern, dbt_models: Dict[str, DBTModel]) -> str:
        """Create a concise prompt for the AI model"""
        
        # Get DBT model information for referenced tables
        model_info = []
        for table in pattern.tables_accessed:
            if table in pattern.dbt_models_used:
                model = dbt_models.get(table)
                if model:
                    model_info.append(
                        f"- {table}: materialized as {model.materialization}, "
                        f"depends on [{', '.join(model.depends_on)}], "
                        f"referenced by [{', '.join(model.referenced_by)}]"
                    )
            else:
                model_info.append(f"- {table}: unmapped table")
        
        # Detect query pattern type
        sql_lower = pattern.sql_pattern.lower()
        pattern_type = ""
        if "group by" in sql_lower:
            pattern_type += "Aggregation|"
        elif "join" in sql_lower:
            pattern_type += "Join|"
        elif "where" in sql_lower:
            pattern_type += "Filter|"
        elif "select" in sql_lower and pattern_type == "":
            pattern_type += "Simple Select"
        
        prompt = (
            f"Analyze this SQL query pattern:\n"
            f"Pattern Type: {pattern_type}\n"
            f"Frequency: {pattern.frequency} executions\n"
            f"Avg Duration: {pattern.avg_duration_ms}ms\n"
            f"Memory Usage: {pattern.memory_usage} bytes\n"
            f"Query Template:\n{pattern.sql_pattern}\n\n"
            f"Tables Referenced:\n" + "\n".join(model_info) + "\n\n"
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
            "Type: [INDEX|MATERIALIZATION|REWRITE]\n"
            "Description: [1-2 sentences]\n"
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
                    model="gpt-3.5-turbo",
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
                
                recommendations.append(AIRecommendation(
                    type=rec_type,
                    description=description,
                    impact=impact,
                    suggested_sql=sql
                ))
                
            except Exception as e:
                logger.error(f"Error generating suggestions: {str(e)}")
                continue
        
        return recommendations