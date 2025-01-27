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
        prompt = (
            f"Analyze this SQL query pattern:\n"
            f"Frequency: {pattern.frequency}\n"
            f"Avg Duration: {pattern.avg_duration_ms}ms\n"
            f"Query Template:\n{pattern.sql_pattern}\n\n"
            f"Tables Referenced: {', '.join(pattern.tables_accessed)}\n\n"
            "Suggest ONE specific optimization focusing on performance. "
            "Keep the response under 100 tokens. Format:\n"
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