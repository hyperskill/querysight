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
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

    def generate_suggestions(
        self,
        analysis_result: AnalysisResult,
        max_patterns: int = 5,
        max_tokens: int = 8000,
        confidence_threshold: float = 0.8
    ) -> List[AIRecommendation]:
        """
        Generate optimization suggestions based on analysis results
        """
        try:
            # Sort patterns by impact score
            sorted_patterns = sorted(
                analysis_result.query_patterns,
                key=lambda x: (x.frequency * x.avg_duration_ms),
                reverse=True
            )[:max_patterns]

            suggestions = []
            for pattern in sorted_patterns:
                # Prepare context for AI
                context = self._prepare_pattern_context(pattern, analysis_result)
                
                # Generate suggestion using AI
                response = self.client.chat.completions.create(
                    model="gpt-4",
                    messages=[
                        {"role": "system", "content": """You are an expert data engineer specializing in dbt and SQL optimization.
                         Analyze the query pattern and suggest improvements. Focus on:
                         1. Potential new dbt models
                         2. Performance optimizations
                         3. Code structure improvements"""},
                        {"role": "user", "content": json.dumps(context)}
                    ],
                    max_tokens=max_tokens,
                    temperature=0.2
                )

                # Parse AI response
                suggestion = self._parse_ai_response(response.choices[0].message.content, pattern.pattern_id)
                
                # Only include high-confidence suggestions
                if suggestion.impact_score >= confidence_threshold:
                    suggestions.append(suggestion)

            return suggestions

        except Exception as e:
            logger.error(f"Error generating suggestions: {str(e)}")
            return []

    def _prepare_pattern_context(
        self,
        pattern: QueryPattern,
        analysis: AnalysisResult
    ) -> Dict[str, Any]:
        """Prepare context for AI analysis"""
        # Find related models
        related_models = [
            model for model in analysis.dbt_models.values()
            if model.name in pattern.tables_accessed
        ]

        return {
            "query_pattern": {
                "sql": pattern.sql_pattern,
                "frequency": pattern.frequency,
                "avg_duration_ms": pattern.avg_duration_ms,
                "tables": list(pattern.tables_accessed),
                "complexity_score": pattern.complexity_score
            },
            "dbt_coverage": {
                "covered_tables": [m.name for m in related_models],
                "uncovered_tables": list(analysis.uncovered_tables),
                "coverage_metrics": analysis.model_coverage
            },
            "existing_models": [
                {
                    "name": model.name,
                    "materialization": model.materialization,
                    "columns": model.columns,
                    "dependencies": list(model.depends_on)
                }
                for model in related_models
            ]
        }

    def _parse_ai_response(self, response_text: str, pattern_id: str) -> AIRecommendation:
        """Parse AI response into structured recommendation"""
        try:
            # Parse the response into sections
            sections = response_text.split("\n\n")
            
            # Extract key information
            suggestion_type = self._extract_suggestion_type(sections[0])
            description = sections[1] if len(sections) > 1 else ""
            sql = self._extract_sql(response_text)
            
            # Generate a unique ID for the recommendation
            recommendation_id = hashlib.md5(
                f"{pattern_id}_{suggestion_type}_{datetime.now().isoformat()}".encode()
            ).hexdigest()

            # Calculate impact and difficulty scores
            impact_score = self._calculate_impact_score(sections)
            implementation_difficulty = self._calculate_difficulty_score(sections)

            return AIRecommendation(
                id=recommendation_id,
                pattern_id=pattern_id,
                suggestion_type=suggestion_type,
                description=description,
                impact_score=impact_score,
                implementation_difficulty=implementation_difficulty,
                suggested_sql=sql,
                affected_models=set(),  # To be filled by caller
                estimated_benefits={
                    "performance_improvement": impact_score * 100,
                    "maintenance_improvement": (1 - implementation_difficulty) * 100
                },
                status="pending"
            )

        except Exception as e:
            logger.error(f"Error parsing AI response: {str(e)}")
            return None

    def _extract_suggestion_type(self, text: str) -> str:
        """Extract suggestion type from text"""
        if "new model" in text.lower():
            return "new_model"
        elif "optimization" in text.lower():
            return "optimization"
        elif "refactor" in text.lower():
            return "refactor"
        return "other"

    def _extract_sql(self, text: str) -> Optional[str]:
        """Extract SQL from response text"""
        import re
        sql_match = re.search(r"```sql\n(.*?)\n```", text, re.DOTALL)
        return sql_match.group(1) if sql_match else None

    def _calculate_impact_score(self, sections: List[str]) -> float:
        """Calculate impact score based on AI response"""
        impact_indicators = {
            "significant": 1.0,
            "high": 0.8,
            "medium": 0.5,
            "low": 0.3
        }
        
        text = " ".join(sections).lower()
        for indicator, score in impact_indicators.items():
            if indicator in text:
                return score
        return 0.5  # Default score

    def _calculate_difficulty_score(self, sections: List[str]) -> float:
        """Calculate implementation difficulty score"""
        difficulty_indicators = {
            "complex": 1.0,
            "difficult": 0.8,
            "moderate": 0.5,
            "simple": 0.3,
            "straightforward": 0.2
        }
        
        text = " ".join(sections).lower()
        for indicator, score in difficulty_indicators.items():
            if indicator in text:
                return score
        return 0.5  # Default score