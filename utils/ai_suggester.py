import json
from typing import List, Dict, Any
from openai import OpenAI

class AISuggester:
    def __init__(self, api_key: str):
        self.client = OpenAI(api_key=api_key)

    def estimate_tokens(self, text: str) -> int:
        """
        Estimate the number of tokens in a text string.
        This is a rough estimate - actual token count may vary.
        """
        # Rough estimation: 1 token ≈ 4 characters for English text
        return len(text) // 4

    def generate_suggestions(
        self,
        query_patterns: List[Dict[str, Any]],
        dbt_structure: Dict[str, Any],
        max_patterns: int = 5,
        max_tokens: int = 8000,
        confidence_threshold: float = 0.8,
        cache_key: str = None
    ) -> List[Dict[str, Any]]:
        """
        Generate optimization suggestions based on query patterns and dbt project structure
        with token usage control and caching
        
        Args:
            query_patterns: List of query patterns to analyze
            dbt_structure: DBT project structure
            max_patterns: Maximum number of patterns to analyze at once
            max_tokens: Maximum tokens to use per request
            confidence_threshold: Minimum confidence for suggestions
            cache_key: Key for caching results
        """
        try:
            # Sort patterns by frequency and impact
            sorted_patterns = sorted(
                query_patterns,
                key=lambda x: (x.get('frequency', 0) * x.get('avg_duration_ms', 0)),
                reverse=True
            )
            
            # Take only top patterns within token limit
            selected_patterns = []
            current_tokens = 0
            
            for pattern in sorted_patterns[:max_patterns]:
                pattern_tokens = self.estimate_tokens(str(pattern))
                if current_tokens + pattern_tokens > max_tokens:
                    break
                selected_patterns.append(pattern)
                current_tokens += pattern_tokens
            
            analysis = self._prepare_analysis(selected_patterns, dbt_structure)
            
            # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
            response = self.client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {
                        "role": "system",
                        "content": """You are an expert data engineer specializing in dbt and ClickHouse optimization with deep knowledge of data modeling best practices.
                        
                        Your task is to provide intelligent suggestions for dbt project improvement by:
                        1. Analyzing query patterns and dbt model structures to identify optimization opportunities
                        2. Recommending specific dbt modeling improvements including:
                           - Optimal materialization strategies (table, view, incremental)
                           - Efficient model grain and partitioning
                           - Smart intermediary models for reusability
                           - Proper use of dbt hooks and configurations
                        3. Suggesting ClickHouse-specific optimizations:
                           - Materialized views and projections
                           - Optimal table engine selection
                           - Partition and order by configurations
                        4. Providing implementation guidance with dbt-specific code examples
                        
                        Focus areas:
                        - dbt model architecture and dependency optimization
                        - Materialization strategy based on query patterns
                        - Performance impact vs. maintenance tradeoffs
                        - ClickHouse-specific dbt configurations
                        - Query optimization through proper model design
                        - Resource utilization and build time optimization
                        - Testing and documentation best practices"""
                    },
                    {
                        "role": "user",
                        "content": json.dumps(analysis)
                    }
                ],
                response_format={"type": "json_object"}
            )
            
            suggestions = json.loads(response.choices[0].message.content)
            return suggestions.get('suggestions', [])
            
        except Exception as e:
            raise Exception(f"Failed to generate suggestions: {str(e)}")

    def _prepare_analysis(
        self,
        query_patterns: List[Dict[str, Any]],
        dbt_structure: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Prepare analysis data for the AI model
        """
        return {
            "query_patterns": query_patterns,
            "dbt_analysis": {
                "model_count": len(dbt_structure.get('models', [])),
                "source_count": len(dbt_structure.get('sources', [])),
                "complex_models": self._identify_complex_models(dbt_structure['models']),
                "dependency_depth": self._calculate_dependency_depth(
                    dbt_structure['model_dependencies']
                ),
                "model_details": self._analyze_model_details(dbt_structure['models']),
                "project_config": dbt_structure.get('project_config', {}),
                "dependency_analysis": self._analyze_dependency_patterns(
                    dbt_structure['model_dependencies'],
                    dbt_structure['models']
                )
            }
        }

    def _identify_complex_models(self, models: List[Dict[str, Any]]) -> List[str]:
        """
        Identify complex models based on SQL complexity
        """
        complex_models = []
        for model in models:
            sql = model['raw_sql'].lower()
            complexity_score = (
                sql.count('join') +
                sql.count('window') +
                sql.count('partition by') +
                sql.count('with') * 2
            )
            if complexity_score > 5:
                complex_models.append(model['name'])
        return complex_models

    def _calculate_dependency_depth(self, dependencies: Dict[str, List[str]]) -> int:
        """
        Calculate the maximum depth of model dependencies
        """
        def get_depth(model: str, visited: set) -> int:
            if model in visited:
                return 0
            visited.add(model)
            if model not in dependencies or not dependencies[model]:
                return 0
            return 1 + max(
                (get_depth(dep, visited.copy()) for dep in dependencies[model]),
                default=0
            )

        if not dependencies:
            return 0
            
        return max(
            (get_depth(model, set()) for model in dependencies.keys()),
            default=0
        )

    def _analyze_model_details(self, models: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Analyze detailed characteristics of each dbt model
        """
        model_details = []
        for model in models:
            details = {
                'name': model['name'],
                'materialization': model.get('materialization', 'view'),
                'reference_count': len(model.get('references', [])),
                'source_count': len(model.get('sources', [])),
                'sql_complexity': {
                    'joins': model['raw_sql'].lower().count('join'),
                    'window_functions': model['raw_sql'].lower().count('over ('),
                    'subqueries': model['raw_sql'].lower().count('select'),
                    'ctes': model['raw_sql'].lower().count('with'),
                },
                'config': model.get('config', {}),
                'upstream_models': model.get('references', []),
                'sources_used': model.get('sources', [])
            }
            model_details.append(details)
        return model_details

    def _analyze_dependency_patterns(
        self,
        dependencies: Dict[str, List[str]],
        models: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """
        Analyze patterns in model dependencies and identify optimization opportunities
        """
        model_map = {m['name']: m for m in models}
        analysis = {
            'bottleneck_models': [],
            'reusability_opportunities': [],
            'materialization_suggestions': []
        }

        # Identify bottleneck models (frequently referenced)
        reference_count = {}
        for deps in dependencies.values():
            for dep in deps:
                reference_count[dep] = reference_count.get(dep, 0) + 1
        
        # Models referenced by many others might benefit from materialization
        bottlenecks = sorted(
            reference_count.items(),
            key=lambda x: x[1],
            reverse=True
        )[:5]
        
        for model_name, ref_count in bottlenecks:
            if model_name in model_map:
                model = model_map[model_name]
                analysis['bottleneck_models'].append({
                    'model': model_name,
                    'reference_count': ref_count,
                    'current_materialization': model.get('materialization', 'view'),
                    'suggestion': 'table' if ref_count > 3 else 'view'
                })

        # Identify potential reusability opportunities
        common_sources = {}
        for model in models:
            source_key = frozenset((s['source'], s['table']) for s in model.get('sources', []))
            if source_key:
                if source_key not in common_sources:
                    common_sources[source_key] = []
                common_sources[source_key].append(model['name'])

        # Suggest intermediate models for commonly used source combinations
        for sources, model_list in common_sources.items():
            if len(model_list) > 2:
                analysis['reusability_opportunities'].append({
                    'sources': list(sources),
                    'affected_models': model_list,
                    'suggestion': 'Create intermediate model'
                })

        return analysis