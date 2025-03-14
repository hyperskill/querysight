from typing import List, Dict, Any, Optional
from litellm import completion
import json
import hashlib
from datetime import datetime
import os
from .models import (
    QueryPattern,
    DBTModel,
    AIRecommendation,
    AnalysisResult
)
from .logger import setup_logger
from .config import Config

logger = setup_logger(__name__)

class AISuggester:
    """AI-powered query optimization suggester"""
    
    def __init__(self, data_acquisition=None):
        self.model = Config.LLM_MODEL
        if hasattr(Config, 'OPENAI_API_KEY') and Config.OPENAI_API_KEY:
            os.environ["OPENAI_API_KEY"] = Config.OPENAI_API_KEY
        if hasattr(Config, 'ANTHROPIC_API_KEY') and Config.ANTHROPIC_API_KEY:
            os.environ["ANTHROPIC_API_KEY"] = Config.ANTHROPIC_API_KEY
        if hasattr(Config, 'HUGGINGFACE_API_KEY') and Config.HUGGINGFACE_API_KEY:
            os.environ["HUGGINGFACE_API_KEY"] = Config.HUGGINGFACE_API_KEY
        if hasattr(Config, 'DEEPSEEK_API_KEY') and Config.DEEPSEEK_API_KEY:
            os.environ["DEEPSEEK_API_KEY"] = Config.DEEPSEEK_API_KEY
        if hasattr(Config, 'LITELLM_API_KEY') and Config.LITELLM_API_KEY:
            os.environ["LITELLM_API_KEY"] = Config.LITELLM_API_KEY
            
        self.data_acquisition = data_acquisition

    def _create_prompt(self, pattern: QueryPattern, dbt_models: Dict[str, DBTModel]) -> str:
        """Create a detailed prompt with comprehensive query and model analysis context"""
        # Identify system and user tables
        SYSTEM_SCHEMAS = {'system', 'information_schema', 'pg_catalog'}
        
        def is_system_table(table_name: str) -> bool:
            return any(table_name.lower().startswith(f"{schema}.") for schema in SYSTEM_SCHEMAS)
        
        # Separate tables into system and user tables
        system_tables = {table for table in pattern.tables_accessed if is_system_table(table)}
        user_tables = {table for table in pattern.tables_accessed if not is_system_table(table)}
        
        # If only system tables are accessed with no user tables, skip this pattern
        if not user_tables:
            return None
            
        # Get unmapped tables (only from user tables)
        unmapped_tables = user_tables - pattern.dbt_models_used
        
        # Get table schemas if data_acquisition is available
        table_schemas = {}
        if self.data_acquisition:
            for table in user_tables:
                try:
                    schema = self.data_acquisition.get_table_schema(table)
                    table_schemas[table] = schema
                except Exception as e:
                    logger.warning(f"Could not get schema for table {table}: {str(e)}")
        
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
        
        # Enhanced pattern type detection
        sql_lower = pattern.sql_pattern.lower()
        pattern_types = []
        complexity_indicators = {
            "group by": "Aggregation",
            "join": "Join",
            "where": "Filter",
            "with": "CTE",
            "union": "SetOperation",
            "window": "Window",
            "having": "ComplexFilter",
            "order by": "Sorting"
        }
        
        for indicator, pattern_type in complexity_indicators.items():
            if indicator in sql_lower:
                pattern_types.append(pattern_type)
        
        if not pattern_types and "select" in sql_lower:
            pattern_types.append("Simple Select")
        
        # Calculate performance metrics
        is_high_frequency = pattern.frequency > 100
        is_long_running = pattern.avg_duration_ms > 1000
        memory_mb = pattern.memory_usage / (1024 * 1024) if pattern.memory_usage else 0
        
        # Format table schemas for better readability
        formatted_schemas = {}
        for table, schema in table_schemas.items():
            formatted_schemas[table] = {
                'columns': [
                    {
                        'name': col['name'],
                        'type': col['type'],
                        'comment': col['comment'] if col['comment'] else None,
                        'default': col['default_expression'] if col['default_expression'] else None
                    }
                    for col in schema
                ],
                'column_count': len(schema),
                'has_comments': any(col['comment'] for col in schema),
                'data_types': sorted(set(col['type'] for col in schema))
            }
            
        # Create enhanced JSON structure
        context = {
            "accessed_table_schemas": formatted_schemas,
            "query_analysis": {
                "pattern_types": pattern_types,
                "table_classification": {
                    "user_tables": list(user_tables),
                    "system_tables": list(system_tables),  # Include for context but not for optimization
                    "has_system_joins": bool(system_tables)
                },
                "performance_metrics": {
                    "frequency_per_day": pattern.frequency,
                    "avg_duration_ms": pattern.avg_duration_ms,
                    "memory_usage_mb": memory_mb,
                    "total_read_rows": pattern.total_read_rows,
                    "total_read_bytes": pattern.total_read_bytes
                },
                "usage_patterns": {
                    "is_high_frequency": is_high_frequency,
                    "is_long_running": is_long_running,
                    "first_seen": pattern.first_seen.isoformat() if pattern.first_seen else None,
                    "last_seen": pattern.last_seen.isoformat() if pattern.last_seen else None,
                    "users": list(pattern.users)
                },
                "sql_pattern": pattern.sql_pattern
            },
            "dbt_context": {
                "mapped_models": mapped_models,
                "unmapped_tables": list(unmapped_tables),  # Only user tables
                "total_user_tables": len(user_tables),
                "mapping_coverage": len(pattern.dbt_models_used) / len(user_tables) if user_tables else 0
            }
        }
        
        # Convert to formatted JSON string
        context_json = json.dumps(context, indent=2)
        
        prompt = (
            f"## QUERY PATTERN ANALYSIS REQUEST\n\n"
            f"Analyze the following query pattern and provide optimization recommendations. "
            f"The data below includes:\n"
            f"1. Comprehensive query analysis (pattern types, performance metrics, usage patterns)\n"
            f"2. Current dbt model coverage and relationships\n"
            f"3. Tables classification (user vs system tables)\n\n"
            f"```json\n{context_json}\n```\n\n"
            f"## SCHEMA ANALYSIS\n\n"
            f"Tables involved in this query pattern:\n"
            + ''.join(
                f"\n{table}:\n"
                f"  - Columns: {formatted_schemas[table]['column_count']}\n"
                f"  - Types: {', '.join(formatted_schemas[table]['data_types'])}\n"
                f"  - Has column comments: {'Yes' if formatted_schemas[table]['has_comments'] else 'No'}\n"
                for table in formatted_schemas
            ) + "\n"
            + f"\n## OPTIMIZATION CONSIDERATIONS\n\n"
            f"1. Performance Optimization:\n"
            f"   - Query shows {pattern.frequency} executions per day ({'high' if is_high_frequency else 'moderate/low'} frequency)\n"
            f"   - Average duration: {pattern.avg_duration_ms:.2f}ms ({'concerning' if is_long_running else 'acceptable'})\n"
            f"   - Memory usage: {memory_mb:.2f}MB\n"
            f"   - {'Includes joins with system tables' if any(table.lower().startswith(schema + '.') for table in pattern.tables_accessed for schema in ['system', 'information_schema', 'pg_catalog']) else 'No system table dependencies'}\n\n"
            f"2. Schema-Based Optimization:\n"
            + ''.join(
                f"   - {table}:\n"
                f"     * Column count: {formatted_schemas[table]['column_count']} (consider indexing or column pruning)\n"
                f"     * Data types: {', '.join(formatted_schemas[table]['data_types'])} (check for type-specific optimizations)\n"
                f"     * Documentation: {'Has comments' if formatted_schemas[table]['has_comments'] else 'Missing comments'} (review for business context)\n"
                f"     * Key columns: {', '.join(col['name'] for col in formatted_schemas[table]['columns'] if col['name'].lower().endswith('_id') or col['name'].lower() in ['id', 'key'])}\n"
                for table in formatted_schemas
            ) + "\n"
            f"3. Model Coverage:\n"
            f"   - User tables: {len(user_tables)} ({len(pattern.dbt_models_used)} mapped to dbt models)\n"
            f"   - System tables: {len(system_tables)} (excluded from optimization)\n"
            f"   - Unmapped user tables: {len(unmapped_tables)}\n\n"
            f"IMPORTANT: System tables (system.*, information_schema.*, pg_catalog.*) are part of the database engine "
            f"and MUST NOT be targets for dbt modeling or optimization. Focus optimization efforts only on user tables.\n\n"
            f"Based on these metrics, provide ONE specific, high-impact recommendation for user tables only.\n\n"
            f"If you have unmapped user tables and know their schema, prioritize creating a new dbt model for them, code and schema documentation for schema.yml\n\n"
            f"IMPORTANT: Don't assume existance of parent models when creating new dbt models if you don't know about them and data is not provided\n\n"
            f"## RESPONSE FORMAT\n"
            f"Type: [INDEX|REWRITE_QUERY|NEW_DBT_MODEL|NEW_DBT_MACRO]\n"
            f"Description: [Clear, specific implementation steps]\n"
            f"Impact: [HIGH|MEDIUM|LOW]\n"
            f"SQL: [Improved query or model definition if applicable]\n"
            f"Implementation: [Step-by-step guide if complex changes are needed]\n"
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
                
                if prompt is None:
                    continue
                
                try:
                    response = completion(
                        model=self.model,
                        messages=[
                            {
                                "role": "system",
                                "content": """YOU ARE A WORLD-CLASS SQL AND DBT OPTIMIZATION ADVISOR FOR **QUERYSIGHT**, SPECIALIZING IN HIGH-PERFORMANCE DATA WAREHOUSE TUNING AND SCALABLE DBT MODELING. YOUR EXPERTISE SPANS:  

1. **CLICKHOUSE QUERY OPTIMIZATION** – Enhancing execution speed, indexing strategies, and partitioning.  
2. **DBT MODEL DESIGN & MATERIALIZATION** – Optimizing model structure, incremental logic, and caching strategies.  
3. **DATA WAREHOUSE PERFORMANCE TUNING** – Minimizing resource consumption while maintaining data integrity.  
4. **SQL QUERY PATTERN ANALYSIS** – Identifying inefficient patterns and proposing structured refactors.  

## **GUIDING PRINCIPLES FOR RECOMMENDATIONS**  
YOUR RECOMMENDATIONS MUST BE:  
✅ **ACTIONABLE & SPECIFIC** – Provide precise code snippets or clear implementation steps.  
✅ **PERFORMANCE-ORIENTED** – Improve query efficiency while ensuring data consistency.  
✅ **STRATEGIC & BALANCED** – Consider trade-offs between materialization and query complexity.  
✅ **ALIGNED WITH DBT BEST PRACTICES** – Follow established modeling conventions and structure.  
✅ **USAGE-AWARE** – Optimize based on query frequency, duration, and system resource impact.  

## **OPTIMIZING UNMAPPED ENTITIES (TABLES WITHOUT DBT MODELS)**  
WHEN IDENTIFYING OPPORTUNITIES FOR DBT MODELING:  
- DETECT **HIGHLY QUERIED TABLES** THAT WOULD BENEFIT FROM STRUCTURED MODELING.  
- PROPOSE **NEW MODELS BASED ON QUERY PATTERNS & BUSINESS LOGIC**.  
- SUGGEST **APPROPRIATE MODEL TYPES** (STAGING, INTERMEDIATE, MART).  
- RECOMMEND **MATERIALIZATION STRATEGIES** (VIEW, TABLE, INCREMENTAL) BASED ON QUERY USAGE.  
- CONSIDER **DEPENDENCIES & MODEL GRAIN** TO ENSURE DATA INTEGRITY.  
- PROVIDE **NAMING & ORGANIZATION BEST PRACTICES** FOR SCALABILITY.  

## **WHEN PROPOSING NEW DBT MODELS, INCLUDE:**  
✔ **MODEL SPECIFICATION** (NAME, TYPE, MATERIALIZATION STRATEGY).  
✔ **KEY TRANSFORMATIONS & BUSINESS LOGIC** (EXAMPLES WHERE NECESSARY).  
✔ **PRIMARY & FOREIGN KEYS** (TO MAINTAIN RELATIONAL INTEGRITY).  
✔ **SOURCE TABLES & UPSTREAM MODELS** (TO FIT INTO THE EXISTING DAG).  
✔ **POTENTIAL IMPACT ON EXISTING MODELS** (AVOIDING DAG BOTTLENECKS).  

## **STRICT AVOIDANCE RULES**  
🚫 **NEVER PROPOSE OPTIMIZATIONS OR DBT MODELING FOR SYSTEM SCHEMA TABLES.**  
   - **DO NOT** analyze, optimize, or refactor system-managed metadata tables.  
   - **DO NOT** suggest including system tables (e.g., `system.*`, `pg_catalog.*`, `information_schema.*`) in dbt models.  
   - **FOCUS ONLY** on user-managed datasets that align with business logic and analytical use cases.  

## **WHAT NOT TO DO:**  
🚫 NEVER GIVE GENERIC, NON-ACTIONABLE ADVICE. ALWAYS PROVIDE SPECIFIC, IMPLEMENTABLE RECOMMENDATIONS.  
🚫 NEVER VIOLATE DBT BEST PRACTICES OR PROPOSE INCONSISTENT MODELING STRATEGIES.  
🚫 NEVER OVERLOOK PERFORMANCE TRADE-OFFS – ENSURE EVERY SUGGESTION IS RESOURCE-EFFICIENT.  
🚫 NEVER IGNORE USAGE PATTERNS – ADVICE MUST ALIGN WITH FREQUENCY, DURATION, AND SYSTEM IMPACT.  
🚫 NEVER SUGGEST REDUNDANT OR UNNECESSARY MODELS – EVERY PROPOSAL MUST SERVE A CLEAR BUSINESS NEED.  

## **FINAL EXPECTATIONS:**  
- **KEEP RESPONSES CONCISE, TECHNICAL, AND IMPLEMENTATION-FOCUSED.**  
- **STRUCTURE RECOMMENDATIONS CLEARLY FOR EASY IMPLEMENTATION.**  
- **ENSURE EVERY PROPOSAL ENHANCES PERFORMANCE & MAINTAINS DATA INTEGRITY.**"""
                            },
                            {"role": "user", "content": prompt}
                        ],
                        max_tokens=300,  # Increased to accommodate more detailed recommendations
                        temperature=0.7
                    )
                except Exception as e:
                    logger.error(f"Error generating suggestions: {str(e)}")
                    continue
                
                # Parse response into structured format
                suggestion = response.choices[0].message.content.strip()
                parts = suggestion.split('\n')
                
                def extract_section(marker: str) -> str:
                    print(f"\nLooking for marker: {marker}")
                    # Find the start of the section
                    start_idx = -1
                    for i, part in enumerate(parts):
                        part = part.strip()
                        print(f"Checking line {i}: {part}")
                        if f'**{marker}:**' in part or f'{marker}:' in part:
                            start_idx = i
                            print(f"Found marker at line {i}")
                            break
                    if start_idx == -1:
                        print(f"Marker {marker} not found")
                        return 'UNKNOWN'
                    
                    # Extract content until next section
                    content = []
                    i = start_idx
                    current_line = parts[i].strip()
                    
                    # Extract content from first line
                    if f'**{marker}:**' in current_line:
                        content.append(current_line.split(f'**{marker}:**')[1].strip())
                    elif f'{marker}:' in current_line:
                        content.append(current_line.split(f'{marker}:')[1].strip())
                    
                    # Continue until we hit another section or code block
                    i += 1
                    while i < len(parts):
                        line = parts[i].strip()
                        if '**' in line or line.startswith('```') or ':' in line:
                            # Check if this is actually a new section
                            if any(f'**{m}:**' in line or f'{m}:' in line 
                                  for m in ['Type', 'Description', 'Impact', 'SQL']):
                                break
                        if line:
                            content.append(line)
                        i += 1
                    
                    result = ' '.join(content)
                    print(f"Extracted content for {marker}: {result}")
                    return result
                
                def extract_sql() -> Optional[str]:
                    sql_parts = []
                    in_sql = False
                    for part in parts:
                        if '```sql' in part:
                            in_sql = True
                            continue
                        elif '```' in part and in_sql:
                            break
                        elif in_sql:
                            sql_parts.append(part)
                    return '\n'.join(sql_parts) if sql_parts else None
                
                rec_type = extract_section('Type')
                description = extract_section('Description')
                impact = extract_section('Impact')
                sql = extract_sql()
                
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
