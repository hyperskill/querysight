"""
Module for generating dbt models based on query patterns and table analysis.
Provides intelligent model generation with proper naming, testing, and documentation.
"""

from typing import Dict, List, Optional, Set
from pathlib import Path
import logging
import json
from datetime import datetime

from .models import QueryPattern, DBTModel, AIRecommendation
from .dbt_analyzer import DBTProjectAnalyzer
from .ai_suggester import AISuggester
from .sql_parser import extract_tables_from_query

logger = logging.getLogger(__name__)

class DBTModelGenerator:
    """Handles intelligent generation of dbt models for uncovered entities."""
    
    def __init__(self, dbt_project_path: str, ai_suggester: Optional[AISuggester] = None):
        """Initialize the model generator with project path and optional AI suggester."""
        self.dbt_project_path = Path(dbt_project_path)
        self.ai_suggester = ai_suggester or AISuggester()
        self.dbt_analyzer = DBTProjectAnalyzer(dbt_project_path)
        
    def _generate_model_name(self, table_name: str, model_type: str = "staging") -> str:
        """Generate appropriate dbt model name based on conventions."""
        # Remove schema prefix if present
        if "." in table_name:
            schema, table = table_name.split(".", 1)
        else:
            table = table_name
            
        # Apply naming conventions
        prefixes = {
            "staging": "stg",
            "intermediate": "int",
            "mart_fact": "",
            "mart_dimension": "",
            "metrics": "mtr"       }
        prefix = prefixes.get(model_type, "stg")
        
        return f"{prefix}_{table.lower()}"
        
    def _generate_model_path(self, model_name: str, model_type: str) -> Path:
        """Generate appropriate path for the new model file."""
        base_dirs = {
            "staging": self.dbt_project_path / "models" / "staging",
            "intermediate": self.dbt_project_path / "models" / "intermediate",
            "mart_fact": self.dbt_project_path / "models" / "mart",
            "mart_dimension": self.dbt_project_path / "models" / "mart"
        }
        
        base_dir = base_dirs.get(model_type, self.dbt_project_path / "models" / "staging")
        return base_dir / f"{model_name}.sql"
        
    def _generate_model_tests(self, table_name: str, pattern: QueryPattern) -> Dict[str, List[str]]:
        """Generate appropriate dbt tests based on query patterns."""
        tests = {
            "unique": [],
            "not_null": [],
            "relationships": [],
            "accepted_values": []
        }
        
        # Analyze query pattern for potential test cases
        sql_lower = pattern.sql_pattern.lower()
        
        # Look for primary key indicators
        if "where" in sql_lower and any(op in sql_lower for op in ["= ", "in "]):
            potential_keys = [word.strip() for word in sql_lower.split() if word.endswith("_id")]
            tests["unique"].extend(potential_keys)
            tests["not_null"].extend(potential_keys)
            
        # Look for join conditions for relationships
        if "join" in sql_lower and "_id" in sql_lower:
            join_conditions = [line for line in sql_lower.split("\n") if "join" in line.lower()]
            for condition in join_conditions:
                if "_id" in condition:
                    tests["relationships"].append(condition)
                    
        return tests
        
    def _generate_model_config(self, pattern: QueryPattern) -> Dict[str, any]:
        """Generate appropriate model configuration based on query patterns."""
        config = {
            "materialized": "view",  # Default materialization
            "tags": [],
            "meta": {
                "owner": "querysight",
                "created_at": datetime.now().isoformat(),
                "generated_by": "querysight_model_generator"
            }
        }
        
        # Determine materialization based on query patterns
        if pattern.frequency > 100 or pattern.avg_duration_ms > 1000:
            config["materialized"] = "table"
            
        if pattern.frequency > 1000:
            config["materialized"] = "incremental"
            config["unique_key"] = "id"  # This should be determined more intelligently
            
        return config
        
    def generate_model(self, table_name: str, pattern: QueryPattern) -> Dict[str, any]:
        """Generate a complete dbt model for an uncovered table."""
        try:
            # 1. Determine model type and name
            model_type = "staging"  # Default to staging, can be made smarter
            model_name = self._generate_model_name(table_name, model_type)
            
            # 2. Generate model configuration
            config = self._generate_model_config(pattern)
            
            # 3. Generate tests
            tests = self._generate_model_tests(table_name, pattern)
            
            # 4. Generate model path
            model_path = self._generate_model_path(model_name, model_type)
            
            # 5. Get AI recommendations for the model
            if self.ai_suggester:
                context = {
                    "table_name": table_name,
                    "query_pattern": pattern.sql_pattern,
                    "frequency": pattern.frequency,
                    "avg_duration": pattern.avg_duration_ms,
                    "tables_accessed": list(pattern.tables_accessed)
                }
                recommendations = self.ai_suggester.generate_recommendations(
                    [pattern],
                    self.dbt_analyzer.get_models()
                )
            
            # 6. Create model structure
            model_structure = {
                "name": model_name,
                "path": str(model_path),
                "config": config,
                "tests": tests,
                "sql": pattern.sql_pattern,  # This should be cleaned up and formatted
                "recommendations": recommendations if self.ai_suggester else None
            }
            
            return model_structure
            
        except Exception as e:
            logger.error(f"Error generating model for table {table_name}: {str(e)}")
            raise
            
    def create_model_files(self, model_structure: Dict[str, any]) -> None:
        """Create the actual model files in the dbt project."""
        try:
            model_path = Path(model_structure["path"])
            
            # Ensure directory exists
            model_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Generate model SQL content
            sql_content = f"""{{{{
    config(
        materialized='{model_structure["config"]["materialized"]}',
        tags={json.dumps(model_structure["config"]["tags"])},
        meta={json.dumps(model_structure["config"]["meta"])}
    )
}}}}

-- Model: {model_structure["name"]}
-- Generated by: QuerySight Model Generator
-- Created at: {model_structure["config"]["meta"]["created_at"]}

{model_structure["sql"]}"""

            # Write model SQL file
            with open(model_path, 'w') as f:
                f.write(sql_content)
                
            # Generate schema.yml content
            schema_content = f"""version: 2

models:
  - name: {model_structure["name"]}
    description: >
      Generated by QuerySight Model Generator.
      This model represents {model_structure["name"].split("_", 1)[1]}.
    columns:
      # TODO: Add column definitions
    tests:
      - unique:
          columns: {json.dumps(model_structure["tests"]["unique"])}
      - not_null:
          columns: {json.dumps(model_structure["tests"]["not_null"])}
"""
            
            # Write schema.yml file
            schema_path = model_path.parent / "schema.yml"
            if not schema_path.exists():
                with open(schema_path, 'w') as f:
                    f.write(schema_content)
            else:
                # TODO: Implement smart schema.yml merging
                logger.info(f"Schema file already exists at {schema_path}")
                
            logger.info(f"Successfully created model files for {model_structure['name']}")
            
        except Exception as e:
            logger.error(f"Error creating model files: {str(e)}")
            raise
