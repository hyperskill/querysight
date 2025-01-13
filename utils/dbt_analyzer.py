import os
import yaml
import glob
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
import json
from .models import DBTModel, AnalysisResult
import logging
logger = logging.getLogger(__name__)

class DBTProjectAnalyzer:
    """Analyzes dbt project structure and maps tables to models."""
    
    def __init__(self, project_path: str):
        self.project_path = project_path
        logger.info(f"Initializing DBTProjectAnalyzer with path: {project_path}")
        self.models_path = os.path.join(project_path, 'models')
        self.target_path = os.path.join(project_path, 'target')
        self.models: Dict[str, DBTModel] = {}
        self.table_to_model: Dict[str, str] = {}  # Maps physical table names to model names
        
    def analyze_project(self) -> AnalysisResult:
        """Analyze the dbt project structure and return relevant information"""
        try:
            if not self.project_path or not os.path.exists(self.project_path):
                return AnalysisResult(
                    timestamp=datetime.now(),
                    query_patterns=[],
                    dbt_models={},
                    uncovered_tables=set(),
                    model_coverage={}
                )
            
            # Load project configuration
            project_config = self._read_project_config()
            default_schema = project_config.get('models', {}).get('schema', 'public')
            default_database = project_config.get('models', {}).get('database', 'default')
            
            # Load manifest if available
            manifest = self._load_manifest()
            if manifest:
                self._load_from_manifest(manifest, default_schema, default_database)
            else:
                # Fallback to file-based analysis
                self._analyze_models()
            
            # Analyze model dependencies
            self._analyze_dependencies()
            
            return AnalysisResult(
                timestamp=datetime.now(),
                query_patterns=[],  # To be filled by data acquisition
                dbt_models=self.models,
                uncovered_tables=set(),
                model_coverage={}
            )
            
        except Exception as e:
            logger.warning(f"Failed to analyze dbt project: {str(e)}")
            return AnalysisResult(
                timestamp=datetime.now(),
                query_patterns=[],
                dbt_models={},
                uncovered_tables=set(),
                model_coverage={}
            )
    
    def get_model_for_table(self, table_name: str) -> Optional[str]:
        """Get the dbt model name for a physical table"""
        # Try exact match first
        if table_name in self.table_to_model:
            return self.table_to_model[table_name]
            
        # Try schema.table format
        if '.' in table_name:
            base_name = table_name.split('.')[-1]
            if base_name in self.table_to_model:
                return self.table_to_model[base_name]
        
        return None
    
    def _load_manifest(self) -> Optional[Dict]:
        """Load dbt manifest.json if available"""
        manifest_path = os.path.join(self.target_path, 'manifest.json')
        if os.path.exists(manifest_path):
            try:
                with open(manifest_path, 'r') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Failed to load manifest.json: {str(e)}")
        return None
    
    def _load_from_manifest(self, manifest: Dict, default_schema: str, default_database: str) -> None:
        """Load model information from dbt manifest"""
        nodes = manifest.get('nodes', {})
        for node_id, node in nodes.items():
            if node.get('resource_type') == 'model':
                model_name = node.get('name')
                if not model_name:
                    continue
                    
                # Get model configuration
                config = node.get('config', {})
                schema = config.get('schema', default_schema)
                database = config.get('database', default_database)
                materialized = config.get('materialized', 'view')
                
                # Create model and map physical table
                model = DBTModel(
                    name=model_name,
                    path=node.get('original_file_path', ''),
                    materialization=materialized
                )
                
                # Add to models dict
                self.models[model_name] = model
                
                # Map physical table names
                physical_name = f"{database}.{schema}.{model_name}"
                self.table_to_model[physical_name] = model_name
                self.table_to_model[f"{schema}.{model_name}"] = model_name
                self.table_to_model[model_name] = model_name
    
    def _read_project_config(self) -> Dict:
        """Read dbt project configuration"""
        try:
            config_path = os.path.join(self.project_path, 'dbt_project.yml')
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    return yaml.safe_load(f)
        except Exception as e:
            logger.warning(f"Failed to read project config: {str(e)}")
        return {}
    
    def _analyze_models(self) -> None:
        """
        Analyze all SQL models in the project
        """
        for sql_file in glob.glob(os.path.join(self.models_path, '**/*.sql'), recursive=True):
            with open(sql_file, 'r') as f:
                content = f.read()
                
            model_name = os.path.basename(sql_file).replace('.sql', '')
            rel_path = os.path.relpath(sql_file, self.models_path)
            
            # Parse model configuration
            config_match = re.search(r'\{\{\s*config\([^)]*\)\s*\}\}', content)
            materialization = "view"  # default
            if config_match:
                config_str = config_match.group(0)
                if 'materialized' in config_str:
                    mat_match = re.search(r"materialized\s*=\s*'(\w+)'", config_str)
                    if mat_match:
                        materialization = mat_match.group(1)
            
            # Create model instance
            self.models[model_name] = DBTModel(
                name=model_name,
                path=rel_path,
                materialization=materialization
            )
            
            # Extract columns from model
            self._extract_columns(model_name, content)
    
    def _analyze_dependencies(self) -> None:
        """
        Analyze dependencies between models
        """
        for model_name, model in self.models.items():
            with open(os.path.join(self.models_path, model.path), 'r') as f:
                content = f.read()
            
            # Find references using ref() macro
            refs = re.finditer(r'{{\s*ref\([\'"]([^\'"]+)[\'"]\)\s*}}', content)
            for ref in refs:
                referenced_model = ref.group(1)
                if referenced_model in self.models:
                    model.add_dependency(referenced_model)
                    self.models[referenced_model].add_reference(model_name)
            
            # Find source references
            sources = re.finditer(r'{{\s*source\([\'"]([^\'"]+)[\'"]\s*,\s*[\'"]([^\'"]+)[\'"]\)\s*}}', content)
            for source in sources:
                source_name = f"{source.group(1)}.{source.group(2)}"
                model.add_dependency(source_name)
    
    def _extract_columns(self, model_name: str, content: str) -> None:
        """
        Extract column definitions from a model
        """
        # Simple column extraction from SELECT statements
        select_pattern = re.compile(r'SELECT\s+(.*?)\s+FROM', re.IGNORECASE | re.DOTALL)
        match = select_pattern.search(content)
        if match:
            columns_str = match.group(1)
            columns = [col.strip() for col in columns_str.split(',')]
            
            for col in columns:
                # Handle aliased columns
                if ' as ' in col.lower():
                    col_name = col.lower().split(' as ')[-1].strip()
                else:
                    col_name = col.split('.')[-1].strip()
                
                self.models[model_name].columns[col_name] = 'unknown'  # Type inference would require more complex analysis
