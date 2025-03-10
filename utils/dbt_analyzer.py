import os
import yaml
import glob
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
import json
from .models import DBTModel, AnalysisResult
from .dbt_mapper import DBTModelMapper, DBTModelInfo
import logging
logger = logging.getLogger(__name__)

class DBTProjectAnalyzer:
    """Analyzes dbt project structure and maps tables to models."""
    
    def __init__(self, project_path: str):
        self.project_path = project_path
        logger.info(f"Initializing DBTProjectAnalyzer with path: {project_path}")
        self.models_path = os.path.join(project_path, 'models')
        self.target_path = os.path.join(project_path, 'target')
        self.mapper = DBTModelMapper(project_path)
        self.models: Dict[str, DBTModel] = {}
        
    def analyze_project(self) -> AnalysisResult:
        """Analyze the dbt project structure and return relevant information"""
        logger.info("Starting dbt project analysis")
        
        try:
            # Validate project path
            if not self.project_path or not os.path.exists(self.project_path):
                logger.warning(f"Invalid dbt project path: {self.project_path}")
                return self._create_empty_result()
                
            # Load models using the mapper
            self.mapper.load_models()
            
            # Convert mapper's model info to our model format
            for name, info in self.mapper.model_info.items():
                self.models[name] = DBTModel(
                    name=name,
                    path=info.path,
                    materialization=info.materialized
                )
            
            # Analyze dependencies
            self._analyze_dependencies()
            
            # Create analysis result
            result = AnalysisResult(
                dbt_models=self.models,
                dbt_mapper=self.mapper,
                query_patterns=[]  # Will be populated later
            )
            
            logger.info(f"Project analysis complete. Found {len(self.models)} models")
            return result
            
        except Exception as e:
            logger.error(f"Failed to analyze dbt project: {str(e)}", exc_info=True)
            return self._create_empty_result()
    
    def _create_empty_result(self) -> AnalysisResult:
        """Create an empty analysis result with proper structure"""
        return AnalysisResult(
            dbt_models={},
            dbt_mapper=self.mapper,
            query_patterns=[]
        )
    
    def get_model_name(self, table_name: str) -> Optional[str]:
        """Get the dbt model name for a table name. Required by AnalysisResult."""
        return self.mapper.get_model_name(table_name)
    
    def get_model_for_table(self, table_name: str) -> Optional[str]:
        """Get the dbt model name for a physical table"""
        # Clean table name
        table_name = table_name.lower().strip()
        
        # Try exact match first
        if table_name in self.table_to_model:
            return self.table_to_model[table_name]
        
        # Try without schema prefix
        if '.' in table_name:
            base_name = table_name.split('.')[-1]
            if base_name in self.table_to_model:
                return self.table_to_model[base_name]
        
        # Try matching model name directly
        if table_name in self.models:
            return table_name
        
        # Try matching physical name
        for model_name, model in self.models.items():
            if model.physical_name.lower() == table_name:
                return model_name
        
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
        """Analyze dependencies between models with enhanced relationship tracking"""
        for model_name, model in self.models.items():
            model_info = self.mapper.get_model_info(model_name)
            if not model_info:
                continue
                
            # Read the model file
            model_path = os.path.join(self.models_path, model_info.path)
            try:
                with open(model_path, 'r') as f:
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
                    
            except Exception as e:
                logger.error(f"Error analyzing dependencies for {model_name}: {str(e)}")
        
        # After mapping all direct dependencies, calculate extended metrics
        self._calculate_dependency_metrics()
    
    def _calculate_dependency_metrics(self) -> None:
        """Calculate and store additional dependency metrics for each model"""
        # Pre-calculate dependency depth for all models
        for model_name, model in self.models.items():
            # Store dependency depth
            depth = model.dependency_depth(self.models)
            setattr(model, '_dependency_depth', depth)
            
            # Identify critical models (those with many downstream dependents)
            descendants = model.get_all_descendants(self.models)
            impact_score = len(descendants)
            setattr(model, '_impact_score', impact_score)
            
        # Log metrics for debugging
        logger.debug(f"Calculated dependency metrics for {len(self.models)} models")
    
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
