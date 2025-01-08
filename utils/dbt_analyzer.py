import os
import yaml
import glob
from typing import Dict, List, Any
import re
from datetime import datetime
from .models import DBTModel, AnalysisResult
import logging
logger = logging.getLogger(__name__)

class DBTProjectAnalyzer:
    def __init__(self, project_path: str):
        self.project_path = project_path
        logger.info(f"Initializing DBTProjectAnalyzer with path: {project_path}")
        self.models_path = os.path.join(project_path, 'models')
        self.models: Dict[str, DBTModel] = {}
        
    def analyze_project(self) -> AnalysisResult:
        """
        Analyze the dbt project structure and return relevant information
        """
        try:
            # Check if project path exists and is accessible
            logger.info(f"Checking dbt project path: {self.project_path}")
            logger.info(f"Path exists: {os.path.exists(self.project_path) if self.project_path else False}")
            if not self.project_path or not os.path.exists(self.project_path):
                # Return empty analysis result if no dbt project
                return AnalysisResult(
                    timestamp=datetime.now(),
                    query_patterns=[],
                    dbt_models={},
                    uncovered_tables=set(),
                    model_coverage={}
                )
            
            project_config = self._read_project_config()
            self._analyze_models()
            self._analyze_dependencies()
            
            return AnalysisResult(
                timestamp=datetime.now(),
                query_patterns=[],  # To be filled by data acquisition
                dbt_models=self.models
            )
        except Exception as e:
            logger.warning(f"Failed to analyze dbt project: {str(e)}")
            # Return empty analysis result on error
            return AnalysisResult(
                timestamp=datetime.now(),
                query_patterns=[],
                dbt_models={},
                uncovered_tables=set(),
                model_coverage={}
            )

    def _read_project_config(self) -> Dict[str, Any]:
        """
        Read and parse dbt_project.yml
        """
        project_file = os.path.join(self.project_path, 'dbt_project.yml')
        if not os.path.exists(project_file):
            return {}  # Return empty config if file doesn't exist
            
        try:
            with open(project_file, 'r') as f:
                return yaml.safe_load(f)
        except Exception:
            return {}  # Return empty config on error

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
