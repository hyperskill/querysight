import os
import yaml
import glob
from typing import Dict, List, Any
import re

class DBTProjectAnalyzer:
    def __init__(self, project_path: str):
        self.project_path = project_path
        self.models_path = os.path.join(project_path, 'models')
        
    def analyze_project(self) -> Dict[str, Any]:
        """
        Analyze the dbt project structure and return relevant information
        """
        try:
            project_config = self._read_project_config()
            models = self._analyze_models()
            sources = self._analyze_sources()
            
            return {
                'project_config': project_config,
                'models': models,
                'sources': sources,
                'model_dependencies': self._analyze_dependencies(models)
            }
        except Exception as e:
            raise Exception(f"Failed to analyze dbt project: {str(e)}")

    def _read_project_config(self) -> Dict[str, Any]:
        """
        Read and parse dbt_project.yml
        """
        project_file = os.path.join(self.project_path, 'dbt_project.yml')
        if not os.path.exists(project_file):
            raise Exception("dbt_project.yml not found")
            
        with open(project_file, 'r') as f:
            return yaml.safe_load(f)

    def _analyze_models(self) -> List[Dict[str, Any]]:
        """
        Analyze all SQL models in the project
        """
        models = []
        for sql_file in glob.glob(os.path.join(self.models_path, '**/*.sql'), recursive=True):
            with open(sql_file, 'r') as f:
                content = f.read()
                
            model_info = {
                'name': os.path.basename(sql_file).replace('.sql', ''),
                'path': os.path.relpath(sql_file, self.models_path),
                'raw_sql': content,
                'materialization': self._extract_materialization(content),
                'references': self._extract_references(content),
                'sources': self._extract_sources(content)
            }
            
            # Try to find corresponding YAML config
            yaml_path = sql_file.replace('.sql', '.yml')
            if os.path.exists(yaml_path):
                with open(yaml_path, 'r') as f:
                    model_info['config'] = yaml.safe_load(f)
                    
            models.append(model_info)
            
        return models

    def _analyze_sources(self) -> List[Dict[str, Any]]:
        """
        Find and analyze source definitions
        """
        sources = []
        for yaml_file in glob.glob(os.path.join(self.models_path, '**/*.yml'), recursive=True):
            with open(yaml_file, 'r') as f:
                content = yaml.safe_load(f)
                
            if not content or 'sources' not in content:
                continue
                
            sources.extend(content['sources'])
            
        return sources

    def _extract_materialization(self, sql_content: str) -> str:
        """
        Extract materialization type from SQL content
        """
        materialization_match = re.search(
            r'{{\s*config\s*\(\s*materialized\s*=\s*[\'"](\w+)[\'"]\s*\)\s*}}',
            sql_content
        )
        return materialization_match.group(1) if materialization_match else 'view'

    def _extract_references(self, sql_content: str) -> List[str]:
        """
        Extract model references from SQL content
        """
        return re.findall(r'{{\s*ref\s*\(\s*[\'"]([^\'"]+)[\'"]\s*\)\s*}}', sql_content)

    def _extract_sources(self, sql_content: str) -> List[Dict[str, str]]:
        """
        Extract source references from SQL content
        """
        source_matches = re.findall(
            r'{{\s*source\s*\(\s*[\'"]([^\'"]+)[\'"]\s*,\s*[\'"]([^\'"]+)[\'"]\s*\)\s*}}',
            sql_content
        )
        return [{'source': src, 'table': tbl} for src, tbl in source_matches]

    def _analyze_dependencies(self, models: List[Dict[str, Any]]) -> Dict[str, List[str]]:
        """
        Create a dependency map between models
        """
        dependencies = {}
        for model in models:
            model_name = model['name']
            dependencies[model_name] = model['references']
            
        return dependencies
