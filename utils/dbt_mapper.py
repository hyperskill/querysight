"""DBT model mapping utilities for QuerySight."""
import os
import yaml
import glob
from typing import Dict, Set, Optional, List
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DBTModelInfo:
    """Information about a dbt model's configuration."""
    name: str
    schema: str
    database: str
    materialized: str
    physical_name: str  # The actual table name in the database
    
    @property
    def full_name(self) -> str:
        """Get fully qualified name."""
        return f"{self.database}.{self.schema}.{self.physical_name}"
    
    @property
    def schema_name(self) -> str:
        """Get schema.table format."""
        return f"{self.schema}.{self.physical_name}"

class DBTModelMapper:
    """Maps between dbt models and physical table names."""
    
    def __init__(self, project_path: str):
        self.project_path = project_path
        self.models_path = os.path.join(project_path, 'models')
        self.target_path = os.path.join(project_path, 'target')
        self.model_info: Dict[str, DBTModelInfo] = {}
        self.table_to_model: Dict[str, str] = {}  # Maps physical table names to model names
        
    def load_models(self) -> None:
        """Load model information from dbt project."""
        try:
            # First load project config
            project_config = self._load_project_config()
            default_schema = project_config.get('models', {}).get('schema', 'public')
            default_database = project_config.get('models', {}).get('database', 'default')
            
            # Load manifest if available
            manifest = self._load_manifest()
            if manifest:
                self._load_from_manifest(manifest, default_schema, default_database)
            else:
                # Fallback to loading from model files
                self._load_from_files(default_schema, default_database)
                
            logger.info(f"Loaded {len(self.model_info)} dbt models")
            
        except Exception as e:
            logger.error(f"Error loading dbt models: {str(e)}")
    
    def _load_project_config(self) -> dict:
        """Load dbt_project.yml configuration."""
        config_path = os.path.join(self.project_path, 'dbt_project.yml')
        if not os.path.exists(config_path):
            logger.warning("dbt_project.yml not found")
            return {}
            
        try:
            with open(config_path, 'r') as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            logger.error(f"Error loading dbt_project.yml: {str(e)}")
            return {}
    
    def _load_manifest(self) -> Optional[dict]:
        """Load manifest.json if available."""
        manifest_path = os.path.join(self.target_path, 'manifest.json')
        if not os.path.exists(manifest_path):
            logger.warning("manifest.json not found")
            return None
            
        try:
            import json
            with open(manifest_path, 'r') as f:
                return json.load(f)
        except Exception as e:
            logger.error(f"Error loading manifest.json: {str(e)}")
            return None
    
    def _load_from_manifest(self, manifest: dict, default_schema: str, default_database: str) -> None:
        """Load model information from manifest.json."""
        nodes = manifest.get('nodes', {})
        for node_id, node in nodes.items():
            if node.get('resource_type') != 'model':
                continue
                
            config = node.get('config', {})
            schema = config.get('schema', default_schema)
            database = config.get('database', default_database)
            materialized = config.get('materialized', 'view')
            name = node.get('name', '')
            
            if name:
                # dbt typically uses the model name as the physical table name
                physical_name = name
                model_info = DBTModelInfo(
                    name=name,
                    schema=schema,
                    database=database,
                    materialized=materialized,
                    physical_name=physical_name
                )
                self.model_info[name] = model_info
                
                # Add mappings for different name formats
                self.table_to_model[name.lower()] = name
                self.table_to_model[f"{schema}.{physical_name}".lower()] = name
                self.table_to_model[f"{database}.{schema}.{physical_name}".lower()] = name
    
    def _load_from_files(self, default_schema: str, default_database: str) -> None:
        """Load model information from SQL files when manifest is not available."""
        for sql_file in glob.glob(os.path.join(self.models_path, '**/*.sql'), recursive=True):
            try:
                name = os.path.basename(sql_file).replace('.sql', '')
                
                # Try to load config from .yml file
                yml_file = os.path.join(os.path.dirname(sql_file), 'schema.yml')
                config = {}
                if os.path.exists(yml_file):
                    with open(yml_file, 'r') as f:
                        yml_content = yaml.safe_load(f) or {}
                        models = yml_content.get('models', [])
                        for model in models:
                            if model.get('name') == name:
                                config = model.get('config', {})
                                break
                
                # Create model info
                model_info = DBTModelInfo(
                    name=name,
                    schema=config.get('schema', default_schema),
                    database=config.get('database', default_database),
                    materialized=config.get('materialized', 'view'),
                    physical_name=name
                )
                self.model_info[name] = model_info
                
                # Add mappings
                self.table_to_model[name.lower()] = name
                self.table_to_model[f"{model_info.schema}.{name}".lower()] = name
                self.table_to_model[f"{model_info.database}.{model_info.schema}.{name}".lower()] = name
                
            except Exception as e:
                logger.error(f"Error loading model from {sql_file}: {str(e)}")
    
    def get_model_name(self, table_reference: str) -> Optional[str]:
        """
        Get the dbt model name for a table reference.
        
        Args:
            table_reference: Table reference (can be fully qualified or just table name)
            
        Returns:
            dbt model name if found, None otherwise
        """
        return self.table_to_model.get(table_reference.lower())
    
    def get_model_info(self, model_name: str) -> Optional[DBTModelInfo]:
        """Get information about a dbt model."""
        return self.model_info.get(model_name)
    
    def get_all_models(self) -> List[str]:
        """Get list of all dbt model names."""
        return list(self.model_info.keys())
    
    def get_physical_tables(self) -> Set[str]:
        """Get set of all physical table names."""
        return {model.physical_name for model in self.model_info.values()}
