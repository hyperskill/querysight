"""DBT model mapping utilities for QuerySight."""
import os
import yaml
import glob
import re
from typing import Dict, Set, Optional, List
import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

@dataclass
class DBTModelInfo:
    """Information about a dbt model's configuration."""
    name: str
    schema: str
    database: str  # Not used in ClickHouse, kept for compatibility
    materialized: str
    physical_name: str  # The actual table name in the database
    path: str = ''  # Path to the model file, relative to models directory
    
    def full_name(self) -> str:
        """Get fully qualified name."""
        return f"{self.schema}.{self.physical_name}"
    
    def schema_name(self) -> str:
        """Get schema.table format."""
        return self.full_name()  # In ClickHouse, this is the same as full_name

class DBTModelMapper:
    """Maps between dbt models and physical table names."""
    
    def __init__(self, project_path: str):
        self.project_path = project_path
        self.models_path = os.path.join(project_path, 'models')
        self.target_path = os.path.join(project_path, 'target')
        self.model_info: Dict[str, DBTModelInfo] = {}
        self.table_to_model: Dict[str, str] = {}  # Maps physical table names to model names
        self.source_refs: Dict[str, str] = {}  # Maps source references to physical tables
        
    def load_models(self) -> None:
        """Load model information from dbt project."""
        try:
            # First load project config
            project_config = self._load_project_config()
            default_schema = project_config.get('models', {}).get('schema', 'public')
            default_database = project_config.get('models', {}).get('database', 'default')
            
            # Load sources
            self._load_sources(project_config)
            
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
    
    def _load_sources(self, project_config: dict) -> None:
        """Load source definitions from schema files."""
        try:
            for yml_file in glob.glob(os.path.join(self.models_path, '**/*.yml'), recursive=True):
                if os.path.basename(yml_file) in ('schema.yml', 'models.yml', 'sources.yml'):
                    with open(yml_file, 'r') as f:
                        yml_content = yaml.safe_load(f) or {}
                        if 'sources' in yml_content:
                            for source in yml_content['sources']:
                                source_name = source.get('name', '')
                                schema = source.get('schema', '')
                                database = source.get('database', '')
                                
                                for table in source.get('tables', []):
                                    table_name = table.get('name', '')
                                    if source_name and table_name:
                                        source_ref = f"{source_name}.{table_name}"
                                        physical_table = table.get('identifier', table_name)
                                        if schema:
                                            physical_table = f"{schema}.{physical_table}"
                                        if database:
                                            physical_table = f"{database}.{physical_table}"
                                        self.source_refs[source_ref] = physical_table
        except Exception as e:
            logger.error(f"Error loading sources: {str(e)}")
    
    def _get_schema_for_path(self, dir_path: str, project_name: str) -> str:
        """Get schema name based on directory path and project config.
        
        Args:
            dir_path: Relative path from models directory
            project_name: Project name from dbt_project.yml
            
        Returns:
            Schema name with appropriate suffix based on config
        """
        # Start with base schema (project name)
        schema = project_name.replace('_dbt', '')  # Remove _dbt suffix if present
        
        # Split path into parts for hierarchical lookup
        path_parts = [p for p in dir_path.split(os.sep) if p]
        
        # Check for schema suffixes based on directory structure
        if 'private' in path_parts:
            schema = f"{schema}_private"
        elif 'reports' in path_parts:
            schema = f"{schema}_reports"
            
        logger.info(f"Resolved schema for path {dir_path}: {schema}")
        return schema

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
                # Get relative path from original_file_path
                path = node.get('original_file_path', '')
                if path.startswith('models/'):
                    path = path[7:]  # Remove 'models/' prefix
                
                # Create model info
                model_info = DBTModelInfo(
                    name=name,
                    schema=schema,
                    database=database,
                    materialized=materialized,
                    physical_name=name,
                    path=path
                )
                self.model_info[name] = model_info
                
                # Only store the canonical form (schema.name)
                self.table_to_model[f"{schema}.{name}".lower()] = name
    
    def _load_from_files(self, default_schema: str, default_database: str) -> None:
        """Load model information from SQL files when manifest is not available."""
        # First load project-level configs
        project_config = self._load_project_config()
        project_name = project_config.get('name', 'hyperskill_dbt')
        model_configs = project_config.get('models', {})
        
        # Get default configs from project
        project_materialized = model_configs.get('materialized', 'view')
        
        # Load all schema.yml files first to get model-specific configs
        schema_configs = {}
        for yml_file in glob.glob(os.path.join(self.models_path, '**/*.yml'), recursive=True):
            try:
                if os.path.basename(yml_file) in ('schema.yml', 'models.yml'):
                    with open(yml_file, 'r') as f:
                        yml_content = yaml.safe_load(f) or {}
                        if 'models' in yml_content:
                            # Get directory-specific schema
                            dir_path = os.path.dirname(yml_file)
                            rel_dir = os.path.relpath(dir_path, self.models_path)
                            schema = self._get_schema_for_path(rel_dir, project_name)
                            
                            # Process each model
                            for model in yml_content['models']:
                                name = model.get('name')
                                if name:
                                    # Merge configs
                                    config = model.get('config', {})
                                    config['schema'] = config.get('schema', schema)
                                    config['materialized'] = config.get('materialized', project_materialized)
                                    schema_configs[name] = config
            except Exception as e:
                logger.error(f"Error loading schema file {yml_file}: {str(e)}")
        
        # Now process SQL files
        for sql_file in glob.glob(os.path.join(self.models_path, '**/*.sql'), recursive=True):
            try:
                name = os.path.basename(sql_file).replace('.sql', '')
                rel_path = os.path.relpath(sql_file, self.models_path)
                dir_path = os.path.dirname(rel_path)
                
                # Get schema based on directory path
                schema = self._get_schema_for_path(dir_path, project_name)
                
                # Start with default config
                config = {
                    'schema': schema,
                    'materialized': project_materialized
                }
                
                # Apply schema.yml configs if any
                if name in schema_configs:
                    config.update(schema_configs[name])
                
                # Extract config block from SQL
                with open(sql_file, 'r') as f:
                    content = f.read()
                    config_match = re.search(r'\{\{\s*config\([^)]*\)\s*\}\}', content)
                    if config_match:
                        config_str = config_match.group(0)
                        # Extract key-value pairs
                        for key in ('materialized', 'schema'):
                            match = re.search(rf"{key}\s*=\s*'([^']*)'", config_str)
                            if match:
                                config[key] = match.group(1)
            
                # Create model info
                model_info = DBTModelInfo(
                    name=name,
                    schema=config['schema'],
                    database='',  # Not used in ClickHouse
                    materialized=config['materialized'],
                    physical_name=name,
                    path=rel_path
                )
                self.model_info[name] = model_info
                
                # Add mappings for all possible references
                self.table_to_model[name.lower()] = name
                self.table_to_model[f"{model_info.schema}.{name}".lower()] = name
                
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
        # Try exact match first
        model_name = self.table_to_model.get(table_reference.lower())
        if model_name:
            return model_name
        
        # Split into parts
        parts = [p.strip() for p in table_reference.split('.')]
        if not parts:
            return None
        
        # Try just the table name
        table_name = parts[-1]
        model_name = self.table_to_model.get(table_name.lower())
        if model_name:
            return model_name
        
        # Try schema.table if we have enough parts
        if len(parts) >= 2:
            schema_table = f"{parts[-2]}.{table_name}".lower()
            model_name = self.table_to_model.get(schema_table)
            if model_name:
                return model_name
            
        # Log the failed mapping attempt
        logger.debug(f"No mapping found for table reference: {table_reference}")
        logger.debug(f"Available mappings: {list(self.table_to_model.keys())}")
        
        return None
    
    def get_model_info(self, model_name: str) -> Optional[DBTModelInfo]:
        """Get information about a dbt model."""
        return self.model_info.get(model_name)
    
    def get_all_models(self) -> List[str]:
        """Get list of all dbt model names."""
        return list(self.model_info.keys())
    
    def get_physical_tables(self) -> Set[str]:
        """Get set of all physical table names."""
        return {model.physical_name for model in self.model_info.values()}
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for serialization"""
        return {
            'project_path': self.project_path,
            'model_info': {
                name: {
                    'name': info.name,
                    'schema': info.schema,
                    'database': info.database,
                    'materialized': info.materialized,
                    'physical_name': info.physical_name,
                    'path': info.path
                }
                for name, info in self.model_info.items()
            },
            'table_to_model': self.table_to_model,
            'source_refs': self.source_refs
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'DBTModelMapper':
        """Create from dictionary"""
        mapper = cls(data['project_path'])
        mapper.model_info = {
            name: DBTModelInfo(
                name=info['name'],
                schema=info['schema'],
                database=info['database'],
                materialized=info['materialized'],
                physical_name=info['physical_name'],
                path=info['path']
            )
            for name, info in data['model_info'].items()
        }
        mapper.table_to_model = data['table_to_model']
        mapper.source_refs = data['source_refs']
        return mapper
