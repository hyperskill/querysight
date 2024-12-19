import os
import pytest
import yaml
from utils.dbt_analyzer import DBTProjectAnalyzer

@pytest.fixture
def temp_dbt_project(tmp_path):
    """Create a temporary dbt project structure for testing."""
    project_dir = tmp_path / "dbt_project"
    models_dir = project_dir / "models"
    models_dir.mkdir(parents=True)
    
    # Create dbt_project.yml
    project_config = {
        'name': 'test_project',
        'version': '1.0.0',
        'config-version': 2,
        'profile': 'test_profile'
    }
    with open(project_dir / "dbt_project.yml", 'w') as f:
        yaml.dump(project_config, f)
    
    # Create a model with materialization, refs and sources
    model_sql = """
    {{{{ config(materialized='table') }}}}

    WITH source_data AS (
        SELECT * FROM {{{{ source('raw_data', 'customers') }}}}
    ),
    transformed AS (
        SELECT * FROM {{{{ ref('intermediate_model') }}}}
    )
    SELECT * FROM transformed
    """
    with open(models_dir / "test_model.sql", 'w') as f:
        f.write(model_sql)
    
    # Create model YAML config
    model_config = {
        'version': 2,
        'models': [{
            'name': 'test_model',
            'description': 'Test model description',
            'columns': [{
                'name': 'id',
                'description': 'Primary key'
            }]
        }]
    }
    with open(models_dir / "test_model.yml", 'w') as f:
        yaml.dump(model_config, f)
    
    # Create source definition
    source_config = {
        'version': 2,
        'sources': [{
            'name': 'raw_data',
            'tables': [{
                'name': 'customers',
                'description': 'Raw customer data'
            }]
        }]
    }
    with open(models_dir / "sources.yml", 'w') as f:
        yaml.dump(source_config, f)
    
    return str(project_dir)

def test_project_config_reading(temp_dbt_project):
    """Test that project configuration is read correctly."""
    analyzer = DBTProjectAnalyzer(temp_dbt_project)
    project_info = analyzer.analyze_project()
    
    assert project_info['project_config']['name'] == 'test_project'
    assert project_info['project_config']['version'] == '1.0.0'
    assert project_info['project_config']['profile'] == 'test_profile'

def test_model_analysis(temp_dbt_project):
    """Test that SQL models are analyzed correctly."""
    analyzer = DBTProjectAnalyzer(temp_dbt_project)
    project_info = analyzer.analyze_project()
    
    assert len(project_info['models']) == 1
    model = project_info['models'][0]
    
    assert model['name'] == 'test_model'
    assert model['materialization'] == 'table'
    assert 'intermediate_model' in model['references']
    assert len(model['sources']) == 1
    assert model['sources'][0] == {'source': 'raw_data', 'table': 'customers'}

def test_source_analysis(temp_dbt_project):
    """Test that sources are analyzed correctly."""
    analyzer = DBTProjectAnalyzer(temp_dbt_project)
    project_info = analyzer.analyze_project()
    
    assert len(project_info['sources']) == 1
    source = project_info['sources'][0]
    
    assert source['name'] == 'raw_data'
    assert len(source['tables']) == 1
    assert source['tables'][0]['name'] == 'customers'

def test_model_dependencies(temp_dbt_project):
    """Test that model dependencies are correctly mapped."""
    analyzer = DBTProjectAnalyzer(temp_dbt_project)
    project_info = analyzer.analyze_project()
    
    dependencies = project_info['model_dependencies']
    assert 'test_model' in dependencies
    assert 'intermediate_model' in dependencies['test_model']

def test_missing_project_file(tmp_path):
    """Test handling of missing dbt_project.yml."""
    analyzer = DBTProjectAnalyzer(str(tmp_path))
    with pytest.raises(Exception) as exc_info:
        analyzer.analyze_project()
    assert "dbt_project.yml not found" in str(exc_info.value)

def test_materialization_extraction():
    """Test extraction of materialization config from SQL content."""
    analyzer = DBTProjectAnalyzer("")  # path not needed for this test
    
    # Test explicit materialization
    sql = "{{ config(materialized='table') }} SELECT 1"
    assert analyzer._extract_materialization(sql) == 'table'
    
    # Test default materialization
    sql = "SELECT 1"
    assert analyzer._extract_materialization(sql) == 'view'

def test_reference_extraction():
    """Test extraction of model references from SQL content."""
    analyzer = DBTProjectAnalyzer("")  # path not needed for this test
    
    sql = """
    WITH data AS (
        SELECT * FROM {{ ref('model1') }}
    )
    SELECT * FROM {{ ref('model2') }}
    """
    refs = analyzer._extract_references(sql)
    assert set(refs) == {'model1', 'model2'}

def test_source_extraction():
    """Test extraction of source references from SQL content."""
    analyzer = DBTProjectAnalyzer("")  # path not needed for this test
    
    sql = """
    WITH data AS (
        SELECT * FROM {{ source('raw', 'table1') }}
    )
    SELECT * FROM {{ source('raw', 'table2') }}
    """
    sources = analyzer._extract_sources(sql)
    assert len(sources) == 2
    assert {'source': 'raw', 'table': 'table1'} in sources
    assert {'source': 'raw', 'table': 'table2'} in sources
