import pytest
from utils.dbt_analyzer import DBTProjectAnalyzer

# Real dbt project path
REAL_DBT_PATH = "/home/mynameisnotyourbusiness/Documents/repos/data/hyperskill_dbt"

@pytest.fixture
def real_dbt_analyzer():
    """Fixture that provides an analyzer instance for the real dbt project."""
    return DBTProjectAnalyzer(REAL_DBT_PATH)

def test_real_project_config(real_dbt_analyzer):
    """Test reading configuration from the real dbt project."""
    project_info = real_dbt_analyzer.analyze_project()
    
    # Verify project configuration
    config = project_info['project_config']
    assert config['name'] == 'hyperskill_dbt'  # Assuming this is the project name
    assert 'version' in config
    assert 'config-version' in config

def test_real_models_structure(real_dbt_analyzer):
    """Test analyzing models from the real dbt project."""
    project_info = real_dbt_analyzer.analyze_project()
    models = project_info['models']
    
    # Verify we have models
    assert len(models) > 0
    
    # Check model structure
    for model in models:
        assert 'name' in model
        assert 'path' in model
        assert 'raw_sql' in model
        assert 'materialization' in model
        assert 'references' in model
        assert 'sources' in model

def test_real_sources(real_dbt_analyzer):
    """Test analyzing sources from the real dbt project."""
    project_info = real_dbt_analyzer.analyze_project()
    sources = project_info['sources']
    
    # Verify sources structure if any exist
    if sources:
        for source in sources:
            assert 'name' in source
            assert 'tables' in source
            for table in source['tables']:
                assert 'name' in table

def test_real_model_dependencies(real_dbt_analyzer):
    """Test analyzing model dependencies from the real dbt project."""
    project_info = real_dbt_analyzer.analyze_project()
    dependencies = project_info['model_dependencies']
    
    # Verify dependencies structure
    assert isinstance(dependencies, dict)
    
    # Check that dependencies are properly mapped
    for model, refs in dependencies.items():
        assert isinstance(model, str)
        assert isinstance(refs, list)
        # All referenced models should exist in the project
        for ref in refs:
            assert any(m['name'] == ref for m in project_info['models'])

def test_real_materializations(real_dbt_analyzer):
    """Test extraction of materializations from real models."""
    project_info = real_dbt_analyzer.analyze_project()
    models = project_info['models']
    
    # Count different materialization types
    materialization_types = set(model['materialization'] for model in models)
    
    # We should have at least one type of materialization
    assert len(materialization_types) > 0
    # Common materializations should be present
    common_types = {'table', 'view', 'incremental'}
    assert any(mtype in common_types for mtype in materialization_types)

def test_real_model_configs(real_dbt_analyzer):
    """Test extraction of model configurations from real models."""
    project_info = real_dbt_analyzer.analyze_project()
    models = project_info['models']
    
    # Check models with YAML configs
    models_with_config = [model for model in models if 'config' in model]
    if models_with_config:
        for model in models_with_config:
            config = model['config']
            # Verify common config structure
            if isinstance(config, dict):
                if 'models' in config:
                    for model_config in config['models']:
                        assert 'name' in model_config
                        # Common config fields
                        for field in ['description', 'columns']:
                            if field in model_config:
                                assert isinstance(model_config[field], (str, list, dict))

def test_real_source_references(real_dbt_analyzer):
    """Test extraction of source references from real models."""
    project_info = real_dbt_analyzer.analyze_project()
    models = project_info['models']
    
    # Find models that reference sources
    models_with_sources = [model for model in models if model['sources']]
    
    if models_with_sources:
        for model in models_with_sources:
            for source_ref in model['sources']:
                assert 'source' in source_ref
                assert 'table' in source_ref
                # Verify these sources exist in project sources
                source_exists = any(
                    s['name'] == source_ref['source'] 
                    and any(t['name'] == source_ref['table'] for t in s['tables'])
                    for s in project_info['sources']
                )
                assert source_exists, f"Source {source_ref['source']}.{source_ref['table']} not found in project sources"
