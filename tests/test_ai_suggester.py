import pytest
from datetime import datetime
from utils.ai_suggester import AISuggester
from utils.config import Config

@pytest.fixture
def ai_suggester():
    """Create an AI suggester instance using the config API key"""
    return AISuggester(Config.OPENAI_API_KEY)

def test_token_estimation(ai_suggester):
    """Test the token estimation function"""
    text = "SELECT * FROM users WHERE id = 1"
    tokens = ai_suggester.estimate_tokens(text)
    assert tokens > 0
    assert isinstance(tokens, int)

def test_identify_complex_models(ai_suggester):
    """Test identification of complex models based on SQL complexity"""
    models = [
        {
            'name': 'simple_model',
            'raw_sql': 'SELECT * FROM users'
        },
        {
            'name': 'complex_model',
            'raw_sql': '''
                WITH user_stats AS (
                    SELECT user_id, COUNT(*) as event_count
                    FROM events
                    GROUP BY user_id
                ),
                user_purchases AS (
                    SELECT user_id, SUM(amount) as total_spent
                    FROM purchases
                    GROUP BY user_id
                )
                SELECT 
                    u.*,
                    us.event_count,
                    up.total_spent,
                    ROW_NUMBER() OVER (PARTITION BY u.country ORDER BY up.total_spent DESC) as country_rank
                FROM users u
                LEFT JOIN user_stats us ON u.id = us.user_id
                LEFT JOIN user_purchases up ON u.id = up.user_id
            '''
        }
    ]
    
    complex_models = ai_suggester._identify_complex_models(models)
    assert len(complex_models) == 1
    assert 'complex_model' in complex_models
    assert 'simple_model' not in complex_models

def test_calculate_dependency_depth(ai_suggester):
    """Test calculation of maximum dependency depth"""
    dependencies = {
        'model_a': ['model_b', 'model_c'],
        'model_b': ['model_d'],
        'model_c': ['model_d'],
        'model_d': []
    }
    
    depth = ai_suggester._calculate_dependency_depth(dependencies)
    assert depth == 2  # model_a -> model_b -> model_d

def test_analyze_model_details(ai_suggester):
    """Test analysis of model details"""
    models = [
        {
            'name': 'user_stats',
            'materialization': 'table',
            'raw_sql': '''
                SELECT user_id, COUNT(*) as event_count
                FROM events
                GROUP BY user_id
            ''',
            'references': ['events'],
            'sources': ['raw.events'],
            'config': {'materialized': 'table'}
        }
    ]
    
    details = ai_suggester._analyze_model_details(models)
    assert len(details) == 1
    model_detail = details[0]
    assert model_detail['name'] == 'user_stats'
    assert model_detail['materialization'] == 'table'
    assert model_detail['reference_count'] == 1
    assert model_detail['source_count'] == 1
    assert 'sql_complexity' in model_detail

def test_analyze_dependency_patterns(ai_suggester):
    """Test analysis of dependency patterns"""
    dependencies = {
        'final_model': ['intermediate_1', 'intermediate_2'],
        'intermediate_1': ['base_1'],
        'intermediate_2': ['base_1'],
        'base_1': []
    }
    
    models = [
        {
            'name': model,
            'materialization': 'view',
            'raw_sql': 'SELECT 1',
            'sources': []
        }
        for model in ['final_model', 'intermediate_1', 'intermediate_2', 'base_1']
    ]
    
    analysis = ai_suggester._analyze_dependency_patterns(dependencies, models)
    assert 'bottleneck_models' in analysis
    assert 'reusability_opportunities' in analysis
    assert 'materialization_suggestions' in analysis

def test_generate_suggestions(ai_suggester):
    """Test generation of optimization suggestions"""
    query_patterns = [
        {
            'pattern': 'SELECT * FROM users WHERE country = ?',
            'frequency': 100,
            'avg_duration_ms': 500,
            'example_query': 'SELECT * FROM users WHERE country = "US"'
        }
    ]
    
    dbt_structure = {
        'models': [
            {
                'name': 'user_stats',
                'materialization': 'view',
                'raw_sql': 'SELECT * FROM users',
                'references': [],
                'sources': ['raw.users'],
                'config': {}
            }
        ],
        'sources': ['raw.users'],
        'model_dependencies': {},
        'project_config': {}
    }
    
    suggestions = ai_suggester.generate_suggestions(
        query_patterns=query_patterns,
        dbt_structure=dbt_structure,
        max_patterns=1,
        max_tokens=4000
    )
    
    assert isinstance(suggestions, list)
    if suggestions:  # OpenAI might be rate limited or have other issues
        suggestion = suggestions[0]
        assert 'title' in suggestion
        assert 'category' in suggestion
        assert 'impact_level' in suggestion
