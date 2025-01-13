#!/usr/bin/env python3

import click
import sys
import json
from datetime import datetime, timedelta
from rich.console import Console
from rich.table import Table
from rich.progress import Progress
from rich.panel import Panel
from rich.syntax import Syntax
from typing import Optional, Dict, List
import logging
from enum import Enum
import hashlib

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

from utils.config import Config
from utils.models import (
    QueryLog, QueryPattern, DBTModel, AnalysisResult, 
    AIRecommendation, QueryType, QueryFocus
)
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.dbt_analyzer import DBTProjectAnalyzer
from utils.ai_suggester import AISuggester
from utils.cache_manager import QueryLogsCacheManager

console = Console()

class AnalysisLevel(Enum):
    DATA_COLLECTION = "data_collection"
    PATTERN_ANALYSIS = "pattern_analysis"
    DBT_INTEGRATION = "dbt_integration"
    OPTIMIZATION = "optimization"

def validate_config() -> None:
    """Validate configuration before running"""
    is_valid, missing_vars = Config.validate_config()
    logger.info(f"Config validation result: valid={is_valid}, missing={missing_vars}")
    if not is_valid:
        console.print("[red]Error: Missing required configuration variables:[/red]")
        for var in missing_vars:
            console.print(f"  - {var}")
        sys.exit(1)

def validate_connection(data_acquisition: ClickHouseDataAcquisition) -> None:
    """Test database connection"""
    try:
        data_acquisition.test_connection()
    except Exception as e:
        console.print(f"[red]Error connecting to ClickHouse: {str(e)}[/red]")
        sys.exit(1)

def display_query_patterns(patterns: List[QueryPattern]) -> None:
    """Display analyzed query patterns in a table"""
    if not patterns:
        console.print("[yellow]No query patterns found[/yellow]")
        return
        
    table = Table(title="Query Patterns")
    table.add_column("Pattern ID", style="cyan")
    table.add_column("Model", style="green")
    table.add_column("Frequency", justify="right")
    table.add_column("Avg Duration (ms)", justify="right")
    table.add_column("Tables", style="blue")
    table.add_column("DBT Models", style="magenta")
    
    for pattern in patterns:
        table.add_row(
            str(pattern.pattern_id),
            pattern.model_name or "N/A",
            str(pattern.frequency),
            f"{pattern.avg_duration_ms:.2f}",
            ", ".join(pattern.tables_accessed) or "N/A",
            ", ".join(pattern.dbt_models_used) or "N/A"
        )
    
    console.print(table)
    console.print(f"\nTotal Patterns: {len(patterns)}")

def display_model_coverage(result: AnalysisResult):
    """Display dbt model coverage metrics"""
    if not result or not result.model_coverage:
        console.print("[yellow]No model coverage data available[/yellow]")
        return
        
    table = Table(title="DBT Model Coverage")
    table.add_column("Model", style="cyan")
    table.add_column("Coverage %", justify="right", style="green")
    table.add_column("Query Patterns", justify="right")
    table.add_column("Dependencies", style="blue")
    
    for model_name, coverage in result.model_coverage.items():
        if model_name not in result.dbt_models:
            continue
            
        model = result.dbt_models[model_name]
        patterns_using_model = [
            p for p in result.query_patterns
            if model_name in p.dbt_models_used
        ]
        
        table.add_row(
            model_name,
            f"{coverage * 100:.1f}%",
            str(len(patterns_using_model)),
            ", ".join(model.depends_on) or "None"
        )
    
    console.print(table)
    
    # Display uncovered tables
    if result.uncovered_tables:
        console.print("\n[yellow]Uncovered Tables:[/yellow]")
        for table in sorted(result.uncovered_tables):
            console.print(f"  - {table}")

def display_recommendations(recommendations: List[AIRecommendation]):
    """Display AI-generated recommendations"""
    if not recommendations:
        console.print("[yellow]No recommendations available[/yellow]")
        return
        
    for i, rec in enumerate(recommendations, 1):
        panel = Panel(
            f"[bold]{rec.type}[/bold]\n\n"
            f"{rec.description}\n\n"
            f"[blue]Impact: {rec.impact}[/blue]"
            + (f"\n\n[green]Suggested SQL:[/green]\n{rec.suggested_sql}" if rec.suggested_sql else ""),
            title=f"Recommendation {i}",
            border_style="cyan"
        )
        console.print(panel)

@click.group()
def cli():
    """QuerySight CLI - Analyze ClickHouse query patterns and optimize dbt models"""
    pass

@cli.command()
@click.option('--days', default=7, help='Number of days to analyze')
@click.option('--focus', default='all', help='Focus of analysis (slow/frequent/all)')
@click.option('--min-frequency', default=2, help='Minimum query frequency')
@click.option('--sample-size', default=1.0, help='Sample size for query logs')
@click.option('--batch-size', default=1000, help='Batch size for processing')
@click.option('--include-users', help='Users to include (comma-separated)')
@click.option('--exclude-users', help='Users to exclude (comma-separated)')
@click.option('--query-types', help='Query types to analyze (comma-separated)')
@click.option('--cache/--no-cache', default=True, help='Use cache')
@click.option('--level', default='optimization', help='Analysis level (data_collection/pattern_analysis/dbt_integration/optimization)')
@click.option('--dbt-project', help='Path to dbt project')
@click.option('--select-patterns', help='Pattern IDs to analyze (comma-separated)')
@click.option('--select-models', help='DBT model names to analyze (comma-separated)')
def analyze(
    days: int,
    focus: str,
    min_frequency: int,
    sample_size: float,
    batch_size: int,
    include_users: Optional[str],
    exclude_users: Optional[str],
    query_types: Optional[str],
    cache: bool,
    level: str,
    dbt_project: Optional[str],
    select_patterns: Optional[str],
    select_models: Optional[str]
):
    """Analyze query patterns and generate recommendations"""
    try:
        # Initialize components and parameters
        components = initialize_analysis_components(dbt_project)
        params = prepare_analysis_parameters(days, focus, include_users, exclude_users, query_types)
        target_level = level.lower()  # Use string comparison instead of enum
        
        # Create progress tracking
        with Progress() as progress:
            tasks = create_progress_tasks(progress, target_level)
            
            # Data Collection Phase
            query_logs = execute_data_collection(components, params, cache, progress, tasks['data_collection'])
            if target_level == AnalysisLevel.DATA_COLLECTION.value:
                display_analysis_results(None, [], [], target_level)
                return
            
            # Pattern Analysis Phase is required for all levels beyond data_collection
            patterns = execute_pattern_analysis(
                components, 
                query_logs, 
                min_frequency, 
                progress, 
                tasks.get('pattern_analysis', tasks['data_collection'])  # Fallback to data_collection task
            )
            if target_level == AnalysisLevel.PATTERN_ANALYSIS.value:
                display_analysis_results(None, patterns, [], target_level)
                return
                
            # Filter patterns if specified
            if select_patterns:
                pattern_ids = set(select_patterns.split(','))
                patterns = [p for p in patterns if p.pattern_id in pattern_ids]
                logger.info(f"Selected {len(patterns)} patterns for analysis")
            
            # DBT Integration Phase
            if target_level >= AnalysisLevel.DBT_INTEGRATION.value:
                analysis_result = execute_dbt_integration(
                    components, 
                    patterns, 
                    progress, 
                    tasks.get('dbt_integration', tasks['data_collection'])
                )
                if target_level == AnalysisLevel.DBT_INTEGRATION.value:
                    display_analysis_results(analysis_result, patterns, [], target_level)
                    return
                    
            # Filter models if specified
            if select_models:
                model_names = set(select_models.split(','))
                # Filter patterns that use selected models
                patterns = [
                    p for p in analysis_result.query_patterns 
                    if any(model in model_names for model in p.dbt_models_used)
                ]
                # Update analysis result
                analysis_result.query_patterns = patterns
                analysis_result.calculate_coverage()
                logger.info(f"Selected {len(patterns)} patterns using specified models")
            
            # Optimization Phase
            if target_level >= AnalysisLevel.OPTIMIZATION.value:
                recommendations = execute_optimization(components, analysis_result, progress, tasks.get('optimization', tasks['data_collection']))
                display_analysis_results(analysis_result, patterns, recommendations, target_level)
            
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

def initialize_analysis_components(dbt_project_path: Optional[str] = None) -> Dict:
    """Initialize and validate all required analysis components"""
    try:
        validate_config()
        
        # Initialize data acquisition
        data_acquisition = ClickHouseDataAcquisition(
            host=Config.CLICKHOUSE_HOST,
            port=Config.CLICKHOUSE_PORT,
            user=Config.CLICKHOUSE_USER,
            password=Config.CLICKHOUSE_PASSWORD,
            database=Config.CLICKHOUSE_DATABASE
        )
        
        # Initialize dbt analyzer
        dbt_analyzer = DBTProjectAnalyzer(dbt_project_path or Config.DBT_PROJECT_PATH)
        
        # Initialize cache manager
        cache_manager = QueryLogsCacheManager()
        
        # Initialize AI suggester if API key is available
        ai_suggester = None
        if Config.OPENAI_API_KEY:
            ai_suggester = AISuggester(Config.OPENAI_API_KEY)
        
        return {
            'data_acquisition': data_acquisition,
            'dbt_analyzer': dbt_analyzer,
            'cache_manager': cache_manager,
            'ai_suggester': ai_suggester
        }
        
    except Exception as e:
        logger.error(f"Failed to initialize analysis components: {str(e)}")
        raise RuntimeError(f"Failed to initialize analysis components: {str(e)}")

def prepare_analysis_parameters(days, focus, include_users, exclude_users, query_types):
    """Prepare and validate analysis parameters"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'query_focus': [QueryFocus[focus.upper()]],
        'user_include': include_users.split(',') if include_users else None,
        'user_exclude': exclude_users.split(',') if exclude_users else None,
        'query_types': [QueryType[qt.strip()] for qt in query_types.split(',')] if query_types else None
    }

def create_progress_tasks(progress, target_level):
    """Create progress tracking tasks for each analysis level"""
    tasks = {}
    analysis_levels = {
        'data_collection': AnalysisLevel.DATA_COLLECTION.value,
        'pattern_analysis': AnalysisLevel.PATTERN_ANALYSIS.value,
        'dbt_integration': AnalysisLevel.DBT_INTEGRATION.value,
        'optimization': AnalysisLevel.OPTIMIZATION.value
    }
    
    # Always create data collection task as it's required for all levels
    tasks['data_collection'] = progress.add_task(
        "[cyan]Data Collection: Fetching query logs...",
        total=100
    )
    
    # Add tasks based on target level
    if target_level >= analysis_levels['pattern_analysis']:
        tasks['pattern_analysis'] = progress.add_task(
            "[cyan]Pattern Analysis: Analyzing query patterns...",
            total=100
        )
    
    if target_level >= analysis_levels['dbt_integration']:
        tasks['dbt_integration'] = progress.add_task(
            "[cyan]DBT Integration: Analyzing models...",
            total=100
        )
    
    if target_level >= analysis_levels['optimization']:
        tasks['optimization'] = progress.add_task(
            "[cyan]Optimization: Generating recommendations...",
            total=100
        )
    
    return tasks

def execute_data_collection(components, params, cache, progress, task):
    """Execute data collection level of analysis"""
    try:
        cache_key = f"level1_{params['start_date'].isoformat()}_{params['end_date'].isoformat()}_{params['query_focus'][0].name}"
        
        if cache and components['cache_manager'].has_valid_cache(cache_key):
            cached_data = components['cache_manager'].get_cached_data(cache_key)
            query_logs = [QueryLog.from_dict(log_dict) for log_dict in cached_data]
            progress.update(task, completed=100)
            logger.info("Using cached query logs")
            return query_logs
        
        query_logs = components['data_acquisition'].get_query_logs(
            days=30,  # Using fixed value for now
            focus=params['query_focus'][0],
            include_users=params['user_include'],
            exclude_users=params['user_exclude'],
            query_types=params['query_types'],
            use_cache=cache
        )
        
        if cache:
            # Convert QueryLog objects to dictionaries for caching
            log_dicts = [log.to_dict() for log in query_logs]
            components['cache_manager'].cache_data(cache_key, log_dicts)
            logger.info("Cached query logs")
        
        progress.update(task, completed=100)
        return query_logs
        
    except Exception as e:
        logger.error(f"Data collection failed: {str(e)}", exc_info=True)
        raise RuntimeError(f"Data collection failed: {str(e)}")

def execute_pattern_analysis(components, query_logs, min_frequency, progress, task):
    """Execute pattern analysis level"""
    try:
        cache_key = f"level2_{hashlib.sha256(str(query_logs).encode()).hexdigest()}_{min_frequency}"
        
        if components.get('cache', True) and components['cache_manager'].has_valid_cache(cache_key):
            patterns = components['cache_manager'].get_cached_data(cache_key)
            progress.update(task, completed=100)
            logger.info("Using cached patterns")
        else:
            patterns = components['data_acquisition'].analyze_query_patterns(
                query_logs,
                min_frequency=min_frequency
            )
            
            if components.get('cache', True):
                components['cache_manager'].cache_data(cache_key, patterns)
                logger.info("Cached query patterns")
            
            progress.update(task, completed=100)
        
        return patterns
    except Exception as e:
        logger.error(f"Pattern analysis failed: {str(e)}", exc_info=True)
        raise RuntimeError(f"Pattern analysis failed: {str(e)}")

def execute_dbt_integration(components, patterns, progress, task):
    """Execute DBT integration level"""
    try:
        cache_key = f"level3_{hashlib.sha256(str(patterns).encode()).hexdigest()}_{Config.DBT_PROJECT_PATH}"
        
        if components.get('cache', True) and components['cache_manager'].has_valid_cache(cache_key):
            analysis_result = components['cache_manager'].get_cached_data(cache_key)
            progress.update(task, completed=100)
            logger.info("Using cached DBT analysis")
        else:
            # Get dbt analysis
            dbt_analyzer = components['dbt_analyzer']
            analysis_result = dbt_analyzer.analyze_project()
            
            # Map tables to models for each pattern
            for pattern in patterns:
                for table in pattern.tables_accessed:
                    model_name = dbt_analyzer.get_model_for_table(table)
                    if model_name:
                        pattern.dbt_models_used.add(model_name)
                        if not pattern.model_name:  # Set primary model name if not set
                            pattern.model_name = model_name
            
            # Update analysis result with patterns
            analysis_result.query_patterns = patterns
            analysis_result.calculate_coverage()
            
            if components.get('cache', True):
                components['cache_manager'].cache_data(cache_key, analysis_result)
                logger.info("Cached DBT analysis")
            
            progress.update(task, completed=100)
        
        return analysis_result
    except Exception as e:
        logger.error(f"DBT integration failed: {str(e)}", exc_info=True)
        raise RuntimeError(f"DBT integration failed: {str(e)}")

def execute_optimization(components, analysis_result, progress, task):
    """Execute optimization level"""
    try:
        cache_key = f"level4_{hashlib.sha256(str(analysis_result).encode()).hexdigest()}"
        
        if components.get('cache', True) and components['cache_manager'].has_valid_cache(cache_key):
            recommendations = components['cache_manager'].get_cached_data(cache_key)
            progress.update(task, completed=100)
            logger.info("Using cached recommendations")
        else:
            recommendations = components['ai_suggester'].generate_suggestions(
                analysis_result=analysis_result,
                max_patterns=5,
                max_tokens=8000,
                confidence_threshold=0.8
            )
            
            if components.get('cache', True):
                components['cache_manager'].cache_data(cache_key, recommendations)
                logger.info("Cached recommendations")
            
            progress.update(task, completed=100)
        
        return recommendations
    except Exception as e:
        logger.error(f"Optimization analysis failed: {str(e)}", exc_info=True)
        raise RuntimeError(f"Optimization analysis failed: {str(e)}")

def display_analysis_results(analysis_result, patterns, recommendations, achieved_level):
    """Display analysis results based on achieved level"""
    console.print("\n[bold green]Analysis Complete![/bold green]\n")
    
    if patterns:
        console.print("[bold]Query Pattern Analysis[/bold]")
        display_query_patterns(patterns)
    
    if analysis_result:
        console.print("\n[bold]DBT Model Coverage[/bold]")
        display_model_coverage(analysis_result)
    
    if recommendations:
        console.print("\n[bold]AI Recommendations[/bold]")
        display_recommendations(recommendations)
    
    console.print(Panel(
        f"Analysis completed at level: [cyan]{achieved_level}[/cyan]",
        title="Analysis Summary",
        border_style="green"
    ))

@cli.command()
@click.option('--output', type=click.Path(), help='Output file path (JSON)')
def export(output: Optional[str]):
    """Export the latest analysis results"""
    try:
        cache_manager = QueryLogsCacheManager()
        latest_result = cache_manager.get_latest_result()
        
        if not latest_result:
            console.print("[yellow]No analysis results found in cache[/yellow]")
            return
        
        result_dict = {
            'timestamp': latest_result.timestamp.isoformat(),
            'query_patterns': [pattern.__dict__ for pattern in latest_result.query_patterns],
            'model_coverage': latest_result.model_coverage,
            'uncovered_tables': list(latest_result.uncovered_tables)
        }
        
        if output:
            with open(output, 'w') as f:
                json.dump(result_dict, f, indent=2)
            console.print(f"[green]Results exported to {output}[/green]")
        else:
            console.print(json.dumps(result_dict, indent=2))
            
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

if __name__ == '__main__':
    cli()
