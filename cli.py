#!/usr/bin/env python3

"""Command-line interface for QuerySight.
Provides tools for analyzing ClickHouse query patterns and generating optimization recommendations."""

import json
import logging
import sys
import hashlib
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Optional, Dict, List

import click
from rich.console import Console
from rich import box
from rich.panel import Panel
from rich.progress import Progress
from rich.table import Table
from rich.syntax import Syntax
from rich.text import Text

from utils.config import Config
from utils.models import (
    AnalysisResult, QueryPattern, QueryLog,
    AIRecommendation, QueryKind, QueryFocus, DBTModel
)
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.dbt_analyzer import DBTProjectAnalyzer
from utils.ai_suggester import AISuggester
from utils.cache_manager import QueryLogsCacheManager

logger = logging.getLogger(__name__)

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

def display_query_patterns(patterns: List[QueryPattern], sort_by: str = 'duration', page_size: int = 20):
    """Display analyzed query patterns in a table with sorting and pagination"""
    if not patterns:
        console.print("[yellow]No query patterns found[/yellow]")
        return

    # Sort patterns
    if sort_by == 'frequency':
        patterns.sort(key=lambda p: p.frequency, reverse=True)
    elif sort_by == 'duration':
        patterns.sort(key=lambda p: p.avg_duration_ms, reverse=True)
    elif sort_by == 'memory':
        patterns.sort(key=lambda p: sum(p.memory_usage) / p.frequency if p.frequency > 0 else 0, reverse=True)

    # Calculate total pages
    total_patterns = len(patterns)
    total_pages = (total_patterns + page_size - 1) // page_size

    for current_page in range(1, total_pages + 1):
        start_idx = (current_page - 1) * page_size
        end_idx = min(start_idx + page_size, total_patterns)
        page_patterns = patterns[start_idx:end_idx]

        table = Table(
            title=f"Query Patterns (Page {current_page}/{total_pages})",
            show_lines=True,
            expand=True
        )

        # Add columns
        table.add_column("Pattern ID", style="cyan", no_wrap=True)
        table.add_column("Frequency", justify="right")
        table.add_column("Avg Duration", justify="right")
        table.add_column("Memory (MB)", justify="right")
        table.add_column("Users", style="blue")
        table.add_column("Tables", style="magenta")
        table.add_column("First Seen", style="green")
        table.add_column("Last Seen", style="green")

        # Add rows with color coding
        for pattern in page_patterns:
            # Color code based on duration
            duration_style = (
                "red" if pattern.avg_duration_ms > 1000 else  # > 1s
                "yellow" if pattern.avg_duration_ms > 100 else  # > 100ms
                "green"
            )

            avg_memory_mb = pattern.memory_usage / (1024 * 1024) if pattern.memory_usage else 0
            users_display = (", ".join(sorted(pattern.users)[:3]) + "...") if len(pattern.users) > 3 else ", ".join(pattern.users)
            tables_display = (", ".join(sorted(pattern.tables_accessed)[:3]) + "...") if len(pattern.tables_accessed) > 3 else ", ".join(pattern.tables_accessed)

            table.add_row(
                pattern.pattern_id[:12] + "...",
                str(pattern.frequency),
                Text(f"{pattern.avg_duration_ms:,.2f} ms", style=duration_style),
                f"{avg_memory_mb:,.2f}",
                users_display or "N/A",
                tables_display or "N/A",
                pattern.first_seen.strftime("%Y-%m-%d %H:%M") if pattern.first_seen else "N/A",
                pattern.last_seen.strftime("%Y-%m-%d %H:%M") if pattern.last_seen else "N/A"
            )

        console.print(table)
        if current_page < total_pages:
            console.print("\n" + "─" * 80 + "\n")  # Page separator

    console.print(f"\nTotal Patterns: {total_patterns}")
    
    # Print summary statistics
    console.print("\n[bold]Summary Statistics[/bold]")
    stats_table = Table(show_header=False, show_lines=True)
    stats_table.add_column("Metric", style="cyan")
    stats_table.add_column("Value", style="green")
    
    # Calculate statistics
    total_queries = sum(p.frequency for p in patterns)
    total_duration_ms = sum(p.avg_duration_ms * p.frequency for p in patterns)
    total_memory = sum(p.memory_usage for p in patterns)
    unique_users = len(set().union(*[p.users for p in patterns]))
    unique_tables = len(set().union(*[p.tables_accessed for p in patterns]))
    
    # Calculate percentages for slow/medium/fast queries
    slow_queries = sum(p.frequency for p in patterns if p.avg_duration_ms > 1000)
    medium_queries = sum(p.frequency for p in patterns if 100 < p.avg_duration_ms <= 1000)
    fast_queries = sum(p.frequency for p in patterns if p.avg_duration_ms <= 100)
    
    # Add rows with enhanced formatting
    stats_table.add_row("Query Count", f"{total_queries:,}")
    stats_table.add_row("Total Duration", f"{total_duration_ms/1000:,.2f} seconds")
    stats_table.add_row("Avg Duration per Query", f"{total_duration_ms/total_queries:,.2f} ms")
    stats_table.add_row("Total Memory Usage", f"{total_memory/(1024*1024):,.2f} MB")
    stats_table.add_row("Avg Memory per Query", f"{total_memory/(1024*1024*total_queries):,.2f} MB")
    stats_table.add_row("Unique Users", str(unique_users))
    stats_table.add_row("Unique Tables", str(unique_tables))
    stats_table.add_row("Query Speed Distribution", 
        f"Slow (>1s): {slow_queries/total_queries*100:.1f}%\n"
        f"Medium (100ms-1s): {medium_queries/total_queries*100:.1f}%\n"
        f"Fast (<100ms): {fast_queries/total_queries*100:.1f}%"
    )
    
    console.print(stats_table)

def display_model_coverage(result: AnalysisResult):
    """Display dbt model coverage metrics with hierarchical relationships"""
    if not result or not result.query_patterns:
        console.print("[yellow]No query patterns available[/yellow]")
        return

    # Display pattern-based coverage
    console.print("\n[bold cyan]DBT Model Coverage Analysis[/bold cyan]")
    
    # First display patterns with model coverage
    patterns_with_models = [p for p in result.query_patterns if p.dbt_models_used]
    if patterns_with_models:
        console.print("\n[bold green]Patterns Using DBT Models[/bold green]")
        for pattern in patterns_with_models:
            display_pattern_coverage(pattern, result)
            console.print()  # Add spacing between patterns
    
    # Then display patterns with only unmapped tables
    patterns_unmapped = [p for p in result.query_patterns 
                        if not p.dbt_models_used and p.tables_accessed]
    if patterns_unmapped:
        console.print("\n[bold yellow]Patterns Using Only Unmapped Tables[/bold yellow]")
        for pattern in patterns_unmapped:
            display_pattern_coverage(pattern, result)
            console.print()  # Add spacing between patterns
    
    # Finally display patterns with no table access
    patterns_no_tables = [p for p in result.query_patterns 
                         if not p.dbt_models_used and not p.tables_accessed]
    if patterns_no_tables:
        console.print("\n[bold red]Patterns Without Table Access[/bold red]")
        for pattern in patterns_no_tables:
            display_pattern_coverage(pattern, result)
            console.print()  # Add spacing between patterns

    # Display uncovered tables summary at the end
    if result.uncovered_tables:
        console.print("\n[bold yellow]Uncovered Tables Summary[/bold yellow]")
        console.print(", ".join(sorted(result.uncovered_tables)))

def display_pattern_coverage(pattern: QueryPattern, result: AnalysisResult):
    """Display coverage information for a single pattern"""
    pattern_table = Table(show_header=False, box=box.ROUNDED)
    pattern_table.add_column("Property", style="bold blue")
    pattern_table.add_column("Value")
    
    pattern_table.add_row("Pattern ID", pattern.pattern_id)
    pattern_table.add_row("Frequency", str(pattern.frequency))
    pattern_table.add_row("Avg Duration", f"{pattern.avg_duration_ms:.2f}ms")
    pattern_table.add_row("SQL Pattern", pattern.sql_pattern)
    
    # Create nested table for model relationships
    models_table = Table(show_header=True, box=box.SIMPLE)
    models_table.add_column("Model Type", style="bold green")
    models_table.add_column("Models")
    
    # Add directly used models
    if pattern.dbt_models_used:
        models_table.add_row(
            "Direct Models",
            ", ".join(sorted(pattern.dbt_models_used))
        )
        
        # Collect all parent and child models
        all_parents = set()
        all_children = set()
        for model_name in pattern.dbt_models_used:
            model = result.dbt_models.get(model_name)
            if model:
                all_parents.update(model.depends_on)
                all_children.update(model.referenced_by)
        
        # Remove direct models from parents/children to avoid duplication
        all_parents -= pattern.dbt_models_used
        all_children -= pattern.dbt_models_used
        
        # Add parent models if any
        if all_parents:
            models_table.add_row(
                "Parent Models",
                ", ".join(sorted(all_parents))
            )
        
        # Add child models if any
        if all_children:
            models_table.add_row(
                "Child Models",
                ", ".join(sorted(all_children))
            )
    
    # Add tables that couldn't be mapped to models
    unmapped_tables = pattern.tables_accessed - {
        model_name for model_name in pattern.dbt_models_used
    }
    if unmapped_tables:
        models_table.add_row(
            "Unmapped Tables",
            ", ".join(sorted(unmapped_tables))
        )
    
    pattern_table.add_row("Model Coverage", models_table)
    console.print(pattern_table)

@click.group()
def cli():
    """QuerySight CLI - A tool for analyzing ClickHouse query patterns and optimizing dbt models.

    Available Commands:
      analyze    Analyze query patterns and generate optimization recommendations
      export     Export the latest analysis results to JSON format
    """
    pass

@cli.command()
@click.option('--days', default=7, help='Number of days of query history to analyze')
@click.option('--focus', default='all', help='Analysis focus: slow (long-running queries), frequent (high-frequency queries), or all')
@click.option('--min-frequency', default=2, help='Minimum frequency threshold for query patterns')
@click.option('--sample-size', default=1.0, help='Sample size ratio (0.0-1.0) of query logs to analyze')
@click.option('--batch-size', default=1000, help='Number of queries to process in each batch')
@click.option('--include-users', help='Filter specific users to include (comma-separated)')
@click.option('--exclude-users', help='Filter specific users to exclude (comma-separated)')
@click.option('--query-kinds', help='Types of queries to analyze (comma-separated)')
@click.option('--cache/--no-cache', default=True, help='Enable/disable caching of query logs')
@click.option('--force-reset', is_flag=True, help='Force reset of cache database')
@click.option('--level', default='optimization', help='Analysis depth: data_collection, pattern_analysis, dbt_integration, or optimization')
@click.option('--dbt-project', help='Path to dbt project for model analysis')
@click.option('--select-patterns', help='Filter specific query patterns to analyze (comma-separated IDs)')
@click.option('--select-models', help='Filter specific dbt models to analyze (comma-separated names)')
@click.option('--sort-by', type=click.Choice(['frequency', 'duration', 'memory']), default='duration',
              help='Sort patterns by frequency, duration, or memory usage')
@click.option('--page-size', type=int, default=20, help='Number of patterns to show per page')
def analyze(days, focus, min_frequency, sample_size, batch_size, include_users,
           exclude_users, query_kinds, cache, force_reset, level, dbt_project, select_patterns,
           select_models, sort_by, page_size):
    try:
        logger.info("Starting analysis with parameters:")
        logger.info(f"  Days: {days}")
        logger.info(f"  Focus: {focus}")
        logger.info(f"  Include users: {include_users}")
        logger.info(f"  Query kinds: {query_kinds}")
        logger.info(f"  Level: {level}")
        
        # Initialize components and parameters
        components = initialize_analysis_components(dbt_project, force_reset)
        logger.info("Components initialized")
        
        params = prepare_analysis_parameters(days, focus, include_users, exclude_users, query_kinds)
        logger.info(f"Analysis parameters prepared: {params}")
        
        target_level = level.lower()
        logger.info(f"Target analysis level: {target_level}")
        
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

def initialize_analysis_components(dbt_project_path: Optional[str] = None, force_reset: bool = False) -> Dict:
    """Initialize and validate all required analysis components"""
    try:
        validate_config()
        
        # Initialize data acquisition
        data_acquisition = ClickHouseDataAcquisition(
            host=Config.CLICKHOUSE_HOST,
            port=Config.CLICKHOUSE_PORT,
            user=Config.CLICKHOUSE_USER,
            password=Config.CLICKHOUSE_PASSWORD,
            database=Config.CLICKHOUSE_DATABASE,
            force_reset=force_reset
        )
        
        # Initialize dbt analyzer
        dbt_analyzer = DBTProjectAnalyzer(dbt_project_path or Config.DBT_PROJECT_PATH)
        
        # Initialize cache manager
        cache_manager = QueryLogsCacheManager(force_reset=force_reset)
        
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

def prepare_analysis_parameters(days, focus, include_users, exclude_users, query_kinds):
    """Prepare and validate analysis parameters"""
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # Handle query kinds
    if query_kinds:
        kinds = [qt.strip().upper() for qt in query_kinds.split(',')]
        query_kinds_list = [QueryKind[qt] for qt in kinds]
    else:
        query_kinds_list = None
    
    # Handle focus - always return a single QueryFocus enum
    focus_enum = QueryFocus.ALL if not focus else QueryFocus[focus.upper()]
    
    return {
        'start_date': start_date,
        'end_date': end_date,
        'query_focus': focus_enum,
        'user_include': [u.lower() for u in include_users.split(',')] if include_users else None,
        'user_exclude': [u.lower() for u in exclude_users.split(',')] if exclude_users else None,
        'query_kinds': query_kinds_list
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
        # Generate cache key
        cache_key = f"level1_{params['start_date'].isoformat()}_{params['end_date'].isoformat()}_{params['query_focus'].name}"
        
        if cache and components['cache_manager'].has_valid_cache(cache_key):
            query_logs = components['cache_manager'].get_cached_data(cache_key)
            progress.update(task, completed=100)
            logger.info("Using cached query logs")
        else:
            query_logs = components['data_acquisition'].get_query_logs(
                days=(params['end_date'] - params['start_date']).days,
                focus=params['query_focus'],
                include_users=params['user_include'],
                exclude_users=params['user_exclude'],
                query_kinds=params['query_kinds']
            )
            
            if cache:
                components['cache_manager'].cache_data(cache_key, query_logs)
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
        # Generate cache key based on pattern IDs to ensure consistent enrichment
        pattern_ids = sorted([p.pattern_id for p in patterns])
        cache_key = f"level3_{hashlib.sha256(','.join(pattern_ids).encode()).hexdigest()}_{Config.DBT_PROJECT_PATH}"
        
        if components.get('cache', True) and components['cache_manager'].has_valid_cache(cache_key):
            analysis_result = components['cache_manager'].get_cached_data(cache_key)
            
            # Only proceed if we got a valid result from cache
            if analysis_result is not None:
                # Ensure dbt_mapper is set even when using cached data
                if 'dbt_analyzer' in components:
                    analysis_result.dbt_mapper = components['dbt_analyzer']
                    analysis_result.calculate_coverage()  # Recalculate with mapper
                
                progress.update(task, completed=100)
                logger.info("Using cached DBT analysis")
                return analysis_result
        
        # Get dbt analysis
        dbt_analyzer = components['dbt_analyzer']
        analysis_result = dbt_analyzer.analyze_project()
        
        # Enrich patterns with historical data and DBT info
        enriched_patterns = components['cache_manager'].enrich_patterns(patterns, cache_key)
        
        # For each pattern, try to map tables to DBT models
        for pattern in enriched_patterns:
            for table in pattern.tables_accessed:
                model_name = dbt_analyzer.get_model_name(table)
                if model_name:
                    pattern.dbt_models_used.add(model_name)
            
            # Cache the updated pattern
            components['cache_manager'].cache_pattern(pattern, cache_key)
        
        # Update analysis result with enriched patterns and recalculate coverage
        analysis_result.query_patterns = enriched_patterns
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
            recommendations = components['ai_suggester'].generate_recommendations(
                patterns=analysis_result.query_patterns,
                dbt_models=analysis_result.dbt_models
            )
            
            if components.get('cache', True):
                # Convert recommendations to dictionaries before caching
                recommendations_dict = [rec.to_dict() for rec in recommendations]
                components['cache_manager'].cache_data(cache_key, recommendations_dict)
                logger.info("Cached recommendations")
            
            progress.update(task, completed=100)
        
        return recommendations
    except Exception as e:
        logger.error(f"Optimization analysis failed: {str(e)}", exc_info=True)
        raise RuntimeError(f"Optimization analysis failed: {str(e)}")

def display_analysis_results(analysis_result, patterns, recommendations, achieved_level):
    """Display analysis results based on achieved level"""
    console.print("\n[bold green]Analysis Complete![/bold green]\n")
    
    # Always show query count first
    if isinstance(patterns, list):
        console.print(f"[bold]Found {len(patterns)} query patterns[/bold]")
    
    # Always show patterns if we have them
    if patterns:
        console.print("\n[bold]Query Pattern Analysis[/bold]")
        display_query_patterns(patterns, sort_by='duration', page_size=20)
    else:
        console.print("\n[yellow]No query patterns found[/yellow]")
    
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

def display_recommendations(recommendations: List[AIRecommendation]) -> None:
    """Display AI-generated optimization recommendations"""
    if not recommendations:
        console.print("[yellow]No optimization recommendations generated[/yellow]")
        return

    console.print("\n[bold]AI Optimization Recommendations[/bold]")
    for i, rec in enumerate(recommendations, 1):
        panel = Panel(
            f"Type: [cyan]{rec.type}[/cyan]\n"
            f"Impact: [{'green' if rec.impact == 'HIGH' else 'yellow' if rec.impact == 'MEDIUM' else 'red'}]{rec.impact}[/]\n"
            f"Description: {rec.description}\n"
            + (f"Suggested SQL:\n[blue]{rec.suggested_sql}[/blue]" if rec.suggested_sql else ""),
            title=f"Recommendation {i}",
            expand=False
        )
        console.print(panel)

@cli.command()
@click.option('--output', type=click.Path(), help='Output file path (JSON)')
def export(output: Optional[str]):
    """Export the latest analysis results to a JSON file.
    
    If no output file is specified, prints the results to stdout. The export
    includes query patterns, model coverage metrics, and uncovered tables from
    the most recent analysis run.
    """
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
