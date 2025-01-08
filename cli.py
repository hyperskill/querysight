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
    table = Table(title="Query Patterns Analysis")
    
    table.add_column("Pattern ID", style="cyan")
    table.add_column("Model", style="blue")
    table.add_column("Frequency", justify="right", style="green")
    table.add_column("Avg Duration", justify="right", style="yellow")
    table.add_column("Last Seen", style="magenta")
    
    for pattern in patterns:
        table.add_row(
            pattern.pattern_id[:8],
            pattern.model_name,
            str(pattern.frequency),
            f"{pattern.avg_duration_ms:.2f}ms",
            pattern.last_seen.strftime("%Y-%m-%d %H:%M") if pattern.last_seen else "N/A"
        )
    
    console.print(table)

def display_model_coverage(result: AnalysisResult) -> None:
    """Display dbt model coverage metrics"""
    table = Table(title="DBT Model Coverage")
    
    table.add_column("Metric", style="blue")
    table.add_column("Value", justify="right", style="green")
    
    table.add_row(
        "Total Models",
        str(len(result.dbt_models))
    )
    table.add_row(
        "Coverage",
        f"{result.model_coverage.get('covered', 0):.1f}%"
    )
    table.add_row(
        "Uncovered Tables",
        str(len(result.uncovered_tables))
    )
    
    console.print(table)
    
    if result.uncovered_tables:
        console.print("\n[yellow]Uncovered Tables:[/yellow]")
        for table in sorted(result.uncovered_tables):
            console.print(f"  - {table}")

def display_recommendations(recommendations: List[AIRecommendation]) -> None:
    """Display AI-generated recommendations"""
    for i, rec in enumerate(recommendations, 1):
        console.print(f"\n[bold cyan]Recommendation {i}[/bold cyan]")
        console.print(Panel(
            f"{rec.description}\n\n" +
            f"[bold]Impact Score:[/bold] {rec.impact_score:.2f}\n" +
            f"[bold]Difficulty:[/bold] {rec.implementation_difficulty:.2f}\n" +
            f"[bold]Priority Score:[/bold] {rec.priority_score:.2f}\n" +
            (f"\n[bold]Suggested SQL:[/bold]\n{rec.suggested_sql}" if rec.suggested_sql else ""),
            title=rec.suggestion_type,
            border_style="blue"
        ))

@click.group()
def cli():
    """QuerySight CLI - Analyze ClickHouse query patterns and optimize dbt models"""
    pass

@cli.command()
@click.option('--days', default=7, help='Number of days to analyze')
@click.option('--focus', type=click.Choice(['SLOW', 'FREQUENT', 'ALL']), default='ALL',
              help='Analysis focus')
@click.option('--min-frequency', default=5, help='Minimum query frequency to consider')
@click.option('--sample-size', default=0.1, type=float, help='Fraction of data to sample (0.0 to 1.0)')
@click.option('--batch-size', default=100, type=int, help='Number of rows to retrieve per batch')
@click.option('--include-users', help='Comma-separated list of users to include')
@click.option('--exclude-users', help='Comma-separated list of users to exclude')
@click.option('--query-types', help='Comma-separated list of query types (SELECT,INSERT,CREATE,ALTER,DROP)')
@click.option('--cache/--no-cache', default=True, help='Use cached results if available')
def analyze(
    days: int,
    focus: str,
    min_frequency: int,
    sample_size: float,
    batch_size: int,
    include_users: Optional[str],
    exclude_users: Optional[str],
    query_types: Optional[str],
    cache: bool
):
    """Analyze query patterns and generate recommendations"""
    try:
        validate_config()
        
        # Initialize components
        data_acquisition = ClickHouseDataAcquisition(
            host=Config.CLICKHOUSE_HOST,
            port=Config.CLICKHOUSE_PORT,
            user=Config.CLICKHOUSE_USER,
            password=Config.CLICKHOUSE_PASSWORD,
            database=Config.CLICKHOUSE_DATABASE
        )
        
        validate_connection(data_acquisition)
        
        cache_manager = QueryLogsCacheManager()
        logger.info(f"DBT Project Path from config: {Config.DBT_PROJECT_PATH}")
        dbt_analyzer = DBTProjectAnalyzer(Config.DBT_PROJECT_PATH)
        ai_suggester = AISuggester(Config.OPENAI_API_KEY)
        
        # Analysis parameters
        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)
        query_focus = [QueryFocus[focus]]  # Convert to list for the API
        
        # Parse user lists
        user_include = include_users.split(',') if include_users else None
        user_exclude = exclude_users.split(',') if exclude_users else None
        
        # Parse query types
        parsed_query_types = None
        if query_types:
            parsed_query_types = [QueryType[qt.strip()] for qt in query_types.split(',')]
        
        with Progress() as progress:
            # Step 1: Get query logs
            task1 = progress.add_task("[cyan]Fetching query logs...", total=100)
            
            if cache and cache_manager.has_valid_cache(start_date, end_date, focus):
                logs_result = cache_manager.get_cached_logs()
                progress.update(task1, completed=100)
            else:
                logs_result = data_acquisition.get_query_logs(
                    start_date=start_date,
                    end_date=end_date,
                    batch_size=batch_size,
                    sample_size=sample_size,
                    user_include=user_include,
                    user_exclude=user_exclude,
                    query_focus=query_focus,
                    query_types=parsed_query_types
                )
                if logs_result['status'] == 'error':
                    console.print(f"[red]Error: {logs_result['error']}[/red]")
                    return
                
                cache_manager.cache_logs(logs_result['data'], start_date, end_date, focus)
                progress.update(task1, completed=100)
            
            # Step 2: Analyze patterns
            task2 = progress.add_task("[cyan]Analyzing query patterns...", total=100)
            # Convert dictionaries back to QueryLog objects
            query_logs = [QueryLog.from_dict(log_dict) for log_dict in logs_result['data']]
            patterns = data_acquisition.analyze_query_patterns(
                query_logs,
                min_frequency=min_frequency
            )
            progress.update(task2, completed=100)
            
            # Step 3: Analyze dbt project
            task3 = progress.add_task("[cyan]Analyzing dbt models...", total=100)
            dbt_analysis = dbt_analyzer.analyze_project()
            progress.update(task3, completed=100)
            
            # Create analysis result
            analysis_result = AnalysisResult(
                timestamp=datetime.now(),
                query_patterns=patterns,
                dbt_models=dbt_analysis.dbt_models,
                uncovered_tables=set(),
                model_coverage={}
            )
            analysis_result.calculate_coverage()
            
            # Step 4: Generate recommendations
            task4 = progress.add_task("[cyan]Generating recommendations...", total=100)
            recommendations = ai_suggester.generate_suggestions(
                analysis_result=analysis_result,
                max_patterns=5,
                max_tokens=8000,
                confidence_threshold=0.8
            )
            progress.update(task4, completed=100)
        
        # Display results
        console.print("\n[bold green]Analysis Complete![/bold green]\n")
        
        console.print("[bold]Query Pattern Analysis[/bold]")
        display_query_patterns(patterns)
        
        console.print("\n[bold]DBT Model Coverage[/bold]")
        display_model_coverage(analysis_result)
        
        console.print("\n[bold]AI Recommendations[/bold]")
        display_recommendations(recommendations)
        
    except Exception as e:
        console.print(f"[red]Error: {str(e)}[/red]")
        sys.exit(1)

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
