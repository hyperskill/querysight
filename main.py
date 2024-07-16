import argparse
import re
import os
from typing import List, Dict
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.dbt_analyzer import DBTProjectAnalyzer
from utils.ai_suggester import AISuggester
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_clickhouse_credentials() -> Dict[str, str]:
    def clean_input(prompt: str) -> str:
        user_input = input(prompt)
        user_input = user_input.strip()
        return user_input

    return {
        'host': clean_input("Enter ClickHouse host: "),
        'port': int(clean_input("Enter ClickHouse port: ")),
        'username': clean_input("Enter ClickHouse username: "),
        'password': clean_input("Enter ClickHouse password: ")
    }

def main():
    parser = argparse.ArgumentParser(description="Analyze ClickHouse queries and suggest dbt improvements")
    parser.add_argument("--dbt-project", required=True, help="Path to the dbt project")
    parser.add_argument("--start-date", required=True, help="Start date for query analysis (YYYY-MM-DD)")
    parser.add_argument("--end-date", required=True, help="End date for query analysis (YYYY-MM-DD)")
    parser.add_argument("--openai-api-key", required=True, help="OpenAI API key")

    args = parser.parse_args()

    # Get ClickHouse credentials
    clickhouse_creds = get_clickhouse_credentials()

    # Initialize ClickHouse data acquisition
    clickhouse = ClickHouseDataAcquisition(**clickhouse_creds)
    
    print("Retrieving and preprocessing query logs...")
    query_logs = clickhouse.retrieve_query_logs(args.start_date, args.end_date)
    preprocessed_data = clickhouse.preprocess_query_data(query_logs)
    
    print("Analyzing queries...")
    query_analysis = clickhouse.analyze_queries(preprocessed_data)
    
    print("Analyzing dbt project...")
    dbt_analyzer = DBTProjectAnalyzer(args.dbt_project)
    dbt_structure = dbt_analyzer.analyze_project()
    
    print("Generating AI suggestions...")
    ai_suggester = AISuggester(args.openai_api_key)
    suggestions = ai_suggester.generate_suggestions(query_analysis, dbt_structure)
    
    print("\nSuggestions for dbt project improvements:")
    for i, suggestion in enumerate(suggestions, 1):
        print(f"{i}. {suggestion}")

if __name__ == "__main__":
    main()
