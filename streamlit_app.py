import streamlit as st
import os
from typing import Dict
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.dbt_analyzer import DBTProjectAnalyzer
from utils.ai_suggester import AISuggester

def get_clickhouse_credentials() -> Dict[str, str]:
    st.sidebar.subheader("ClickHouse Credentials")
    host = st.sidebar.text_input("ClickHouse host:")
    port = st.sidebar.number_input("ClickHouse port:", min_value=1, max_value=65535, value=8123)
    username = st.sidebar.text_input("ClickHouse username:")
    password = st.sidebar.text_input("ClickHouse password:", type="password")
    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }

def main():
    st.title("QuerySight Analyzer and dbt Improver")

    # Sidebar for inputs
    st.sidebar.header("Configuration")
    dbt_project = st.sidebar.text_input("Path to dbt project:")
    start_date = st.sidebar.date_input("Start date for query analysis:")
    end_date = st.sidebar.date_input("End date for query analysis:")
    openai_api_key = st.sidebar.text_input("OpenAI API key:", type="password")

    # Get ClickHouse credentials in the sidebar
    clickhouse_creds = get_clickhouse_credentials()

    # Main content
    if st.sidebar.button("Analyze and Suggest"):
        if not all([dbt_project, start_date, end_date, openai_api_key]):
            st.error("Please fill in all the required fields in the sidebar.")
            return

        if not all(clickhouse_creds.values()):
            st.error("Please provide all ClickHouse credentials.")
            return

        with st.spinner("Initializing..."):
            clickhouse = ClickHouseDataAcquisition(**clickhouse_creds)

        with st.spinner("Retrieving and preprocessing query logs..."):
            query_logs = clickhouse.retrieve_query_logs(str(start_date), str(end_date))
            preprocessed_data = clickhouse.preprocess_query_data(query_logs)

        with st.spinner("Analyzing queries..."):
            query_analysis = clickhouse.analyze_queries(preprocessed_data)
            st.subheader("Query Analysis")
            st.json(query_analysis)

        with st.spinner("Analyzing dbt project..."):
            dbt_analyzer = DBTProjectAnalyzer(dbt_project)
            dbt_structure = dbt_analyzer.analyze_project()
            st.subheader("dbt Project Structure")
            st.json(dbt_structure)

        with st.spinner("Generating AI suggestions..."):
            ai_suggester = AISuggester(openai_api_key)
            suggestions = ai_suggester.generate_suggestions(query_analysis, dbt_structure)
            st.subheader("Suggestions for dbt Project Improvements")
            for i, suggestion in enumerate(suggestions, 1):
                st.write(f"{i}. {suggestion}")

if __name__ == "__main__":
    main()