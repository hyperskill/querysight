import streamlit as st
import os
from typing import Dict
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.dbt_analyzer import DBTProjectAnalyzer
from utils.ai_suggester import AISuggester

# Set page configuration
st.set_page_config(page_title="QuerySight Analyzer", page_icon="🔍", layout="wide")

# Custom CSS to improve the design
st.markdown("""
<style>
    .reportview-container {
        background: #f0f2f6
    }
    .sidebar .sidebar-content {
        background: #ffffff
    }
    .Widget>label {
        color: #31333F;
        font-weight: bold;
    }
    .stButton>button {
        color: #ffffff;
        background-color: #4CAF50;
        border-radius: 5px;
    }
    .stButton>button:hover {
        background-color: #45a049;
    }
</style>
""", unsafe_allow_html=True)

def get_clickhouse_credentials() -> Dict[str, str]:
    with st.sidebar.expander("ClickHouse Credentials", expanded=False):
        host = st.text_input("Host:", key="ch_host")
        port = st.number_input("Port:", min_value=1, max_value=65535, value=8123, key="ch_port")
        username = st.text_input("Username:", key="ch_username")
        password = st.text_input("Password:", type="password", key="ch_password")
    return {
        'host': host,
        'port': port,
        'username': username,
        'password': password
    }

def main():
    st.title("🔍 QuerySight Analyzer and dbt Improver")

    # Sidebar for inputs
    st.sidebar.header("Configuration")
    
    dbt_project = st.sidebar.text_input("Path to dbt project:", key="dbt_project")
    col1, col2 = st.sidebar.columns(2)
    with col1:
        start_date = st.date_input("Start date:", key="start_date")
    with col2:
        end_date = st.date_input("End date:", key="end_date")
    
    openai_api_key = st.sidebar.text_input("OpenAI API key:", type="password", key="openai_key")

    # Get ClickHouse credentials in the sidebar
    clickhouse_creds = get_clickhouse_credentials()

    # Main content
    if st.sidebar.button("🚀 Analyze and Suggest", key="analyze_button"):
        if not all([dbt_project, start_date, end_date, openai_api_key]):
            st.error("❗ Please fill in all the required fields in the sidebar.")
            return

        if not all(clickhouse_creds.values()):
            st.error("❗ Please provide all ClickHouse credentials.")
            return

        with st.spinner("🔄 Initializing..."):
            clickhouse = ClickHouseDataAcquisition(**clickhouse_creds)

        with st.spinner("📊 Retrieving and preprocessing query logs..."):
            query_logs = clickhouse.retrieve_query_logs(str(start_date), str(end_date))
            preprocessed_data = clickhouse.preprocess_query_data(query_logs)

        col1, col2 = st.columns(2)

        with col1:
            with st.spinner("🔍 Analyzing queries..."):
                query_analysis = clickhouse.analyze_queries(preprocessed_data)
                st.subheader("📊 Query Analysis")
                st.json(query_analysis)

        with col2:
            with st.spinner("📁 Analyzing dbt project..."):
                dbt_analyzer = DBTProjectAnalyzer(dbt_project)
                dbt_structure = dbt_analyzer.analyze_project()
                st.subheader("🗂️ dbt Project Structure")
                st.json(dbt_structure)

        with st.spinner("🤖 Generating AI suggestions..."):
            ai_suggester = AISuggester(openai_api_key)
            suggestions = ai_suggester.generate_suggestions(query_analysis, dbt_structure)
            st.subheader("💡 Suggestions for dbt Project Improvements")
            for i, suggestion in enumerate(suggestions, 1):
                st.write(f"{i}. {suggestion}")

if __name__ == "__main__":
    main()
