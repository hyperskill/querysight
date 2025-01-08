import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
import os
import traceback
from utils.data_acquisition import ClickHouseDataAcquisition
from utils.dbt_analyzer import DBTProjectAnalyzer
from utils.ai_suggester import AISuggester
from utils.pdf_generator import PDFReportGenerator
from utils.logger import setup_logger
from utils.config import Config
from utils.sampling_wizard import SamplingWizard
from utils.visualization import (
    render_analysis_dashboard,
    render_recommendations
)
import altair as alt

# Set up logger for this module
logger = setup_logger(__name__, log_level="DEBUG")

def init_session_state():
    """Initialize session state variables"""
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'query_patterns' not in st.session_state:
        st.session_state.query_patterns = []  # Initialize as empty list instead of None
    if 'processed_patterns' not in st.session_state:
        st.session_state.processed_patterns = set()
    if 'accumulated_suggestions' not in st.session_state:
        st.session_state.accumulated_suggestions = []
    if 'current_sampling_config' not in st.session_state:
        st.session_state.current_sampling_config = None
    if 'selected_patterns' not in st.session_state:
        st.session_state.selected_patterns = set()
    if 'can_suggest' not in st.session_state:
        st.session_state.can_suggest = False

def render_sidebar():
    """Render the configuration sidebar"""
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Configuration status at the top
        st.subheader("Status")
        is_valid_config, missing_vars = Config.validate_config()
        if not is_valid_config:
            st.error("⚠️ Missing required configuration:")
            for var in missing_vars:
                st.markdown(f"- {var}")
            st.info("💡 Set these in your .env file")
        else:
            st.success("✅ Configuration valid")

        # Project configuration
        st.subheader("🏗️ Project")
        dbt_project_path = st.text_input(
            "dbt Project Path",
            value=Config.DBT_PROJECT_PATH,
            placeholder="/path/to/dbt/project",
            help="Path to your dbt project directory"
        )
        
        if dbt_project_path != Config.DBT_PROJECT_PATH:
            st.info("Note: Update .env file to make changes permanent")

        # ClickHouse configuration
        st.subheader("🔍 ClickHouse")
        ch_host = st.text_input("Host", value=Config.CLICKHOUSE_HOST)
        ch_port = st.number_input("Port", value=Config.CLICKHOUSE_PORT, min_value=1, max_value=65535)
        ch_user = st.text_input("Username", value=Config.CLICKHOUSE_USER)
        ch_password = st.text_input("Password", value=Config.CLICKHOUSE_PASSWORD, type="password")
        ch_database = st.text_input("Database", value=Config.CLICKHOUSE_DATABASE)

        # OpenAI configuration
        st.subheader("🤖 OpenAI")
        openai_api_key = st.text_input("API Key", value=Config.OPENAI_API_KEY or "", type="password")
        max_patterns = st.number_input("Max Patterns/Batch", min_value=1, max_value=20, value=5,
            help="Higher values use more tokens")
        max_tokens = st.number_input("Max Tokens/Request", min_value=1000, max_value=16000, value=8000, step=1000,
            help="Higher values allow more detailed analysis")
        
        return {
            'dbt_project_path': dbt_project_path,
            'ch_host': ch_host,
            'ch_port': ch_port,
            'ch_user': ch_user,
            'ch_password': ch_password,
            'ch_database': ch_database,
            'openai_api_key': openai_api_key,
            'max_patterns': max_patterns,
            'max_tokens': max_tokens,
            'is_valid_config': is_valid_config
        }

def main():
    """Main application function"""
    try:
        # Initialize session state
        init_session_state()
        
        # Set page config
        st.set_page_config(
            page_title="QuerySight - dbt Project Enhancer",
            page_icon="📊",
            layout="wide"
        )

        # Render header
        st.title("QuerySight: ClickHouse Log-Driven dbt Project Enhancer")
        st.markdown("""
        Analyze your ClickHouse query logs and get AI-powered suggestions for improving your dbt project.
        Follow these steps:
        1. Configure your settings in the sidebar
        2. Set up sampling parameters
        3. Analyze query patterns
        4. Generate AI suggestions
        """)

        # Get configuration from sidebar
        config = render_sidebar()
        
        # Main workflow tabs
        tab_setup, tab_analysis, tab_recommendations = st.tabs([
            "🎯 Setup",
            "📊 Analysis",
            "🤖 Recommendations"
        ])
        
        with tab_setup:
            # Only show sampling wizard if config is valid
            if config['is_valid_config']:
                wizard = SamplingWizard()
                config_result = wizard.render_wizard()
                
                # Update the current sampling config if we have a result
                if config_result is not None:
                    st.session_state.current_sampling_config = config_result
                    st.success("✅ Sampling configuration completed!")
            else:
                st.warning("⚠️ Please complete the configuration in the sidebar first")
        
        with tab_analysis:
            if not st.session_state.current_sampling_config:
                st.warning("⚠️ Please complete the sampling setup first")
                return
                
            # Analysis buttons in main area
            col1, col2 = st.columns(2)
            with col1:
                analyze_button = st.button("🔍 Analyze Query Patterns", use_container_width=True)
            with col2:
                suggest_button = st.button("💡 Generate Suggestions", use_container_width=True)
            
            if analyze_button or 'analysis_result' in st.session_state:
                with st.spinner("Analyzing query patterns..."):
                    if analyze_button or st.session_state.analysis_result is None:
                        # Get query logs
                        data_acquisition = ClickHouseDataAcquisition(
                            host=config['ch_host'],
                            port=config['ch_port'],
                            user=config['ch_user'],
                            password=config['ch_password'],
                            database=config['ch_database']
                        )
                        
                        logs_result = data_acquisition.get_query_logs(
                            start_date=st.session_state.current_sampling_config.start_date,
                            end_date=st.session_state.current_sampling_config.end_date,
                            sample_size=st.session_state.current_sampling_config.sample_size,
                            user_include=st.session_state.current_sampling_config.user_include,
                            user_exclude=st.session_state.current_sampling_config.user_exclude,
                            query_focus=st.session_state.current_sampling_config.query_focus,
                            query_types=st.session_state.current_sampling_config.query_types
                        )
                        
                        if logs_result['status'] == 'error':
                            st.error(f"Error retrieving logs: {logs_result['error']}")
                            return
                        
                        # Analyze patterns
                        patterns = data_acquisition.analyze_query_patterns(logs_result['data'])
                        
                        # Analyze dbt project
                        dbt_analyzer = DBTProjectAnalyzer(config['dbt_project_path'])
                        analysis_result = dbt_analyzer.analyze_project()
                        
                        # Update analysis result with patterns
                        analysis_result.query_patterns = patterns
                        analysis_result.calculate_coverage()
                        
                        # Store in session state
                        st.session_state.analysis_result = analysis_result
                    
                    # Render analysis dashboard
                    render_analysis_dashboard(st.session_state.analysis_result)
            
        with tab_recommendations:
            if not hasattr(st.session_state, 'analysis_result'):
                st.warning("⚠️ Please run the analysis first")
                return
                
            if suggest_button or 'recommendations' in st.session_state:
                with st.spinner("Generating AI recommendations..."):
                    if suggest_button or st.session_state.recommendations is None:
                        # Generate suggestions
                        ai_suggester = AISuggester(config['openai_api_key'])
                        recommendations = ai_suggester.generate_suggestions(
                            st.session_state.analysis_result
                        )
                        st.session_state.recommendations = recommendations
                    
                    # Render recommendations
                    render_recommendations(st.session_state.recommendations)
                    
    except Exception as e:
        st.error(f"An error occurred: {str(e)}")
        st.error(traceback.format_exc())

if __name__ == "__main__":
    main()