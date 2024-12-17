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

# Set up logger for this module
logger = setup_logger(__name__, log_level="DEBUG")

# Log startup information
logger.info("QuerySight application starting up...")

def validate_date(date_str):
    try:
        return datetime.strptime(date_str, '%Y-%m-%d')
    except ValueError:
        return None

def init_session_state():
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'query_patterns' not in st.session_state:
        st.session_state.query_patterns = None

def main():
    logger.info("Starting Streamlit application main function")
    
    # Initialize session state first
    logger.debug("Initializing session state")
    init_session_state()
    
    try:
        logger.debug("Setting page configuration")
        st.set_page_config(
            page_title="QuerySight - dbt Project Enhancer",
            page_icon="📊",
            layout="wide"
        )
        logger.info("Page configuration set successfully")
    except Exception as e:
        logger.error(f"Error in set_page_config: {str(e)}")
        logger.error(f"Traceback: {traceback.format_exc()}")
        st.error(f"Error initializing application: {str(e)}")
        return

    st.title("QuerySight: ClickHouse Log-Driven dbt Project Enhancer")
    st.markdown("""
    Analyze your ClickHouse query logs and get AI-powered suggestions for improving your dbt project.
    """)
    
    # Intelligent Sampling Configuration
    from utils.sampling_wizard import SamplingWizard
    
    wizard = SamplingWizard()
    sampling_config = wizard.render_wizard()
    
    # If wizard completed, store config in session state
    if sampling_config is not None:
        st.session_state.current_sampling_config = sampling_config

    print("Configuring sidebar...")  # Debug log
    try:
        # Sidebar configuration
        with st.sidebar:
            st.header("Configuration")
            
            dbt_project_path = st.text_input(
                "dbt Project Path",
                value=Config.DBT_PROJECT_PATH,
                placeholder="/path/to/dbt/project",
                help="Path to your dbt project directory. This can also be set in the .env file."
            )
            
            if dbt_project_path != Config.DBT_PROJECT_PATH:
                st.info("Note: Changes to the project path will only persist for the current session. To make it permanent, update your .env file.")

            col1, col2 = st.columns(2)
            with col1:
                start_date = st.date_input(
                    "Start Date",
                    value=datetime.now() - timedelta(days=30)
                )
            with col2:
                end_date = st.date_input(
                    "End Date",
                    value=datetime.now()
                )

            # ClickHouse credentials
            st.subheader("ClickHouse Credentials")
            ch_host = st.text_input("Host", value=Config.CLICKHOUSE_HOST)
            ch_port = st.number_input("Port", value=Config.CLICKHOUSE_PORT, min_value=1, max_value=65535)
            ch_user = st.text_input("Username", value=Config.CLICKHOUSE_USER)
            ch_password = st.text_input("Password", value=Config.CLICKHOUSE_PASSWORD, type="password")
            ch_database = st.text_input("Database", value=Config.CLICKHOUSE_DATABASE)

            # OpenAI Configuration
            st.subheader("OpenAI Configuration")
            col1, col2 = st.columns(2)
            with col1:
                openai_api_key = st.text_input("OpenAI API Key", value=Config.OPENAI_API_KEY or "", type="password")
            with col2:
                max_patterns = st.number_input("Max Patterns per Analysis", min_value=1, max_value=20, value=5,
                    help="Maximum number of query patterns to analyze in each batch. Higher values use more tokens.")
            
            col3, col4 = st.columns(2)
            with col3:
                max_tokens = st.number_input("Max Tokens per Request", min_value=1000, max_value=16000, value=8000, step=1000,
                    help="Maximum tokens to use per API request. Higher values allow more detailed analysis but cost more.")
            with col4:
                auto_refresh = st.checkbox("Auto-refresh Analysis", value=False,
                    help="Automatically refresh analysis when new data is available")
            
            # Configuration status
            st.subheader("Configuration Status")
            is_valid_config, missing_vars = Config.validate_config()
            if not is_valid_config:
                st.warning("⚠️ Missing required configuration variables in .env file:")
                for var in missing_vars:
                    st.markdown(f"- {var}")
                st.info("Create a .env file based on .env.template to set these variables.")

            # Separate buttons for analysis and AI suggestions
            col1, col2 = st.columns(2)
            with col1:
                analyze_button = st.button("📊 Analyze Query Patterns")
            with col2:
                suggest_button = st.button("🤖 Generate AI Suggestions", 
                    disabled='query_patterns' not in st.session_state or not st.session_state.query_patterns,
                    help="First analyze query patterns before generating AI suggestions")

            # Saved Proposals Management
            st.subheader("💡 Saved Proposals")
            if 'saved_proposals' not in st.session_state:
                st.session_state.saved_proposals = []
            
            if st.session_state.query_patterns is not None:
                st.write("Select Query for Analysis:")
                selected_query = st.selectbox(
                    "Choose a query pattern to analyze:",
                    options=[p['pattern'] for p in st.session_state.query_patterns] if st.session_state.query_patterns else [],
                    format_func=lambda x: x[:100] + "..." if len(x) > 100 else x
                )
                
                if st.button("🤖 Generate New Proposal"):
                    if not dbt_project_path:
                        st.warning("Please provide the dbt project path first.")
                    elif selected_query and openai_api_key:
                        try:
                            # Create containers for progress updates
                            ai_status = st.empty()
                            ai_progress = st.empty()
                            
                            with ai_status:
                                st.info("🤖 Initializing AI analysis...")
                            
                            # Analysis steps with visual feedback
                            with ai_progress:
                                with st.spinner("📊 Analyzing dbt project structure..."):
                                    dbt_analyzer = DBTProjectAnalyzer(dbt_project_path)
                                    project_structure = dbt_analyzer.analyze_project()
                                
                                with st.spinner("🧠 Initializing AI engine..."):
                                    ai_suggester = AISuggester(openai_api_key)
                                
                                with st.spinner("🔍 Finding relevant query patterns..."):
                                    selected_pattern = next(
                                        (p for p in st.session_state.query_patterns if p['pattern'] == selected_query),
                                        None
                                    )
                                if selected_pattern:
                                    suggestions = ai_suggester.generate_suggestions(
                                        query_patterns=[selected_pattern],
                                        dbt_structure=dbt_analyzer.analyze_project()
                                    )
                                    if suggestions and len(suggestions) > 0:
                                        new_suggestion = suggestions[0]
                                        new_suggestion['timestamp'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                                        st.session_state.saved_proposals.append(new_suggestion)
                                        st.success("New proposal generated and saved!")
                                    else:
                                        st.warning("No suggestions were generated. Please try again.")
                        except Exception as e:
                            st.error(f"Error generating proposal: {str(e)}")
                    else:
                        st.warning("Please provide OpenAI API key and select a query pattern.")

            if st.session_state.saved_proposals:
                for idx, proposal in enumerate(st.session_state.saved_proposals):
                    with st.expander(f"📝 Proposal {idx + 1} - {proposal['timestamp']}"):
                        st.markdown(f"**Title:** {proposal['title']}")
                        st.markdown(f"**Impact:** {proposal['impact_level']}")
                        if st.button("🗑️ Delete", key=f"delete_{idx}"):
                            st.session_state.saved_proposals.pop(idx)
                            st.rerun()

    except Exception as e:
        print(f"Error in sidebar configuration: {str(e)}")  # Debug log
        st.error("Failed to load sidebar configuration")
        return

    if analyze_button:
        logger.info("Analysis button clicked - starting query pattern analysis")
        if not all([ch_host, ch_user, ch_password, ch_database]):
            logger.warning("Missing required ClickHouse credentials")
            st.error("Please fill in all ClickHouse connection fields")
            return

        try:
            with st.spinner("Analyzing query logs..."):
                logger.info("Initializing analysis components")
                # Initialize ClickHouse connection
                logger.debug(f"Initializing ClickHouse connection to {ch_host}:{ch_port}")
                # Analyze dbt project structure
                logger.info("Starting dbt project analysis")
                dbt_structure = dbt_analyzer.analyze_project()
                logger.info("DBT project analysis completed")

                # Initialize or reset AI analysis progress tracking
                if 'processed_patterns' not in st.session_state:
                    st.session_state.processed_patterns = set()
                if 'accumulated_suggestions' not in st.session_state:
                    st.session_state.accumulated_suggestions = []

                # Get unprocessed patterns
                all_patterns = st.session_state.query_patterns
                unprocessed_patterns = [
                    p for p in all_patterns 
                    if p['pattern'] not in st.session_state.processed_patterns
                ]
                
                if unprocessed_patterns:
                    with st.spinner(f"Analyzing patterns ({len(st.session_state.processed_patterns)} / {len(all_patterns)})..."):
                        suggestions = ai_suggester.generate_suggestions(
                            query_patterns=unprocessed_patterns,
                            dbt_structure=dbt_structure,
                            max_patterns=max_patterns,
                            max_tokens=max_tokens
                        )
                        
                        # Update processed patterns
                        st.session_state.processed_patterns.update(
                            p['pattern'] for p in unprocessed_patterns[:max_patterns]
                        )
                        
                        # Accumulate suggestions
                        st.session_state.accumulated_suggestions.extend(suggestions)
                        logger.info(f"Generated {len(suggestions)} new suggestions. Total: {len(st.session_state.accumulated_suggestions)}")
                
                st.session_state.analysis_results = st.session_state.accumulated_suggestions
                st.success(f"AI analysis complete! Generated {len(st.session_state.accumulated_suggestions)} suggestions")
                data_acquisition = ClickHouseDataAcquisition(
                    host=ch_host,
                    port=ch_port,
                    user=ch_user,
                    password=ch_password,
                    database=ch_database
                )
                

                # Initialize progress containers
                progress_container = st.empty()
                status_container = st.empty()
                pattern_container = st.empty()
            
                with progress_container:
                    progress_bar = st.progress(0)
            
                with status_container:
                    status_text = st.text("Initializing data loading...")
            
                # Calculate optimal sample size based on date range
                date_range = (end_date - start_date).days
                sample_size = min(1.0, max(0.1, 1000 / (date_range * 1000)))
                
                logger.info(f"Retrieving query logs from {start_date} to {end_date} with {sample_size:.1%} sampling")
                
                if not hasattr(st.session_state, 'current_sampling_config'):
                    st.error("Please complete the sampling configuration wizard first")
                    return
                
                config = st.session_state.current_sampling_config
                
                # Initialize progress tracking
                progress_container = st.empty()
                status_container = st.empty()
                pattern_container = st.empty()
                
                with progress_container:
                    progress_bar = st.progress(0)
                with status_container:
                    status_text = st.text("Loading query data...")
                
                query_result = data_acquisition.get_query_logs(
                    start_date=config.start_date,
                    end_date=config.end_date,
                    sample_size=config.sample_size,
                    user_include=config.user_include,
                    user_exclude=config.user_exclude,
                    query_focus=config.query_focus,
                    query_types=config.query_types
                )
                
                while query_result['status'] == 'in_progress':
                    progress = query_result['loaded_rows'] / query_result['total_rows']
                    progress_bar.progress(progress)
                    sample_info = f" (Sampling at {sample_size:.1%})" if sample_size < 1.0 else ""
                    status_text.text(
                        f"Loading data... ({query_result['loaded_rows']:,} / "
                        f"{query_result['total_rows']:,} rows){sample_info}"
                    )
                    
                    if query_result['data']:
                        logger.info("Analyzing current batch of query patterns")
                        with pattern_container:
                            current_patterns = data_acquisition.analyze_query_patterns(query_result['data'])
                            st.session_state.query_patterns = current_patterns
                            st.info(f"Found {len(current_patterns)} distinct query patterns...")
                    
                    query_result = data_acquisition.get_query_logs(start_date, end_date)
                
                # Final update
                progress_bar.progress(1.0)
                total_patterns = len(st.session_state.query_patterns) if st.session_state.query_patterns else 0
                status_text.text(f"Analysis complete! Found {total_patterns} distinct query patterns")
                logger.info(f"Query pattern analysis completed with {total_patterns} patterns")
                
                # Clear temporary containers
                progress_container.empty()
                status_container.empty()
                pattern_container.empty()
                
                if not st.session_state.query_patterns:
                    st.error("No query patterns found in the selected date range")
                    return
                
                st.success(f"Successfully analyzed {total_patterns} query patterns")

        except Exception as e:
            st.error(f"An error occurred during analysis: {str(e)}")
            return

    if suggest_button:
        logger.info("Suggest button clicked - starting AI analysis")
        if not all([dbt_project_path, openai_api_key]):
            logger.warning("Missing required fields for AI analysis")
            st.error("Please provide both dbt project path and OpenAI API key")
            return
            
        try:
            with st.spinner("Initializing AI analysis..."):
                logger.debug(f"Initializing DBT analyzer for project: {dbt_project_path}")
                dbt_analyzer = DBTProjectAnalyzer(dbt_project_path)
                logger.debug("Initializing AI suggester")
                ai_suggester = AISuggester(openai_api_key)

                # Initialize progress tracking for AI analysis
                if 'processed_patterns' not in st.session_state:
                    st.session_state.processed_patterns = set()
                if 'accumulated_suggestions' not in st.session_state:
                    st.session_state.accumulated_suggestions = []
                
                # Get unprocessed patterns
                all_patterns = st.session_state.query_patterns
                unprocessed_patterns = [
                    p for p in all_patterns 
                    if p['pattern'] not in st.session_state.processed_patterns
                ]
                
                if unprocessed_patterns:
                    with st.spinner(f"Analyzing patterns ({len(st.session_state.processed_patterns)} / {len(all_patterns)})..."):
                        suggestions = ai_suggester.generate_suggestions(
                            query_patterns=unprocessed_patterns,
                            dbt_structure=dbt_analyzer.analyze_project(),
                            max_patterns=max_patterns,
                            max_tokens=max_tokens
                        )
                        
                        # Update processed patterns
                        st.session_state.processed_patterns.update(
                            p['pattern'] for p in unprocessed_patterns[:max_patterns]
                        )
                        
                        # Accumulate suggestions
                        st.session_state.accumulated_suggestions.extend(suggestions)
                        logger.info(f"Generated {len(suggestions)} new suggestions. Total: {len(st.session_state.accumulated_suggestions)}")
                
                st.session_state.analysis_results = st.session_state.accumulated_suggestions
                logger.info("AI suggestion process completed successfully")

        except Exception as e:
            st.error(f"An error occurred during AI suggestion generation: {str(e)}")
            return

    # Display results
    if st.session_state.analysis_results is not None:
        st.header("Analysis Results")
        
        # Add PDF Export Button
        if st.session_state.analysis_results and st.session_state.query_patterns:
            col1, col2 = st.columns([6, 2])
            with col2:
                if st.button("📊 Export PDF Report"):
                    try:
                        pdf_generator = PDFReportGenerator()
                        pdf_bytes = pdf_generator.generate_report(
                            query_patterns=st.session_state.query_patterns,
                            suggestions=st.session_state.analysis_results
                        )
                        st.download_button(
                            label="📥 Download Report",
                            data=pdf_bytes,
                            file_name=f"querysight_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.pdf",
                            mime="application/pdf"
                        )
                    except Exception as e:
                        st.error(f"Error generating PDF report: {str(e)}")
        
        # Query Patterns
        st.subheader("Query Patterns")
        if st.session_state.query_patterns:
            try:
                patterns_df = pd.DataFrame(st.session_state.query_patterns)
                if not patterns_df.empty:
                    st.dataframe(patterns_df)
                    
                    # Query Pattern Visualizations
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.subheader("Query Frequency Distribution")
                        if 'pattern' in patterns_df.columns and 'frequency' in patterns_df.columns:
                            # Calculate percentages
                            total_queries = patterns_df['frequency'].sum()
                            frequency_data = patterns_df.nlargest(10, 'frequency')
                            frequency_data['percentage'] = (frequency_data['frequency'] / total_queries * 100).round(2)
                            
                            # Create visualization data
                            chart_data = frequency_data[['pattern', 'frequency']].set_index('pattern')
                            st.bar_chart(chart_data, use_container_width=True)
                            
                            # Show detailed frequency table
                            st.markdown("### Top Query Patterns")
                            detail_table = frequency_data[['pattern', 'frequency', 'percentage']]
                            detail_table.columns = ['Query Pattern', 'Frequency', 'Percentage (%)']
                            st.dataframe(
                                detail_table,
                                hide_index=True,
                                use_container_width=True
                            )
                            
                            # Summary statistics
                            st.markdown("### Query Distribution Summary")
                            st.markdown(f"- Total Unique Patterns: **{len(patterns_df)}**")
                            st.markdown(f"- Total Query Executions: **{total_queries:,}**")
                            st.markdown(f"- Top 10 Patterns: **{frequency_data['percentage'].sum():.2f}%** of all queries")
                    
                    with col2:
                        st.subheader("Top Time-Consuming Queries")
                        if 'pattern' in patterns_df.columns and 'avg_duration_ms' in patterns_df.columns:
                            # Sort by duration and get top 10
                            performance_data = patterns_df.nlargest(10, 'avg_duration_ms')
                            performance_data = performance_data[['pattern', 'avg_duration_ms']].set_index('pattern')
                            # Convert to seconds for better readability
                            performance_data = performance_data / 1000
                            performance_data.columns = ['Average Duration (seconds)']
                            st.bar_chart(performance_data, use_container_width=True)
                            
                            # Detailed performance table
                            st.subheader("Query Performance Details")
                            detail_data = patterns_df.nlargest(5, 'avg_duration_ms')[
                                ['pattern', 'avg_duration_ms', 'frequency', 'avg_read_rows']
                            ]
                            detail_data['avg_duration_ms'] = detail_data['avg_duration_ms'].round(2)
                            detail_data.columns = ['Query Pattern', 'Avg Duration (ms)', 'Frequency', 'Avg Rows Read']
                            st.dataframe(detail_data, hide_index=True)
                else:
                    st.info("No query patterns found in the specified date range.")
            except Exception as e:
                st.error(f"Error displaying query patterns: {str(e)}")
        else:
            st.info("No query patterns available. Please analyze your queries first.")

        # Improvement Suggestions
        st.subheader("Query Optimization Suggestions")
        
        # Group suggestions by category
        if st.session_state.analysis_results:
            categories = set(sugg['category'] for sugg in st.session_state.analysis_results)
            
            # Create tabs for different categories
            tabs = st.tabs(list(categories))
            for tab, category in zip(tabs, categories):
                with tab:
                    category_suggestions = [s for s in st.session_state.analysis_results if s['category'] == category]
                    
                    for idx, suggestion in enumerate(category_suggestions, 1):
                        # Create a colorful card-like container for each suggestion
                        with st.container():
                            # Header with impact level indicator
                            col1, col2 = st.columns([8, 2])
                            with col1:
                                st.markdown(f"### {idx}. {suggestion['title']}")
                            with col2:
                                impact_colors = {
                                    'HIGH': '🔴 High',
                                    'MEDIUM': '🟡 Medium',
                                    'LOW': '🟢 Low'
                                }
                                st.markdown(f"**Impact:** {impact_colors[suggestion['impact_level']]}")
                            
                            # Problem and Benefits
                            if 'problem_description' in suggestion:
                                st.markdown(f"**Current Issue:**")
                                st.markdown(suggestion['problem_description'])
                            
                            # Optimization Details
                            if 'optimization_details' in suggestion:
                                with st.expander("📊 Optimization Details", expanded=False):
                                    details = suggestion['optimization_details']
                                    
                                    if 'benefits' in details:
                                        st.markdown("**Expected Benefits:**")
                                        for benefit in details['benefits']:
                                            st.markdown(f"✅ {benefit}")
                                    
                                    if 'potential_risks' in details:
                                        st.markdown("**Considerations:**")
                                        for risk in details['potential_risks']:
                                            st.markdown(f"⚠️ {risk}")
                                    
                                    if 'estimated_improvement' in details:
                                        st.info(f"**Estimated Improvement:** {details['estimated_improvement']}")
                            
                            # Code Comparison
                            with st.expander("💻 Code Comparison", expanded=False):
                                col1, col2 = st.columns(2)
                                with col1:
                                    st.markdown("**Current Pattern:**")
                                    st.code(suggestion['current_pattern'], language='sql')
                                with col2:
                                    st.markdown("**Optimized Pattern:**")
                                    st.code(suggestion['optimized_pattern'], language='sql')
                            
                            # Implementation Steps
                            with st.expander("📝 Implementation Guide", expanded=False):
                                for step_num, step in enumerate(suggestion['implementation_steps'], 1):
                                    st.markdown(f"{step_num}. {step}")
                                
                                if suggestion.get('code_example'):
                                    st.markdown("**Complete Example:**")
                                    st.code(suggestion['code_example'], language='sql')
                            
                            st.divider()

if __name__ == "__main__":
    main()