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

# Set up logger for this module
logger = setup_logger(__name__, log_level="DEBUG")

def init_session_state():
    """Initialize session state variables"""
    if 'analysis_results' not in st.session_state:
        st.session_state.analysis_results = None
    if 'query_patterns' not in st.session_state:
        st.session_state.query_patterns = None
    if 'processed_patterns' not in st.session_state:
        st.session_state.processed_patterns = set()
    if 'accumulated_suggestions' not in st.session_state:
        st.session_state.accumulated_suggestions = []
    if 'current_sampling_config' not in st.session_state:
        st.session_state.current_sampling_config = None

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
        tab_setup, tab_analysis = st.tabs(["🎯 Setup", "📊 Analysis"])
        
        with tab_setup:
            # Only show sampling wizard if config is valid
            if config['is_valid_config']:
                wizard = SamplingWizard()
                st.session_state.current_sampling_config = wizard.render_wizard()
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
                suggest_button = st.button(
                    "🤖 Generate AI Suggestions",
                    disabled=not st.session_state.query_patterns,
                    use_container_width=True
                )
            
            # Progress tracking
            progress_placeholder = st.empty()
            
            if analyze_button:
                if not all([config['ch_host'], config['ch_user'], config['ch_password'], config['ch_database']]):
                    st.error("❌ Please fill in all ClickHouse connection fields")
                    return
                    
                try:
                    with progress_placeholder.container():
                        st.markdown("### 📊 Analysis Progress")
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Initialize components
                        data_acquisition = ClickHouseDataAcquisition(
                            host=config['ch_host'],
                            port=config['ch_port'],
                            user=config['ch_user'],
                            password=config['ch_password'],
                            database=config['ch_database']
                        )
                        
                        sampling_config = st.session_state.current_sampling_config
                        
                        # Get and analyze query logs
                        query_result = data_acquisition.get_query_logs(
                            start_date=sampling_config.start_date,
                            end_date=sampling_config.end_date,
                            sample_size=sampling_config.sample_size,
                            user_include=sampling_config.user_include,
                            user_exclude=sampling_config.user_exclude,
                            query_focus=sampling_config.query_focus,
                            query_types=sampling_config.query_types
                        )
                        
                        while query_result['status'] == 'in_progress':
                            progress = query_result['loaded_rows'] / query_result['total_rows']
                            progress_bar.progress(progress)
                            status_text.text(
                                f"Loading... {query_result['loaded_rows']:,} / {query_result['total_rows']:,} rows"
                                f"{' (Sampling)' if sampling_config.sample_size < 1.0 else ''}"
                            )
                            
                            if query_result['data']:
                                current_patterns = data_acquisition.analyze_query_patterns(query_result['data'])
                                st.session_state.query_patterns = current_patterns
                            
                            query_result = data_acquisition.get_query_logs(
                                start_date=sampling_config.start_date,
                                end_date=sampling_config.end_date,
                                sample_size=sampling_config.sample_size,
                                user_include=sampling_config.user_include,
                                user_exclude=sampling_config.user_exclude,
                                query_focus=sampling_config.query_focus,
                                query_types=sampling_config.query_types
                            )
                        
                        # Final update
                        progress_bar.progress(1.0)
                        total_patterns = len(st.session_state.query_patterns) if st.session_state.query_patterns else 0
                        if total_patterns > 0:
                            st.success(f"✅ Found {total_patterns} distinct query patterns")
                        else:
                            st.error("❌ No query patterns found in the selected date range")
                
                except Exception as e:
                    st.error(f"❌ Analysis error: {str(e)}")
                    logger.error(f"Analysis error: {traceback.format_exc()}")
                    return
            
            if suggest_button:
                if not all([config['dbt_project_path'], config['openai_api_key']]):
                    st.error("❌ Please provide both dbt project path and OpenAI API key")
                    return
                
                try:
                    with progress_placeholder.container():
                        st.markdown("### 🤖 AI Analysis Progress")
                        progress_bar = st.progress(0)
                        status_text = st.empty()
                        
                        # Initialize components
                        dbt_analyzer = DBTProjectAnalyzer(config['dbt_project_path'])
                        ai_suggester = AISuggester(config['openai_api_key'])
                        
                        # Get unprocessed patterns
                        all_patterns = st.session_state.query_patterns
                        unprocessed_patterns = [
                            p for p in all_patterns 
                            if p['pattern'] not in st.session_state.processed_patterns
                        ]
                        
                        if unprocessed_patterns:
                            total_batches = (len(unprocessed_patterns) + config['max_patterns'] - 1) // config['max_patterns']
                            for batch in range(total_batches):
                                batch_patterns = unprocessed_patterns[batch * config['max_patterns']:(batch + 1) * config['max_patterns']]
                                progress = (batch + 1) / total_batches
                                progress_bar.progress(progress)
                                status_text.text(f"Analyzing batch {batch + 1} of {total_batches}...")
                                
                                suggestions = ai_suggester.generate_suggestions(
                                    query_patterns=batch_patterns,
                                    dbt_structure=dbt_analyzer.analyze_project(),
                                    max_patterns=config['max_patterns'],
                                    max_tokens=config['max_tokens']
                                )
                                
                                st.session_state.processed_patterns.update(p['pattern'] for p in batch_patterns)
                                st.session_state.accumulated_suggestions.extend(suggestions)
                            
                            st.session_state.analysis_results = st.session_state.accumulated_suggestions
                            st.success(f"✅ Generated {len(st.session_state.accumulated_suggestions)} suggestions")
                        else:
                            st.info("ℹ️ All patterns have been analyzed")
                
                except Exception as e:
                    st.error(f"❌ AI analysis error: {str(e)}")
                    logger.error(f"AI analysis error: {traceback.format_exc()}")
                    return
            
            # Display results
            if st.session_state.analysis_results:
                st.markdown("---")
                st.markdown("## 📈 Results")
                
                # Export button
                if st.session_state.query_patterns:
                    col1, col2 = st.columns([6, 2])
                    with col2:
                        if st.button("📊 Export PDF Report", use_container_width=True):
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
                                    mime="application/pdf",
                                    use_container_width=True
                                )
                            except Exception as e:
                                st.error(f"❌ PDF generation error: {str(e)}")
                
                # Query pattern analysis
                if st.session_state.query_patterns:
                    st.markdown("### 🔍 Query Patterns")
                    patterns_df = pd.DataFrame(st.session_state.query_patterns)
                    if not patterns_df.empty:
                        # Overview metrics
                        col1, col2, col3 = st.columns(3)
                        with col1:
                            st.metric("Total Patterns", len(patterns_df))
                        with col2:
                            st.metric("Total Executions", f"{patterns_df['frequency'].sum():,}")
                        with col3:
                            st.metric(
                                "Avg Duration",
                                f"{patterns_df['avg_duration_ms'].mean():.2f}ms"
                            )
                        
                        # Detailed analysis tabs
                        tab1, tab2 = st.tabs(["📊 Pattern Distribution", "⚡ Performance Analysis"])
                        
                        with tab1:
                            # Top patterns by frequency
                            total_queries = patterns_df['frequency'].sum()
                            top_patterns = patterns_df.nlargest(10, 'frequency')
                            top_patterns['percentage'] = (top_patterns['frequency'] / total_queries * 100).round(2)
                            
                            st.markdown("#### 📈 Top Query Patterns")
                            chart_data = top_patterns[['pattern', 'frequency']].set_index('pattern')
                            st.bar_chart(chart_data)
                            
                            st.markdown("#### 📋 Pattern Details")
                            detail_table = top_patterns[['pattern', 'frequency', 'percentage']]
                            detail_table.columns = ['Query Pattern', 'Frequency', 'Percentage (%)']
                            st.dataframe(detail_table, hide_index=True, use_container_width=True)
                        
                        with tab2:
                            # Performance analysis
                            st.markdown("#### ⏱️ Time-Consuming Queries")
                            perf_data = patterns_df.nlargest(10, 'avg_duration_ms')
                            perf_chart = perf_data[['pattern', 'avg_duration_ms']].set_index('pattern')
                            perf_chart = perf_chart / 1000  # Convert to seconds
                            perf_chart.columns = ['Average Duration (seconds)']
                            st.bar_chart(perf_chart)
                            
                            st.markdown("#### 📋 Performance Details")
                            perf_table = patterns_df.nlargest(5, 'avg_duration_ms')[
                                ['pattern', 'avg_duration_ms', 'frequency', 'avg_read_rows']
                            ]
                            perf_table.columns = ['Query Pattern', 'Avg Duration (ms)', 'Frequency', 'Avg Rows Read']
                            st.dataframe(perf_table, hide_index=True, use_container_width=True)
                
                # AI suggestions
                if st.session_state.analysis_results:
                    st.markdown("### 🤖 AI Suggestions")
                    categories = set(sugg['category'] for sugg in st.session_state.analysis_results)
                    tabs = st.tabs([f"📌 {category}" for category in categories])
                    
                    for tab, category in zip(tabs, categories):
                        with tab:
                            category_suggestions = [s for s in st.session_state.analysis_results if s['category'] == category]
                            
                            for idx, suggestion in enumerate(category_suggestions, 1):
                                with st.container():
                                    # Header with impact
                                    col1, col2 = st.columns([8, 2])
                                    with col1:
                                        st.markdown(f"#### {idx}. {suggestion['title']}")
                                    with col2:
                                        impact_colors = {
                                            'HIGH': '🔴',
                                            'MEDIUM': '🟡',
                                            'LOW': '🟢'
                                        }
                                        st.markdown(f"**Impact:** {impact_colors[suggestion['impact_level']]} {suggestion['impact_level'].title()}")
                                    
                                    # Problem description
                                    if 'problem_description' in suggestion:
                                        st.markdown(f"**🔍 Issue:**")
                                        st.markdown(suggestion['problem_description'])
                                    
                                    # Optimization details
                                    if 'optimization_details' in suggestion:
                                        with st.expander("📊 Details", expanded=False):
                                            details = suggestion['optimization_details']
                                            
                                            if 'benefits' in details:
                                                st.markdown("**✨ Benefits:**")
                                                for benefit in details['benefits']:
                                                    st.markdown(f"✅ {benefit}")
                                            
                                            if 'potential_risks' in details:
                                                st.markdown("**⚠️ Considerations:**")
                                                for risk in details['potential_risks']:
                                                    st.markdown(f"- {risk}")
                                            
                                            if 'estimated_improvement' in details:
                                                st.info(f"**📈 Estimated Improvement:** {details['estimated_improvement']}")
                                    
                                    # Code comparison
                                    with st.expander("💻 Code", expanded=False):
                                        col1, col2 = st.columns(2)
                                        with col1:
                                            st.markdown("**Current:**")
                                            st.code(suggestion['current_pattern'], language='sql')
                                        with col2:
                                            st.markdown("**Optimized:**")
                                            st.code(suggestion['optimized_pattern'], language='sql')
                                    
                                    # Implementation steps
                                    with st.expander("📝 Implementation", expanded=False):
                                        for step_num, step in enumerate(suggestion['implementation_steps'], 1):
                                            st.markdown(f"{step_num}. {step}")
                                        
                                        if suggestion.get('code_example'):
                                            st.markdown("**Complete Example:**")
                                            st.code(suggestion['code_example'], language='sql')
                                    
                                    st.divider()

    except Exception as e:
        st.error(f"❌ Application error: {str(e)}")
        logger.error(f"Application error: {traceback.format_exc()}")

if __name__ == "__main__":
    main()