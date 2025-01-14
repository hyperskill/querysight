from dataclasses import dataclass
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
import streamlit as st

@dataclass
class SamplingConfig:
    sample_size: float
    start_date: datetime
    end_date: datetime
    user_include: Optional[List[str]]
    user_exclude: Optional[List[str]]
    db_include: Optional[List[str]]
    db_exclude: Optional[List[str]]
    query_focus: List[str]
    query_types: List[str]

class SamplingWizard:
    def __init__(self):
        if 'wizard_step' not in st.session_state:
            st.session_state.wizard_step = 1
        if 'sampling_config' not in st.session_state:
            st.session_state.sampling_config = None
        self.total_steps = 6  # Increased from 5 to 6

    def _reset_wizard(self):
        st.session_state.wizard_step = 1
        st.session_state.sampling_config = None

    def _next_step(self):
        st.session_state.wizard_step += 1

    def _prev_step(self):
        st.session_state.wizard_step = max(1, st.session_state.wizard_step - 1)

    def render_wizard(self) -> Optional[SamplingConfig]:
        """Render the sampling configuration wizard"""
        st.markdown("### 📊 Sampling Configuration Wizard")
        
        # Progress bar
        st.progress(st.session_state.wizard_step / self.total_steps)

        if st.session_state.wizard_step == 1:
            self._render_step_1()  # Time Window
        elif st.session_state.wizard_step == 2:
            self._render_step_2()  # Sample Size
        elif st.session_state.wizard_step == 3:
            self._render_step_3()  # Query Types
        elif st.session_state.wizard_step == 4:
            self._render_step_4()  # User Filtering
        elif st.session_state.wizard_step == 5:
            self._render_step_5()  # Database Filtering
        elif st.session_state.wizard_step == 6:
            return self._render_final_step()  # Performance Focus
        return None

    def _render_step_1(self) -> None:
        """Time Window Selection"""
        st.markdown("#### Step 1: Select Time Window 📅")
        st.markdown("""
        Choose the time period for your analysis. Consider:
        - Recent data for current performance
        - Longer periods for trend analysis
        - Specific timeframes for incident investigation
        """)

        col1, col2 = st.columns(2)
        with col1:
            start_date = st.date_input(
                "Start Date",
                value=datetime.now() - timedelta(days=7),
                help="Beginning of the analysis period"
            )
        with col2:
            end_date = st.date_input(
                "End Date",
                value=datetime.now(),
                help="End of the analysis period"
            )

        if end_date < start_date:
            st.error("End date must be after start date")
            return None

        if st.button("Next →"):
            st.session_state.start_date = start_date
            st.session_state.end_date = end_date
            self._next_step()

    def _render_step_2(self) -> None:
        """Sample Size Configuration"""
        st.markdown("#### Step 2: Configure Sample Size 📈")
        st.markdown("""
        Adjust the sampling rate based on your needs:
        - Larger samples: More accurate but slower
        - Smaller samples: Faster but less precise
        """)

        # Date range affects recommended sample size
        date_range = (st.session_state.end_date - st.session_state.start_date).days
        
        if date_range > 30:
            recommended_sample = min(10.0, 300 / date_range)
            st.info(f"📌 For your {date_range} day range, we recommend {recommended_sample:.1f}% sampling")
        else:
            recommended_sample = 25.0
            st.info("📌 For short time ranges, we recommend 25% sampling")

        sample_size = st.slider(
            "Sample Size (%)",
            min_value=0.1,
            max_value=100.0,
            value=float(recommended_sample),
            step=0.1,
            help="Percentage of data to analyze"
        )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                self._prev_step()
        with col2:
            if st.button("Next →"):
                st.session_state.sample_size = sample_size
                self._next_step()

    def _render_step_3(self) -> None:
        """Query Type Selection"""
        st.markdown("#### Step 3: Select Query Types 🔍")
        st.markdown("""
        Filter queries by type to focus your analysis:
        - SELECT: Read operations
        - INSERT/UPDATE: Write operations
        - DDL: Schema changes
        """)

        query_types = st.multiselect(
            "Query Types to Include",
            options=["SELECT", "INSERT", "CREATE", "ALTER", "DROP", "All"],
            default=["All"],
            help="Choose specific query types to analyze"
        )

        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                self._prev_step()
        with col2:
            if st.button("Next →"):
                st.session_state.query_types = query_types
                self._next_step()

    def _render_step_4(self) -> None:
        """User Filtering"""
        st.markdown("#### Step 4: Configure User Filtering 👥")
        st.markdown("""
        Specify users to include or exclude:
        - Include specific users for targeted analysis
        - Exclude system or automated users
        - One username per line
        - Prefix with '-' to exclude
        """)

        user_filter = st.text_area(
            "User Filter",
            placeholder="user1\nuser2\n-user3",
            help="Enter usernames (prefix with - to exclude)"
        )

        # Process user input
        users = [u.strip() for u in user_filter.split('\n') if u.strip()]
        include_users = [u for u in users if not u.startswith('-')]
        exclude_users = [u[1:] for u in users if u.startswith('-')]

        # Show preview
        col1, col2 = st.columns(2)
        with col1:
            if include_users:
                st.markdown("**Including:**")
                for user in include_users:
                    st.markdown(f"- {user}")
        with col2:
            if exclude_users:
                st.markdown("**Excluding:**")
                for user in exclude_users:
                    st.markdown(f"- {user}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                self._prev_step()
        with col2:
            if st.button("Next →"):
                st.session_state.user_include = include_users if include_users else None
                st.session_state.user_exclude = exclude_users if exclude_users else None
                self._next_step()

    def _render_step_5(self) -> None:
        """Database Filtering Step"""
        st.markdown("#### Step 5: Database Selection 🗄️")
        st.markdown("""
        Filter queries by database:
        - Select specific databases to analyze
        - Leave empty to analyze all databases
        - Useful for focusing on specific data domains
        """)

        database_filter = st.text_area(
            "Database Filter",
            placeholder="database1\ndatabase2\n-database3",
            help="Enter database names (prefix with - to exclude)"
        )

        # Process database input
        databases = [d.strip() for d in database_filter.split('\n') if d.strip()]
        include_dbs = [d for d in databases if not d.startswith('-')]
        exclude_dbs = [d[1:] for d in databases if d.startswith('-')]

        # Show preview
        col1, col2 = st.columns(2)
        with col1:
            if include_dbs:
                st.markdown("**Including Databases:**")
                for db in include_dbs:
                    st.markdown(f"- {db}")
        with col2:
            if exclude_dbs:
                st.markdown("**Excluding Databases:**")
                for db in exclude_dbs:
                    st.markdown(f"- {db}")

        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                self._prev_step()
        with col2:
            if st.button("Next →"):
                st.session_state.db_include = include_dbs if include_dbs else None
                st.session_state.db_exclude = exclude_dbs if exclude_dbs else None
                self._next_step()

    def _render_final_step(self) -> Optional[SamplingConfig]:
        """Final Step (Performance Focus)"""
        st.markdown("#### Step 6: Set Performance Focus 🎯")
        st.markdown("""
        Choose analysis focus:
        - All Queries: Complete analysis
        - Slow Queries: Focus on performance issues
        - Frequent Queries: Focus on high-impact patterns
        """)

        query_focus = st.multiselect(
            "Analysis Focus",
            options=["ALL", "SLOW", "FREQUENT"],
            default=["ALL"],
            help="Choose which types of queries to focus on"
        )

        # Show summary of all configurations
        st.markdown("### Configuration Summary")
        st.json({
            'Time Window': {
                'Start': st.session_state.start_date.strftime('%Y-%m-%d'),
                'End': st.session_state.end_date.strftime('%Y-%m-%d')
            },
            'Sample Size': f"{st.session_state.sample_size}%",
            'Query Types': st.session_state.query_types,
            'Users': {
                'Include': st.session_state.user_include,
                'Exclude': st.session_state.user_exclude
            },
            'Databases': {
                'Include': st.session_state.db_include,
                'Exclude': st.session_state.db_exclude
            },
            'Focus': query_focus
        })

        col1, col2 = st.columns(2)
        with col1:
            if st.button("← Back"):
                self._prev_step()
        with col2:
            if st.button("Start Analysis"):
                return SamplingConfig(
                    sample_size=st.session_state.sample_size,
                    start_date=st.session_state.start_date,
                    end_date=st.session_state.end_date,
                    user_include=st.session_state.user_include,
                    user_exclude=st.session_state.user_exclude,
                    db_include=st.session_state.db_include,
                    db_exclude=st.session_state.db_exclude,
                    query_focus=query_focus,
                    query_types=st.session_state.query_types
                )
        return None