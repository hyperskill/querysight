import pytest
from datetime import datetime, timedelta
from unittest.mock import MagicMock, patch
from utils.sampling_wizard import SamplingWizard, SamplingConfig

class MockSessionState(dict):
    """Mock Streamlit's SessionState"""
    def __setattr__(self, key, value):
        self[key] = value
    
    def __getattr__(self, key):
        if key not in self:
            return None
        return self[key]

@pytest.fixture
def mock_streamlit():
    """Mock streamlit session state and UI components"""
    with patch('utils.sampling_wizard.st') as mock_st:
        # Mock session state
        mock_st.session_state = MockSessionState()
        
        # Mock UI components
        mock_st.markdown = MagicMock()
        mock_st.progress = MagicMock()
        mock_st.date_input = MagicMock()
        mock_st.slider = MagicMock()
        mock_st.multiselect = MagicMock()
        mock_st.text_area = MagicMock()
        mock_st.button = MagicMock()
        
        # Mock columns to return different numbers based on call
        def mock_columns(n):
            return [MagicMock() for _ in range(n)]
        mock_st.columns = MagicMock(side_effect=mock_columns)
        
        mock_st.error = MagicMock()
        mock_st.info = MagicMock()
        
        yield mock_st

@pytest.fixture
def wizard(mock_streamlit):
    """Create a SamplingWizard instance with mocked streamlit"""
    return SamplingWizard()

def test_wizard_initialization(wizard, mock_streamlit):
    """Test wizard initialization"""
    assert mock_streamlit.session_state.wizard_step == 1
    assert mock_streamlit.session_state.sampling_config is None

def test_wizard_navigation(wizard, mock_streamlit):
    """Test wizard navigation methods"""
    # Test next step
    wizard._next_step()
    assert mock_streamlit.session_state.wizard_step == 2
    
    # Test prev step
    wizard._prev_step()
    assert mock_streamlit.session_state.wizard_step == 1
    
    # Test prev step at beginning
    wizard._prev_step()
    assert mock_streamlit.session_state.wizard_step == 1
    
    # Test reset
    mock_streamlit.session_state.wizard_step = 3
    wizard._reset_wizard()
    assert mock_streamlit.session_state.wizard_step == 1
    assert mock_streamlit.session_state.sampling_config is None

def test_step1_time_window(wizard, mock_streamlit):
    """Test time window selection step"""
    # Setup session state
    mock_streamlit.session_state.wizard_step = 1
    
    # Mock date inputs
    start_date = datetime.now().date() - timedelta(days=7)
    end_date = datetime.now().date()
    mock_streamlit.date_input.side_effect = [start_date, end_date]
    
    # Mock next button click
    mock_streamlit.button.return_value = True
    
    wizard._render_step_1()
    
    # Verify dates were stored
    assert mock_streamlit.session_state.start_date == start_date
    assert mock_streamlit.session_state.end_date == end_date
    assert mock_streamlit.session_state.wizard_step == 2

def test_step1_invalid_dates(wizard, mock_streamlit):
    """Test time window validation"""
    # Setup session state
    mock_streamlit.session_state.wizard_step = 1
    
    # Mock invalid dates (end before start)
    start_date = datetime.now().date()
    end_date = start_date - timedelta(days=1)
    mock_streamlit.date_input.side_effect = [start_date, end_date]
    
    wizard._render_step_1()
    
    # Verify error was shown
    mock_streamlit.error.assert_called_once_with("End date must be after start date")

def test_step2_sample_size(wizard, mock_streamlit):
    """Test sample size configuration step"""
    # Setup session state
    mock_streamlit.session_state.wizard_step = 2
    mock_streamlit.session_state.start_date = datetime.now().date() - timedelta(days=7)
    mock_streamlit.session_state.end_date = datetime.now().date()
    
    # Mock slider and button
    mock_streamlit.slider.return_value = 25.0
    mock_streamlit.button.side_effect = [False, True]  # Back button False, Next button True
    
    wizard._render_step_2()
    
    # Verify sample size was stored
    assert mock_streamlit.session_state.sample_size == 25.0
    assert mock_streamlit.session_state.wizard_step == 3

def test_step3_query_types(wizard, mock_streamlit):
    """Test query type selection step"""
    # Setup session state
    mock_streamlit.session_state.wizard_step = 3
    
    # Mock multiselect and button
    mock_streamlit.multiselect.return_value = ["SELECT", "INSERT"]
    mock_streamlit.button.side_effect = [False, True]  # Back button False, Next button True
    
    wizard._render_step_3()
    
    # Verify query types were stored
    assert mock_streamlit.session_state.query_types == ["SELECT", "INSERT"]
    assert mock_streamlit.session_state.wizard_step == 4

def test_step4_user_filtering(wizard, mock_streamlit):
    """Test user filtering step"""
    # Setup session state
    mock_streamlit.session_state.wizard_step = 4
    
    # Mock text area input
    mock_streamlit.text_area.return_value = "user1\nuser2\n-user3\n-user4"
    mock_streamlit.button.side_effect = [False, True]  # Back button False, Next button True
    
    wizard._render_step_4()
    
    # Verify user filters were stored
    assert mock_streamlit.session_state.user_include == ["user1", "user2"]
    assert mock_streamlit.session_state.user_exclude == ["user3", "user4"]
    assert mock_streamlit.session_state.wizard_step == 5

def test_step5_performance_focus(wizard, mock_streamlit):
    """Test performance focus step"""
    # Setup session state
    mock_streamlit.session_state.wizard_step = 5
    mock_streamlit.session_state.start_date = datetime.now().date() - timedelta(days=7)
    mock_streamlit.session_state.end_date = datetime.now().date()
    mock_streamlit.session_state.sample_size = 25.0
    mock_streamlit.session_state.query_types = ["SELECT", "INSERT"]
    mock_streamlit.session_state.user_include = ["user1", "user2"]
    mock_streamlit.session_state.user_exclude = ["user3", "user4"]
    
    # Mock multiselect and finish button
    mock_streamlit.multiselect.return_value = ["Slow Queries", "Frequent Queries"]
    mock_streamlit.button.side_effect = [False, False, True]  # Back False, Reset False, Finish True
    
    config = wizard._render_step_5()
    
    # Verify config was created correctly
    assert isinstance(config, SamplingConfig)
    assert config.sample_size == 0.25  # Converted to fraction
    assert config.query_types == ["SELECT", "INSERT"]
    assert config.user_include == ["user1", "user2"]
    assert config.user_exclude == ["user3", "user4"]
    assert config.query_focus == ["Slow Queries", "Frequent Queries"]

def test_render_wizard_complete_flow(wizard, mock_streamlit):
    """Test complete wizard flow"""
    # Start at step 1
    mock_streamlit.session_state.wizard_step = 1
    
    # Mock date inputs
    start_date = datetime.now().date() - timedelta(days=7)
    end_date = datetime.now().date()
    mock_streamlit.date_input.side_effect = [start_date, end_date] * 5
    
    # Mock other inputs
    mock_streamlit.slider.return_value = 25.0
    mock_streamlit.multiselect.side_effect = [
        ["SELECT", "INSERT"],  # Step 3
        ["Slow Queries", "Frequent Queries"]  # Step 5
    ] * 5
    
    mock_streamlit.text_area.return_value = "user1\n-user2"
    
    # Mock button clicks for each step
    mock_streamlit.button.side_effect = [
        # Step 1: Only Next button
        True,  # Next
        
        # Step 2: Back + Next buttons
        False,  # Back
        True,   # Next
        
        # Step 3: Back + Next buttons
        False,  # Back
        True,   # Next
        
        # Step 4: Back + Next buttons
        False,  # Back
        True,   # Next
        
        # Step 5: Back + Reset + Finish buttons
        False,  # Back
        False,  # Reset
        True    # Finish
    ]
    
    # Step 1: Time Window
    config = wizard.render_wizard()
    assert config is None
    assert mock_streamlit.session_state.wizard_step == 2
    assert mock_streamlit.session_state.start_date == start_date
    assert mock_streamlit.session_state.end_date == end_date
    
    # Step 2: Sample Size
    config = wizard.render_wizard()
    assert config is None
    assert mock_streamlit.session_state.wizard_step == 3
    assert mock_streamlit.session_state.sample_size == 25.0
    
    # Step 3: Query Types
    config = wizard.render_wizard()
    assert config is None
    assert mock_streamlit.session_state.wizard_step == 4
    assert mock_streamlit.session_state.query_types == ["SELECT", "INSERT"]
    
    # Step 4: User Filtering
    config = wizard.render_wizard()
    assert config is None
    assert mock_streamlit.session_state.wizard_step == 5
    assert mock_streamlit.session_state.user_include == ["user1"]
    assert mock_streamlit.session_state.user_exclude == ["user2"]
    
    # Step 5: Performance Focus
    config = wizard.render_wizard()
    
    # Verify final config
    assert isinstance(config, SamplingConfig)
    assert config.sample_size == 0.25
    assert config.query_types == ["SELECT", "INSERT"]
    assert config.user_include == ["user1"]
    assert config.user_exclude == ["user2"]
    assert config.query_focus == ["Slow Queries", "Frequent Queries"]
