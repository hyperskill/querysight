import pytest
import io
from datetime import datetime
from pypdf import PdfReader
from utils.pdf_generator import PDFReportGenerator

@pytest.fixture
def sample_query_patterns():
    return [
        {
            'pattern': 'SELECT * FROM users WHERE id = ?',
            'frequency': 1000,
            'avg_duration_ms': 150.5,
            'avg_read_rows': 1
        },
        {
            'pattern': 'SELECT * FROM orders WHERE user_id = ?',
            'frequency': 800,
            'avg_duration_ms': 200.0,
            'avg_read_rows': 10
        },
        {
            'pattern': 'SELECT * FROM products WHERE category = ?',
            'frequency': 600,
            'avg_duration_ms': 180.0,
            'avg_read_rows': 50
        }
    ]

@pytest.fixture
def sample_suggestions():
    return [
        {
            'title': 'Add Index on users.email',
            'impact_level': 'High',
            'problem_description': 'Frequent full table scans on users table',
            'optimization_details': {
                'benefits': [
                    'Faster email lookups',
                    'Reduced I/O operations'
                ],
                'potential_risks': [
                    'Increased write time',
                    'Additional disk space usage'
                ]
            },
            'implementation_steps': [
                'Take a backup of the database',
                'Create index during low-traffic period',
                'Monitor query performance'
            ]
        },
        {
            'title': 'Optimize JOIN order',
            'impact_level': 'Medium',
            'problem_description': 'Suboptimal JOIN order in complex queries',
            'optimization_details': {
                'benefits': [
                    'Reduced intermediate result sizes',
                    'Better memory utilization'
                ],
                'potential_risks': [
                    'May require query rewriting',
                    'Testing needed for all scenarios'
                ]
            },
            'implementation_steps': [
                'Identify affected queries',
                'Test different JOIN orders',
                'Update application code'
            ]
        }
    ]

def test_pdf_generator_initialization():
    """Test PDF generator initialization"""
    generator = PDFReportGenerator()
    assert generator.styles is not None
    assert generator.title_style is not None
    assert generator.heading_style is not None
    assert generator.body_style is not None

def test_generate_report_structure(sample_query_patterns, sample_suggestions):
    """Test PDF report generation structure"""
    generator = PDFReportGenerator()
    pdf_bytes = generator.generate_report(
        query_patterns=sample_query_patterns,
        suggestions=sample_suggestions
    )

    # Verify PDF was generated
    assert isinstance(pdf_bytes, bytes)
    assert len(pdf_bytes) > 0

    # Read PDF content
    reader = PdfReader(io.BytesIO(pdf_bytes))
    page = reader.pages[0]
    text = page.extract_text()

    # Verify sections
    assert "QuerySight Performance Analysis" in text
    assert "Report" in text
    assert "Query Pattern Analysis" in text
    assert "Optimization Suggestions" in text

    # Verify query patterns
    assert "SELECT * FROM users WHERE id = ?" in text
    assert "1000" in text  # frequency
    assert "150.50" in text  # avg duration

    # Verify suggestions
    assert "Add Index on users.email" in text
    assert "High" in text  # impact level
    assert "Frequent full table scans" in text
    assert "Faster email lookups" in text
    assert "Take a backup of the database" in text

def test_generate_report_empty_data():
    """Test PDF generation with empty data"""
    generator = PDFReportGenerator()
    pdf_bytes = generator.generate_report(
        query_patterns=[],
        suggestions=[]
    )

    # Verify PDF was generated
    assert isinstance(pdf_bytes, bytes)
    assert len(pdf_bytes) > 0

    # Read PDF content
    reader = PdfReader(io.BytesIO(pdf_bytes))
    page = reader.pages[0]
    text = page.extract_text()

    # Verify basic structure
    assert "QuerySight Performance Analysis" in text
    assert "Report" in text
    assert "Query Pattern Analysis" in text
    assert "Optimization Suggestions" in text

def test_generate_report_special_characters():
    """Test PDF generation with special characters"""
    generator = PDFReportGenerator()
    patterns = [{
        'pattern': 'SELECT * FROM "users" WHERE email LIKE \'%@%\'',
        'frequency': 100,
        'avg_duration_ms': 150.5,
        'avg_read_rows': 1
    }]
    suggestions = [{
        'title': 'Handle UTF-8 & special characters: é, ñ, 漢字',
        'impact_level': 'Medium',
        'problem_description': 'Issues with <special> & "quoted" text',
        'optimization_details': {
            'benefits': ['Improved character handling'],
            'potential_risks': ['None']
        },
        'implementation_steps': ['Update collation']
    }]

    pdf_bytes = generator.generate_report(
        query_patterns=patterns,
        suggestions=suggestions
    )

    # Verify PDF was generated
    assert isinstance(pdf_bytes, bytes)
    assert len(pdf_bytes) > 0

    # Read PDF content
    reader = PdfReader(io.BytesIO(pdf_bytes))
    page = reader.pages[0]
    text = page.extract_text()

    # Verify special characters
    assert 'SELECT * FROM "users"' in text
    assert 'special characters' in text
    assert 'quoted' in text

def test_generate_report_long_content():
    """Test PDF generation with long content that spans multiple pages"""
    generator = PDFReportGenerator()
    
    # Create many query patterns
    patterns = []
    for i in range(20):  # 20 patterns should force multiple pages
        patterns.append({
            'pattern': f'SELECT * FROM table_{i} WHERE id = ?',
            'frequency': 100 + i,
            'avg_duration_ms': 150.5 + i,
            'avg_read_rows': 1 + i
        })
    
    # Create many suggestions
    suggestions = []
    for i in range(10):  # 10 detailed suggestions
        suggestions.append({
            'title': f'Optimization {i + 1}',
            'impact_level': 'High' if i % 2 == 0 else 'Medium',
            'problem_description': f'Problem description for optimization {i + 1}',
            'optimization_details': {
                'benefits': [f'Benefit {j + 1}' for j in range(5)],
                'potential_risks': [f'Risk {j + 1}' for j in range(3)]
            },
            'implementation_steps': [f'Step {j + 1}' for j in range(5)]
        })

    pdf_bytes = generator.generate_report(
        query_patterns=patterns,
        suggestions=suggestions
    )

    # Verify PDF was generated
    assert isinstance(pdf_bytes, bytes)
    assert len(pdf_bytes) > 0

    # Read PDF content
    reader = PdfReader(io.BytesIO(pdf_bytes))
    
    # Verify multiple pages were generated
    assert len(reader.pages) > 1

    # Check content on first page
    first_page_text = reader.pages[0].extract_text()
    assert "QuerySight Performance Analysis" in first_page_text
    assert "Query Pattern Analysis" in first_page_text
    assert "table_0" in first_page_text  # First pattern

    # Check content on last page
    last_page_text = reader.pages[-1].extract_text()
    assert "Implementation Steps" in last_page_text  # Last suggestion's steps
    assert "Step 1" in last_page_text
    assert "Step 5" in last_page_text  # Last step
