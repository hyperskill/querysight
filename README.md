# QuerySight: ClickHouse Log-Driven dbt Project Enhancer
# [WORK IN PROGRESS]

This project analyzes ClickHouse query logs and dbt project structure to suggest improvements for optimizing the most common queries and data patterns.

## Project Structure

The project consists of the following main components:

- `main.py`: The main script that combines all components and runs the analysis process.
- `utils/data_acquisition.py`: Module for retrieving and preprocessing query logs from ClickHouse.
- `utils/dbt_analyzer.py`: Module for analyzing the dbt project structure.
- `utils/ai_suggester.py`: Module for generating improvement suggestions using the OpenAI API.

## Requirements

- Python 3.7+
- clickhouse-driver
- PyYAML
- openai
- streamlit (optional, in case of using web interface)

## Installation

1. Clone the repository:  
  git clone https://github.com/yourusername/clickhouse-dbt-optimizer.git  
  cd clickhouse-dbt-optimizer  

2. Install dependencies:  
  `pip install -r requirements.txt`

## Usage

### Console interface

Run the `main.py` script with the necessary arguments:  
  `python main.py --dbt-project /path/to/dbt/project --start-date 2023-01-01 --end-date 2023-12-31 --openai-api-key your_openai_api_key`

Arguments:
- `--dbt-project`: Path to the dbt project
- `--start-date`: Start date for query analysis (YYYY-MM-DD)
- `--end-date`: End date for query analysis (YYYY-MM-DD)
- `--openai-api-key`: OpenAI API key

### Streamlit web interface
Run the Streamlit app:  
  `streamlit run streamlit_app.py`
Your default web browser should automatically open to `http://localhost:8501`. If it doesn't, you can manually open this URL.

Use the sidebar to input your configuration:
- Enter the path to your dbt project
- Select the start and end dates for query analysis
- Input your OpenAI API key
- Provide your ClickHouse credentials

Click the "Analyze and Suggest" button to start the analysis process.


## Workflow

1. The script will prompt for ClickHouse credentials.
2. Retrieves and preprocesses query logs from ClickHouse.
3. Analyzes queries to identify common patterns.
4. Analyzes the dbt project structure.
5. Generates improvement suggestions using the OpenAI API.
6. Outputs suggestions for dbt project improvements.

## Modules

### ClickHouseDataAcquisition

Responsible for retrieving query logs from ClickHouse, preprocessing the data, and analyzing queries.

### DBTProjectAnalyzer

Analyzes the dbt project structure, including models, sources, and macros.

### AISuggester

Uses the OpenAI API to generate improvement suggestions based on query analysis and dbt structure.

## Security

- Do not store ClickHouse credentials or OpenAI API key in the code. Use environment variables or a secure secret storage.
- Ensure you have the necessary permissions to access ClickHouse query logs.

## Contributing

Please create issues to report problems or suggest new features. Pull requests are welcome!
