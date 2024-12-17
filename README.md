# QuerySight: ClickHouse Log-Driven dbt Project Enhancer

QuerySight is an advanced Streamlit-powered analytics platform designed to revolutionize dbt project performance monitoring through intelligent insights and interactive optimization tools.

## Features

- 📊 Streamlit web interface with AI-driven performance insights
- 🔍 ClickHouse log parsing for real-time data transformation analysis
- 🤖 Intelligent performance optimization recommendations
- 📈 Advanced dbt workflow tracking and diagnostic capabilities
- 🧠 Machine learning-enhanced query improvement suggestions
- 💡 AI-powered proposal management system
- 🎯 Smart sampling wizard for efficient data analysis
- 📦 Intelligent cache management for improved performance

## Prerequisites

- Python 3.11+
- ClickHouse database instance
- OpenAI API key
- dbt project

## Installation

1. Clone the repository:
```bash
git clone https://github.com/codeium/querysight.git
cd querysight
```

2. Install dependencies:
The project uses the following main packages:
- clickhouse-driver: For ClickHouse database connectivity
- openai: For AI-powered suggestions
- pandas: For data analysis
- python-dotenv: For environment variable management
- pyyaml: For dbt project configuration parsing
- reportlab: For PDF report generation
- streamlit: For the web interface
- trafilatura: For web content extraction
- twilio: For notifications (optional)

You can install all dependencies using:
```bash
pip install -r requirements.txt
```

## Configuration

1. Set up your environment variables:
   - `OPENAI_API_KEY`: Your OpenAI API key
   - `DBT_PROJECT_PATH`: Path to your dbt project (when using Docker)

2. Configure ClickHouse connection:
   - Host
   - Port
   - Username
   - Password
   - Database

3. Cache Directory:
   - The application uses a `.cache` directory for the sampling wizard
   - This is automatically created in Docker, or you can create it manually:
   ```bash
   mkdir -p .cache
   chmod 777 .cache
   ```

## Usage

1. Start the Streamlit application:
```bash
streamlit run streamlit_app.py
```

2. Access the web interface at `http://localhost:8501`

3. In the sidebar:
   - Enter your dbt project path
   - Configure date range for analysis
   - Provide ClickHouse credentials
   - Enter your OpenAI API key

4. Use the "Analyze and Suggest" button to:
   - Analyze query patterns
   - Get AI-powered optimization suggestions
   - Generate performance reports

5. Manage improvement proposals:
   - Generate new proposals for specific query patterns
   - View and organize saved proposals
   - Track implementation progress

## Docker Deployment

### Prerequisites
- Docker
- Docker Compose

### Quick Start

1. Clone the repository:
```bash
git clone https://github.com/codeium/querysight.git
cd querysight
```

2. Set up environment variables:
```bash
cp .env.example .env
```
Edit the `.env` file with your configuration:
- Set your ClickHouse credentials
- Add your OpenAI API key

3. Build and run with Docker Compose:
```bash
docker compose up -d
```

The application will be available at `http://localhost:8501`

### Docker Configuration

The application is containerized with the following components:
- QuerySight web application (Streamlit)
- ClickHouse database

Key features of the Docker setup:
- Automatic database initialization
- Volume persistence for logs and database data
- Environment variable configuration
- Exposed ports:
  - 8501: Streamlit web interface
  - 9000: ClickHouse native interface
  - 8123: ClickHouse HTTP interface

### Maintenance

- View logs:
```bash
docker compose logs -f querysight
```

- Stop the application:
```bash
docker compose down
```

- Reset everything (including volumes):
```bash
docker compose down -v
```

## Components

### Data Acquisition
- `utils/data_acquisition.py`: Handles ClickHouse query log retrieval and analysis

### dbt Analysis
- `utils/dbt_analyzer.py`: Analyzes dbt project structure and dependencies

### AI Suggestions
- `utils/ai_suggester.py`: Generates intelligent optimization suggestions using OpenAI

### PDF Reports
- `utils/pdf_generator.py`: Creates detailed PDF reports of analysis and suggestions

## Project Structure

```
querysight/
├── streamlit_app.py      # Main Streamlit application
├── utils/               # Core functionality modules
│   ├── ai_suggester.py       # AI-powered optimization suggestions
│   ├── cache_manager.py      # Caching system for performance
│   ├── config.py             # Configuration management
│   ├── data_acquisition.py   # ClickHouse data retrieval
│   ├── dbt_analyzer.py       # dbt project analysis
│   ├── logger.py            # Logging configuration
│   ├── pdf_generator.py     # Report generation
│   └── sampling_wizard.py   # Smart data sampling
├── Dockerfile           # Container definition
├── docker-compose.yml   # Container orchestration
├── requirements.txt     # Python dependencies
└── pyproject.toml      # Project metadata and tools config
```

## Security Considerations

- Store sensitive credentials securely
- Use environment variables for API keys
- Ensure proper access controls for ClickHouse
- Regular security updates for dependencies

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. For major changes, please open an issue first to discuss what you would like to change.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
