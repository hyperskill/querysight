# QuerySight: ClickHouse Log-Driven dbt Project Enhancer

QuerySight is a powerful command-line tool that analyzes ClickHouse query patterns and provides intelligent optimization recommendations for dbt projects. By analyzing query logs and integrating with your dbt project, it helps identify optimization opportunities and improve query performance.

## Key Features

- 🔍 **Advanced Query Analysis**
  - Parse and analyze ClickHouse query logs
  - Track query frequency, duration, and memory usage patterns
  - Filter queries by users, types, and custom criteria
  - Intelligent pattern detection and categorization

- 📊 **dbt Integration**
  - Map queries to dbt models for coverage analysis
  - Track model dependencies and relationships
  - Identify unused or inefficient models
  - Generate model-specific optimization recommendations

- 🤖 **AI-Powered Optimization**
  - Smart recommendations using OpenAI integration
  - Pattern-based performance improvement suggestions
  - Model-specific optimization strategies
  - Best practices enforcement

- 💾 **Performance & Usability**
  - Intelligent caching system for faster repeated analysis
  - Batch processing for large query logs
  - Progress tracking with rich CLI interface
  - Flexible output formats (CLI, JSON)

## Prerequisites

- Python 3.10+
- ClickHouse database instance
- OpenAI API key (optional, for AI-powered recommendations)
- dbt project (recommended, for dbt integration features)

## Installation

1. Clone the repository:
```bash
git clone https://github.com/codeium/querysight.git
cd querysight
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

## Configuration

Create a `.env` file with your configuration (or copy from `.env.example`):

```bash
# ClickHouse Connection
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=default

# OpenAI Configuration
OPENAI_API_KEY=your_openai_key

# Optional dbt Configuration
DBT_PROJECT_PATH=/path/to/dbt/project
```

## Usage

### Analysis Command

```bash
python cli.py analyze [OPTIONS]

Analysis Options:
  --days INTEGER              Analysis timeframe [default: 7]
  --focus [queries|models]    Analysis focus [default: queries]
  --min-frequency INTEGER     Minimum query frequency [default: 5]
  --min-duration INTEGER      Minimum query duration in ms
  --sample-size INTEGER       Sample size for pattern analysis
  --batch-size INTEGER        Batch size for processing

Filtering Options:
  --include-users TEXT       Include specific users (comma-separated)
  --exclude-users TEXT       Exclude specific users (comma-separated)
  --query-kinds TEXT         Filter by query kinds (SELECT,INSERT,etc)
  --select-patterns TEXT     Filter specific patterns by pattern_id (pattern_id is getting created at the first analysis step, you can select patterns of interest                              on the next steps
  --select-tables TEXT       Filter specific tables
  --select-models TEXT       Filter specific dbt models

Output Options:
  --sort-by TEXT            Sort by [frequency|duration|memory]
  --page-size INTEGER       Results per page [default: 20]

Cache Options:
  --cache / --no-cache      Use cached data [default: True]
  --force-reset            Force cache reset

Analysis Level:
  --level TEXT             Analysis depth [data_collection|pattern_analysis|dbt_integration|optimization]
  --dbt-project TEXT       dbt project path
```

### Export Command

Export analysis results to JSON format:

```bash
python cli.py export [OPTIONS]
  --output TEXT    Output file path [default: stdout]
```

## Docker Support

Run QuerySight in a containerized environment:

```bash
# Using docker-compose
docker-compose up --build

# Or with Docker directly
docker build -t querysight .
docker run -it --network host \
  -v ~/.ssh:/root/.ssh:ro \
  -v /path/to/dbt:/app/dbt_project:ro \
  -v ./logs:/app/logs \
  -v ./.cache:/app/.cache \
  --env-file .env \
  querysight analyze --days 7
```

## Project Structure

```
querysight/
├── cli.py              # Main CLI interface
├── utils/
│   ├── ai_suggester.py     # AI-powered recommendations
│   ├── cache_manager.py    # Query cache management
│   ├── data_acquisition.py # ClickHouse data fetching
│   ├── dbt_analyzer.py     # dbt project analysis
│   ├── dbt_mapper.py       # Query to model mapping
│   ├── filtering.py        # Query filtering logic
│   ├── models.py           # Data models
│   └── sql_parser.py       # SQL parsing utilities
├── tests/              # Test suite
└── docker/            # Docker configuration
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.

## Acknowledgments

- Built with [ClickHouse](https://clickhouse.com/) integration
- Powered by [OpenAI](https://openai.com/) for intelligent recommendations
- Integrates with [dbt](https://www.getdbt.com/) for data transformation analysis
