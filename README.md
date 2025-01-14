# QuerySight: ClickHouse Log-Driven dbt Project Enhancer

QuerySight is a powerful command-line tool that analyzes ClickHouse query patterns and provides intelligent optimization recommendations for dbt projects. The code presented in this repository is work-in-progress, presented as is and is not intended for production use yet.

## Features

- 🔍 Analyze ClickHouse query logs for patterns and performance insights
- 📊 Generate detailed query pattern analysis with frequency, duration, and memory metrics
- 🎯 Map queries to dbt models for comprehensive coverage analysis
- 🤖 AI-powered optimization recommendations using OpenAI
- 💾 Intelligent caching system for faster repeated analysis
- 🔄 Export analysis results in JSON format

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
```bash
pip install -r requirements.txt
```

## Configuration

1. Set up environment variables in `.env` file:
```bash
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=default
OPENAI_API_KEY=your_openai_key
DBT_PROJECT_PATH=/path/to/dbt/project  # Optional
```

2. Or copy and modify the example file:
```bash
cp .env.example .env
```

## Usage

QuerySight provides two main commands:

### Analyze Command

Analyze query patterns and generate optimization recommendations:

```bash
python cli.py analyze [OPTIONS]

Options:
  --days INTEGER              Number of days to analyze [default: 7]
  --focus [queries|models]    Analysis focus [default: queries]
  --min-frequency INTEGER     Minimum query frequency [default: 5]
  --sample-size INTEGER      Sample size for pattern analysis
  --batch-size INTEGER       Batch size for processing
  --include-users TEXT       Include specific users
  --exclude-users TEXT       Exclude specific users
  --query-kinds TEXT         Filter by query kinds
  --cache / --no-cache      Use cached data [default: True]
  --force-reset             Force cache reset
  --level TEXT              Analysis level
  --dbt-project TEXT        dbt project path
  --select-patterns TEXT    Filter specific patterns
  --select-models TEXT      Filter specific models
  --sort-by TEXT           Sort results by [frequency|duration|memory]
  --page-size INTEGER      Results per page [default: 20]
```

### Export Command

Export analysis results to JSON:

```bash
python cli.py export [OPTIONS]

Options:
  --output TEXT  Output file path [default: stdout]
```

## Docker Support

Run QuerySight in a Docker container:

```bash
# Build and run with docker-compose
docker-compose up --build

# Or run directly with Docker
docker build -t querysight .
docker run -it --network host \
  -v ~/.ssh:/root/.ssh:ro \
  -v /path/to/dbt:/app/dbt_project:ro \
  -v ./logs:/app/logs \
  -v ./.cache:/app/.cache \
  --env-file .env \
  querysight analyze --days 7
```

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.
