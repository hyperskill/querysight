# QuerySight: ClickHouse Log-Driven dbt Project Enhancer

QuerySight helps optimize dbt projects by analyzing ClickHouse query logs, identifying inefficiencies, and suggesting improvements. By analyzing query logs and integrating with your dbt project, it helps identify optimization opportunities and improve query performance.

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
git clone https://github.com/hyperskill/querysight.git
cd querysight
```

2. Install dependencies:
```bash
python -m venv venv
source venv/bin/activate  # (or `venv\Scripts\activate` on Windows)
pip install -r requirements.txt
```

## Getting Started

QuerySight now supports multiple free LLM providers to ensure the tool is accessible without requiring paid API subscriptions. Follow these steps to get started quickly:

### Option 1: Using Google's Gemini 1.5 Flash (Free Tier)

1. Get a free Gemini API key from [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Set up your configuration in `.env`:

```bash
# ClickHouse Connection
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=default

# Gemini API Key (free tier)
GEMINI_API_KEY=your_gemini_key
LLM_MODEL=gemini-1.5-flash

# Base URL for API endpoints (optional)
# BASE_URL=http://localhost:8000

# Optional dbt Configuration
DBT_PROJECT_PATH=/path/to/dbt/project
```

3. Run your first analysis:
```bash
python querysight.py analyze --days 3
```

### Option 2: Using GitHub Marketplace Models (Free Tier)

1. Generate a GitHub personal access token with appropriate scopes from [GitHub Settings](https://github.com/settings/tokens)
2. Set up your configuration in `.env`:

```bash
# Required configuration
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=default
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=default

# GitHub API token for GitHub Marketplace Models
GITHUB_API_TOKEN=your_github_token
# Specify a GitHub Marketplace model (examples below)
LLM_MODEL=github/openrouter/llama3-70b-8192

# Optional: Base URL for API endpoints (optional)
# BASE_URL=http://localhost:8000

# Optional dbt Configuration
DBT_PROJECT_PATH=/path/to/dbt/project
```

3. Available GitHub Marketplace Models:
   - `github/openrouter/llama3-70b-8192` - Llama 3 70B (powerful and free)
   - `github/mistral/mistral-7b` - Mistral 7B (fast and efficient)
   - `github/codestral/codestral-22b` - Codestral 22B (optimized for code)
   
Rate limits depend on your GitHub account tier (Free/Pro/Team/Enterprise).

### Additional Free LLM Providers

You can use any of these additional free LLM providers by setting the appropriate API key and model name:

| Provider | Environment Variable | Model Example | URL | Notes |
|----------|---------------------|---------------|-----|-------|
| Gemini   | GEMINI_API_KEY      | gemini-1.5-flash | [Google AI Studio](https://aistudio.google.com) | Free tier available |
| HuggingFace | HUGGINGFACE_API_KEY | huggingface/... | [HuggingFace](https://huggingface.co) | Free API credits |
| Together.ai | Set via litellm | together/... | [Together](https://together.ai) | Free credits |
| Groq | Set via litellm | groq/... | [Groq](https://console.groq.com) | Fast inference |

## Configuration

Create a `.env` file with your configuration (or copy from `.env.example`):

```bash
# ClickHouse Connection, QuerySight needs read-only permissions for system schema and users schemas
CLICKHOUSE_HOST=localhost
CLICKHOUSE_PORT=9000
CLICKHOUSE_USER=readonly_user_with_additional_permissions
CLICKHOUSE_PASSWORD=your_password
CLICKHOUSE_DATABASE=default

# Base URL for API endpoints (optional)
# BASE_URL=http://localhost:8000

# LLM Provider (choose one or more)
GEMINI_API_KEY=your_gemini_key
GITHUB_API_TOKEN=your_github_token
OPENAI_API_KEY=your_openai_key
ANTHROPIC_API_KEY=your_anthropic_key
HUGGINGFACE_API_KEY=your_huggingface_key
LITELLM_API_KEY=your_litellm_key

# Specify which model to use (select one)
LLM_MODEL=gemini-1.5-flash
# LLM_MODEL=github/openrouter/llama3-70b-8192
# LLM_MODEL=openai/gpt-4o-mini

# Cache TTL settings (in seconds)
CACHE_TTL_DEFAULT=3600       # 1 hour for default data
CACHE_TTL_PATTERNS=43200     # 12 hours for query patterns
CACHE_TTL_MODELS=86400       # 24 hours for dbt models
CACHE_TTL_AI=604800          # 1 week for AI responses

# Optional dbt Configuration
DBT_PROJECT_PATH=/path/to/dbt/project
```

## Usage

### Analysis Command

```bash
python querysight.py analyze [OPTIONS]

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
  --select-patterns TEXT     Filter specific patterns by pattern_id (pattern_id is getting created at the first analysis step, you can select patterns of interest on the next steps
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
python querysight.py export [OPTIONS]
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
├── querysight.py           # Main CLI interface
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
