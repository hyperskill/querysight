import os
from dotenv import load_dotenv
from typing import Optional

# Load environment variables from .env file
load_dotenv()

class Config:
    # ClickHouse configuration
    CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', '')
    CLICKHOUSE_DATABASE: str = os.getenv('CLICKHOUSE_DATABASE', 'default')

    # OpenAI configuration
    OPENAI_API_KEY: Optional[str] = os.getenv('OPENAI_API_KEY')
    
    # DBT configuration
    DBT_PROJECT_PATH: str = os.getenv('DBT_PROJECT_PATH', '')

    @classmethod
    def validate_config(cls) -> tuple[bool, list[str]]:
        """
        Validate the configuration and return a tuple of (is_valid, missing_vars)
        """
        required_vars = {
            'CLICKHOUSE_HOST': cls.CLICKHOUSE_HOST,
            'CLICKHOUSE_USER': cls.CLICKHOUSE_USER,
            'CLICKHOUSE_PASSWORD': cls.CLICKHOUSE_PASSWORD,
            'CLICKHOUSE_DATABASE': cls.CLICKHOUSE_DATABASE,
            'OPENAI_API_KEY': cls.OPENAI_API_KEY,
            'DBT_PROJECT_PATH': cls.DBT_PROJECT_PATH
        }
        
        missing_vars = [var for var, value in required_vars.items() 
                       if not value or value.strip() == '']
        
        return len(missing_vars) == 0, missing_vars
