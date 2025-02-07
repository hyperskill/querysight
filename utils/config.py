import os
from dotenv import load_dotenv
from typing import Optional
import logging

logger = logging.getLogger(__name__)

# Load environment variables from .env file
logger.info("Loading environment variables")
load_dotenv()

class Config:
    # Cache configuration
    CACHE_DIR: str = os.getenv('CACHE_DIR', os.path.join(os.path.expanduser('~'), '.querysight', 'cache'))

    # ClickHouse configuration
    CLICKHOUSE_HOST: str = os.getenv('CLICKHOUSE_HOST', 'localhost')
    CLICKHOUSE_PORT: int = int(os.getenv('CLICKHOUSE_PORT', '9000'))
    CLICKHOUSE_USER: str = os.getenv('CLICKHOUSE_USER', 'default')
    CLICKHOUSE_PASSWORD: str = os.getenv('CLICKHOUSE_PASSWORD', '')
    CLICKHOUSE_DATABASE: str = os.getenv('CLICKHOUSE_DATABASE', 'default')

    # AI Providers
    OPENAI_API_KEY: Optional[str] = os.getenv("OPENAI_API_KEY")
    ANTHROPIC_API_KEY: Optional[str] = os.getenv("ANTHROPIC_API_KEY")
    HUGGINGFACE_API_KEY: Optional[str] = os.getenv("HUGGINGFACE_API_KEY")
    DEEPSEEK_API_KEY: Optional[str] = os.getenv("DEEPSEEK_API_KEY")
    LITELLM_API_KEY: Optional[str] = os.getenv("LITELLM_API_KEY")
    LLM_MODEL: str = os.getenv("LLM_MODEL", "openai/gpt-4")

    # DBT configuration
    DBT_PROJECT_PATH: str = os.getenv('DBT_PROJECT_PATH', '')
    logger.info(f"Loaded DBT_PROJECT_PATH: {DBT_PROJECT_PATH}")

    @classmethod
    def validate_config(cls) -> tuple[bool, list[str]]:
        """
        Validate the configuration and return a tuple of (is_valid, missing_vars)
        """
        required_vars = {
            'CACHE_DIR': cls.CACHE_DIR,
            'CLICKHOUSE_HOST': cls.CLICKHOUSE_HOST,
            'CLICKHOUSE_USER': cls.CLICKHOUSE_USER,
            'CLICKHOUSE_PASSWORD': cls.CLICKHOUSE_PASSWORD,
            'CLICKHOUSE_DATABASE': cls.CLICKHOUSE_DATABASE,
            'LLM_MODEL': cls.LLM_MODEL,
            'DBT_PROJECT_PATH': cls.DBT_PROJECT_PATH
        }
        
        logger.info("Validating config variables:")
        for var, value in required_vars.items():
            logger.info(f"  {var}: {'[EMPTY]' if not value or value.strip() == '' else '[SET]'}")
        
        missing_vars = [var for var, value in required_vars.items() 
                       if not value or value.strip() == '']
        
        return len(missing_vars) == 0, missing_vars
