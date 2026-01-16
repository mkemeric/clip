"""
Configuration management for presto-ai-cli.
"""

import os
import json
from pathlib import Path
from dataclasses import dataclass, field, asdict
from typing import Optional


def get_config_path() -> Path:
    """Get the path to the configuration file."""
    config_dir = Path(os.environ.get('XDG_CONFIG_HOME', Path.home() / '.config')) / 'presto-ai'
    config_dir.mkdir(parents=True, exist_ok=True)
    return config_dir / 'config.json'


@dataclass
class Config:
    """Configuration settings."""
    
    # Presto connection settings
    presto_host: str = 'localhost'
    presto_port: int = 8080
    presto_user: str = os.environ.get('USER', 'presto')
    presto_catalog: str = 'hive'
    presto_schema: str = 'default'
    presto_https: bool = False
    presto_insecure: bool = False  # Skip SSL verification (like curl -k)
    presto_password: Optional[str] = None
    presto_cookie: Optional[str] = None  # OAuth2 proxy cookie value
    presto_source: Optional[str] = None  # X-Presto-Source header
    
    # LLM settings
    llm_provider: str = 'openai'  # openai, anthropic, ollama, custom
    llm_model: str = 'gpt-4o'
    llm_api_key: Optional[str] = None
    llm_api_base: Optional[str] = None  # For custom endpoints
    
    # Superset settings
    superset_url: Optional[str] = None
    superset_username: Optional[str] = None
    superset_password: Optional[str] = None
    superset_token: Optional[str] = None  # Alternative to username/password
    superset_database_id: Optional[int] = None  # Default database ID for queries
    
    def __post_init__(self):
        """Load API keys from environment if not set."""
        if not self.llm_api_key:
            if self.llm_provider == 'openai':
                self.llm_api_key = os.environ.get('OPENAI_API_KEY')
            elif self.llm_provider == 'anthropic':
                self.llm_api_key = os.environ.get('ANTHROPIC_API_KEY')
        
        # Load Superset settings from environment
        if not self.superset_url:
            self.superset_url = os.environ.get('SUPERSET_URL')
        if not self.superset_username:
            self.superset_username = os.environ.get('SUPERSET_USERNAME')
        if not self.superset_password:
            self.superset_password = os.environ.get('SUPERSET_PASSWORD')
        if not self.superset_token:
            self.superset_token = os.environ.get('SUPERSET_TOKEN')
    
    @classmethod
    def load(cls) -> 'Config':
        """Load configuration from file."""
        config_path = get_config_path()
        
        if config_path.exists():
            try:
                data = json.loads(config_path.read_text())
                return cls(**data)
            except (json.JSONDecodeError, TypeError):
                pass
        
        return cls()
    
    def save(self):
        """Save configuration to file."""
        config_path = get_config_path()
        config_path.write_text(json.dumps(asdict(self), indent=2))
    
    def set(self, key: str, value):
        """Set a configuration value and save."""
        setattr(self, key, value)
        self.save()
    
    def get_presto_url(self) -> str:
        """Get the Presto JDBC-style URL."""
        return f"presto://{self.presto_host}:{self.presto_port}/{self.presto_catalog}/{self.presto_schema}"
