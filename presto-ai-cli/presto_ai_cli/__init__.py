"""
Presto AI CLI - Generate SQL and PySpark from natural language.
"""

__version__ = "0.1.0"

from .config import Config
from .presto_client import PrestoClient
from .llm_client import LLMClient
from .superset_client import SupersetClient, SupersetConfig
from .notebook import (
    generate_sql_notebook,
    generate_pyspark_notebook,
    generate_analysis_notebook,
    save_notebook
)

__all__ = [
    'Config', 
    'PrestoClient', 
    'LLMClient',
    'SupersetClient',
    'SupersetConfig',
    'generate_sql_notebook',
    'generate_pyspark_notebook', 
    'generate_analysis_notebook',
    'save_notebook'
]
