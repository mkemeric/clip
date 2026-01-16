"""
Unit tests for LLM client.

Tests that LLM client handles requests correctly after renaming.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clip.llm_client import (
    LLMClient,
    BaseLLMProvider,
    OpenAIProvider,
    AnthropicProvider,
    OllamaProvider,
    CustomProvider
)
from clip.config import Config


class TestLLMClientCreation:
    """Test LLM client instantiation."""
    
    def test_llm_client_import(self):
        """Test that LLMClient can be imported from clip.llm_client."""
        from clip.llm_client import LLMClient
        assert LLMClient is not None
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_llm_client_creation_openai(self, mock_openai):
        """Test LLMClient creation with OpenAI provider."""
        config = Config(
            llm_provider='openai',
            llm_api_key='test-key',
            llm_model='gpt-4'
        )
        client = LLMClient(config)
        assert client.config == config
        assert client.provider is not None
    
    @patch('clip.llm_client.AnthropicProvider')
    def test_llm_client_creation_anthropic(self, mock_anthropic):
        """Test LLMClient creation with Anthropic provider."""
        config = Config(
            llm_provider='anthropic',
            llm_api_key='test-key',
            llm_model='claude-3'
        )
        client = LLMClient(config)
        assert client.config == config
    
    @patch('clip.llm_client.OllamaProvider')
    def test_llm_client_creation_ollama(self, mock_ollama):
        """Test LLMClient creation with Ollama provider."""
        config = Config(
            llm_provider='ollama',
            llm_model='llama2'
        )
        client = LLMClient(config)
        assert client.config == config
    
    def test_llm_client_invalid_provider(self):
        """Test LLMClient raises error for invalid provider."""
        config = Config(llm_provider='invalid_provider')
        with pytest.raises(ValueError, match='Unknown LLM provider'):
            LLMClient(config)
    
    def test_llm_client_missing_api_key_openai(self):
        """Test LLMClient raises error when OpenAI key missing."""
        config = Config(llm_provider='openai', llm_api_key=None)
        with pytest.raises(ValueError, match='OpenAI API key required'):
            LLMClient(config)


class TestLLMProviders:
    """Test individual LLM provider classes."""
    
    @patch('clip.llm_client.OpenAI')
    def test_openai_provider_creation(self, mock_openai_class):
        """Test OpenAI provider initialization."""
        mock_openai_class.return_value = Mock()
        provider = OpenAIProvider(
            api_key='test-key',
            model='gpt-4'
        )
        assert provider.model == 'gpt-4'
        mock_openai_class.assert_called_once()
    
    @patch('clip.llm_client.OpenAI')
    def test_openai_provider_with_custom_base(self, mock_openai_class):
        """Test OpenAI provider with custom API base."""
        mock_openai_class.return_value = Mock()
        provider = OpenAIProvider(
            api_key='test-key',
            model='gpt-4',
            api_base='https://custom.api.com'
        )
        mock_openai_class.assert_called_once()
        call_kwargs = mock_openai_class.call_args[1]
        assert call_kwargs['base_url'] == 'https://custom.api.com'
    
    @patch('clip.llm_client.Anthropic')
    def test_anthropic_provider_creation(self, mock_anthropic_class):
        """Test Anthropic provider initialization."""
        mock_anthropic_class.return_value = Mock()
        provider = AnthropicProvider(
            api_key='test-key',
            model='claude-3'
        )
        assert provider.model == 'claude-3'
    
    @patch('clip.llm_client.ollama')
    def test_ollama_provider_creation(self, mock_ollama):
        """Test Ollama provider initialization."""
        provider = OllamaProvider(model='llama2')
        assert provider.model == 'llama2'
        assert provider.host == 'http://localhost:11434'
    
    @patch('clip.llm_client.OpenAI')
    def test_custom_provider_creation(self, mock_openai_class):
        """Test custom provider initialization."""
        mock_openai_class.return_value = Mock()
        provider = CustomProvider(
            api_key='test-key',
            model='custom-model',
            api_base='https://custom.api.com'
        )
        assert provider.model == 'custom-model'


class TestCodeExtraction:
    """Test code extraction from LLM responses."""
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_extract_sql_from_code_block(self, mock_provider):
        """Test extracting SQL from markdown code block."""
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        response = """```sql
SELECT * FROM users
WHERE active = true;
```"""
        
        extracted = client._extract_code(response, 'sql')
        assert 'SELECT * FROM users' in extracted
        assert extracted.strip().endswith('true')  # semicolon removed
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_extract_python_from_code_block(self, mock_provider):
        """Test extracting Python from markdown code block."""
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        response = """```python
import pandas as pd
df = pd.read_csv('data.csv')
```"""
        
        extracted = client._extract_code(response, 'python')
        assert 'import pandas' in extracted
        assert 'pd.read_csv' in extracted
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_extract_code_without_markers(self, mock_provider):
        """Test extracting code when no code blocks present."""
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        response = "SELECT * FROM table;"
        extracted = client._extract_code(response, 'sql')
        assert extracted == 'SELECT * FROM table'  # semicolon removed
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_extract_removes_trailing_semicolon_sql(self, mock_provider):
        """Test that SQL extraction removes trailing semicolons."""
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        response = "SELECT 1;;;"
        extracted = client._extract_code(response, 'sql')
        assert not extracted.endswith(';')


class TestSQLGeneration:
    """Test SQL generation functionality."""
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_generate_sql_called(self, mock_provider_class):
        """Test that generate_sql calls provider correctly."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "SELECT * FROM users"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        sql = client.generate_sql(
            prompt="Get all users",
            schema_context="CREATE TABLE users (id INT, name VARCHAR)",
            dialect='presto'
        )
        
        mock_provider.complete.assert_called_once()
        assert sql == "SELECT * FROM users"
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_generate_sql_includes_schema_context(self, mock_provider_class):
        """Test that generate_sql includes schema in prompt."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "SELECT id FROM users"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        schema = "CREATE TABLE users (id INT, name VARCHAR)"
        client.generate_sql(
            prompt="Get user IDs",
            schema_context=schema,
            dialect='presto'
        )
        
        # Verify schema was included in the messages
        call_args = mock_provider.complete.call_args
        messages = call_args[0][0]
        assert any(schema in str(msg) for msg in messages)
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_generate_sql_uses_correct_dialect(self, mock_provider_class):
        """Test that generate_sql uses specified dialect."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "SELECT 1"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        client.generate_sql(
            prompt="Test query",
            schema_context="",
            dialect='presto'
        )
        
        # Verify PRESTO dialect is mentioned in system prompt
        call_args = mock_provider.complete.call_args
        messages = call_args[0][0]
        system_msg = next(m for m in messages if m['role'] == 'system')
        assert 'PRESTO' in system_msg['content'].upper()


class TestPySparkGeneration:
    """Test PySpark code generation functionality."""
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_generate_pyspark_called(self, mock_provider_class):
        """Test that generate_pyspark calls provider correctly."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "df = spark.read.csv('data.csv')"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        code = client.generate_pyspark(
            prompt="Read CSV file",
            schema_context=""
        )
        
        mock_provider.complete.assert_called_once()
        assert 'spark.read' in code
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_generate_pyspark_with_presto_config(self, mock_provider_class):
        """Test PySpark generation with Presto configuration."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "df = spark.read.jdbc(...)"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        presto_config = {
            'host': 'presto.example.com',
            'port': 8080,
            'catalog': 'hive',
            'schema': 'default'
        }
        
        client.generate_pyspark(
            prompt="Read from Presto",
            schema_context="",
            presto_config=presto_config
        )
        
        # Verify Presto config is in the prompt
        call_args = mock_provider.complete.call_args
        messages = call_args[0][0]
        system_msg = next(m for m in messages if m['role'] == 'system')
        assert 'presto.example.com' in system_msg['content']


class TestResultsExplanation:
    """Test results explanation functionality."""
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_explain_results_called(self, mock_provider_class):
        """Test that explain_results calls provider correctly."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "The query returned 5 users."
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        results = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        
        explanation = client.explain_results(
            question="How many users?",
            sql="SELECT * FROM users",
            results=results
        )
        
        mock_provider.complete.assert_called_once()
        assert explanation == "The query returned 5 users."
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_explain_results_includes_query_info(self, mock_provider_class):
        """Test that explanation includes query information."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "Explanation"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        sql = "SELECT COUNT(*) FROM users"
        client.explain_results(
            question="User count?",
            sql=sql,
            results=[{'count': 42}]
        )
        
        # Verify SQL was included in messages
        call_args = mock_provider.complete.call_args
        messages = call_args[0][0]
        user_msg = next(m for m in messages if m['role'] == 'user')
        assert sql in user_msg['content']
    
    @patch('clip.llm_client.OpenAIProvider')
    def test_explain_results_limits_data(self, mock_provider_class):
        """Test that explanation limits result data sent to LLM."""
        mock_provider = Mock()
        mock_provider.complete.return_value = "Summary"
        mock_provider_class.return_value = mock_provider
        
        config = Config(llm_provider='openai', llm_api_key='test')
        client = LLMClient(config)
        
        # Create 20 results
        results = [{'id': i, 'name': f'User{i}'} for i in range(20)]
        
        client.explain_results(
            question="List users",
            sql="SELECT * FROM users",
            results=results
        )
        
        # Verify only preview was sent (implementation limits to 10)
        call_args = mock_provider.complete.call_args
        messages = call_args[0][0]
        user_msg = next(m for m in messages if m['role'] == 'user')
        # Should not include all 20 results
        assert 'User19' not in user_msg['content'] or 'preview' in user_msg['content'].lower()


class TestProviderMissingDependencies:
    """Test handling of missing provider dependencies."""
    
    def test_openai_provider_missing_import(self):
        """Test OpenAI provider handles missing openai package."""
        with patch.dict('sys.modules', {'openai': None}):
            with pytest.raises(ImportError, match='openai package required'):
                OpenAIProvider('key', 'model')
    
    def test_anthropic_provider_missing_import(self):
        """Test Anthropic provider handles missing anthropic package."""
        with patch.dict('sys.modules', {'anthropic': None}):
            with pytest.raises(ImportError, match='anthropic package required'):
                AnthropicProvider('key', 'model')
    
    def test_ollama_provider_missing_import(self):
        """Test Ollama provider handles missing ollama package."""
        with patch.dict('sys.modules', {'ollama': None}):
            with pytest.raises(ImportError, match='ollama package required'):
                OllamaProvider('model')


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
