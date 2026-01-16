"""
Unit tests for configuration loading and saving.

Tests that configuration loading and saving works with new config paths.
"""

import pytest
import json
import tempfile
import os
from pathlib import Path
from unittest.mock import patch, Mock
import sys

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clip.config import Config, get_config_path


class TestConfigPath:
    """Test configuration path handling."""
    
    def test_get_config_path_returns_path(self):
        """Test that get_config_path returns a Path object."""
        config_path = get_config_path()
        assert isinstance(config_path, Path)
        assert 'presto-ai' in str(config_path)
        assert 'config.json' in str(config_path)
    
    def test_get_config_path_creates_directory(self):
        """Test that get_config_path creates config directory."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {'XDG_CONFIG_HOME': tmpdir}):
                config_path = get_config_path()
                assert config_path.parent.exists()
                assert config_path.parent.name == 'presto-ai'
    
    def test_config_path_respects_xdg_config_home(self):
        """Test that config path respects XDG_CONFIG_HOME env var."""
        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {'XDG_CONFIG_HOME': tmpdir}):
                config_path = get_config_path()
                assert tmpdir in str(config_path)


class TestConfigCreation:
    """Test Config object creation and defaults."""
    
    def test_config_default_creation(self):
        """Test that Config can be created with defaults."""
        config = Config()
        assert config.presto_host == 'localhost'
        assert config.presto_port == 8080
        assert config.presto_catalog == 'hive'
        assert config.presto_schema == 'default'
        assert config.llm_provider == 'openai'
        assert config.llm_model == 'gpt-4o'
    
    def test_config_custom_values(self):
        """Test that Config can be created with custom values."""
        config = Config(
            presto_host='custom-host',
            presto_port=9090,
            presto_catalog='custom_catalog',
            llm_provider='anthropic',
            llm_model='claude-3'
        )
        assert config.presto_host == 'custom-host'
        assert config.presto_port == 9090
        assert config.presto_catalog == 'custom_catalog'
        assert config.llm_provider == 'anthropic'
        assert config.llm_model == 'claude-3'
    
    def test_config_user_from_env(self):
        """Test that presto_user defaults to USER env var."""
        with patch.dict(os.environ, {'USER': 'testuser'}):
            config = Config()
            assert config.presto_user == 'testuser'
    
    def test_config_boolean_fields(self):
        """Test boolean configuration fields."""
        config = Config(presto_https=True, presto_insecure=True)
        assert config.presto_https is True
        assert config.presto_insecure is True
    
    def test_config_optional_fields(self):
        """Test optional configuration fields."""
        config = Config()
        assert config.presto_password is None
        assert config.presto_cookie is None
        assert config.llm_api_key is None
        assert config.superset_url is None


class TestConfigEnvironmentVariables:
    """Test environment variable loading in Config."""
    
    def test_openai_api_key_from_env(self):
        """Test loading OpenAI API key from environment."""
        with patch.dict(os.environ, {'OPENAI_API_KEY': 'test-key-123'}):
            config = Config(llm_provider='openai')
            assert config.llm_api_key == 'test-key-123'
    
    def test_anthropic_api_key_from_env(self):
        """Test loading Anthropic API key from environment."""
        with patch.dict(os.environ, {'ANTHROPIC_API_KEY': 'test-anthropic-key'}):
            config = Config(llm_provider='anthropic')
            assert config.llm_api_key == 'test-anthropic-key'
    
    def test_superset_url_from_env(self):
        """Test loading Superset URL from environment."""
        with patch.dict(os.environ, {'SUPERSET_URL': 'http://superset.local'}):
            config = Config()
            assert config.superset_url == 'http://superset.local'
    
    def test_superset_credentials_from_env(self):
        """Test loading Superset credentials from environment."""
        env_vars = {
            'SUPERSET_USERNAME': 'admin',
            'SUPERSET_PASSWORD': 'password123',
            'SUPERSET_TOKEN': 'token-abc'
        }
        with patch.dict(os.environ, env_vars):
            config = Config()
            assert config.superset_username == 'admin'
            assert config.superset_password == 'password123'
            assert config.superset_token == 'token-abc'


class TestConfigSaveLoad:
    """Test configuration saving and loading."""
    
    def test_config_save_creates_file(self):
        """Test that save() creates a config file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config(presto_host='save-test-host')
                config.save()
                
                assert config_path.exists()
    
    def test_config_save_content(self):
        """Test that save() writes correct JSON content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config(
                    presto_host='json-test-host',
                    presto_port=7777,
                    llm_provider='ollama'
                )
                config.save()
                
                # Read and verify JSON content
                with open(config_path) as f:
                    data = json.load(f)
                
                assert data['presto_host'] == 'json-test-host'
                assert data['presto_port'] == 7777
                assert data['llm_provider'] == 'ollama'
    
    def test_config_load_existing_file(self):
        """Test that load() reads existing config file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write test config
            test_data = {
                'presto_host': 'load-test-host',
                'presto_port': 8888,
                'llm_provider': 'anthropic',
                'llm_model': 'claude-3'
            }
            config_path.write_text(json.dumps(test_data))
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config.load()
                
                assert config.presto_host == 'load-test-host'
                assert config.presto_port == 8888
                assert config.llm_provider == 'anthropic'
                assert config.llm_model == 'claude-3'
    
    def test_config_load_nonexistent_file(self):
        """Test that load() returns defaults when file doesn't exist."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config.load()
                
                # Should return defaults
                assert config.presto_host == 'localhost'
                assert config.presto_port == 8080
    
    def test_config_load_invalid_json(self):
        """Test that load() handles invalid JSON gracefully."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write invalid JSON
            config_path.write_text('{ invalid json }')
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config.load()
                
                # Should return defaults on error
                assert config.presto_host == 'localhost'
    
    def test_config_save_load_roundtrip(self):
        """Test that save() and load() work together correctly."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with patch('clip.config.get_config_path', return_value=config_path):
                # Create and save config
                original = Config(
                    presto_host='roundtrip-host',
                    presto_port=9999,
                    presto_catalog='roundtrip_catalog',
                    presto_schema='roundtrip_schema',
                    llm_provider='ollama',
                    llm_model='llama2'
                )
                original.save()
                
                # Load config
                loaded = Config.load()
                
                # Verify all fields match
                assert loaded.presto_host == original.presto_host
                assert loaded.presto_port == original.presto_port
                assert loaded.presto_catalog == original.presto_catalog
                assert loaded.presto_schema == original.presto_schema
                assert loaded.llm_provider == original.llm_provider
                assert loaded.llm_model == original.llm_model


class TestConfigSetMethod:
    """Test the Config.set() method."""
    
    def test_config_set_updates_value(self):
        """Test that set() updates configuration value."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config()
                config.set('presto_host', 'updated-host')
                
                assert config.presto_host == 'updated-host'
    
    def test_config_set_saves_automatically(self):
        """Test that set() saves configuration automatically."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = Path(tmpdir) / 'presto-ai' / 'config.json'
            config_path.parent.mkdir(parents=True, exist_ok=True)
            
            with patch('clip.config.get_config_path', return_value=config_path):
                config = Config()
                config.set('presto_port', 7000)
                
                # Verify file was saved
                assert config_path.exists()
                
                # Verify saved value
                data = json.loads(config_path.read_text())
                assert data['presto_port'] == 7000


class TestConfigPrestoURL:
    """Test the get_presto_url() method."""
    
    def test_get_presto_url_default(self):
        """Test get_presto_url with default values."""
        config = Config()
        url = config.get_presto_url()
        assert url == 'presto://localhost:8080/hive/default'
    
    def test_get_presto_url_custom(self):
        """Test get_presto_url with custom values."""
        config = Config(
            presto_host='custom-host',
            presto_port=9090,
            presto_catalog='custom_cat',
            presto_schema='custom_schema'
        )
        url = config.get_presto_url()
        assert url == 'presto://custom-host:9090/custom_cat/custom_schema'


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
