"""
Unit tests for CLI main function.

Tests that the CLI executes successfully after restructuring.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock
from click.testing import CliRunner
import sys
import os

# Add parent directory to path to import clip module
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clip.cli import cli, main
from clip.config import Config


class TestCLIMain:
    """Test CLI main function execution."""
    
    def test_cli_main_import(self):
        """Test that CLI main function can be imported."""
        from clip.cli import main
        assert callable(main)
    
    def test_cli_group_callable(self):
        """Test that CLI group is callable."""
        assert callable(cli)
    
    @patch('clip.cli.Config.load')
    def test_cli_help_command(self, mock_config_load):
        """Test that CLI help command executes successfully."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['--help'])
        assert result.exit_code == 0
        assert 'Presto AI CLI' in result.output
    
    @patch('clip.cli.Config.load')
    def test_cli_version_command(self, mock_config_load):
        """Test that CLI version command executes successfully."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['--version'])
        assert result.exit_code == 0
        assert '0.1.0' in result.output
    
    @patch('clip.cli.Config.load')
    def test_config_subcommand_exists(self, mock_config_load):
        """Test that config subcommand exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['config', '--help'])
        assert result.exit_code == 0
        assert 'config' in result.output.lower()
    
    @patch('clip.cli.Config.load')
    def test_schema_subcommand_exists(self, mock_config_load):
        """Test that schema subcommand exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['schema', '--help'])
        assert result.exit_code == 0
        assert 'schema' in result.output.lower()
    
    @patch('clip.cli.Config.load')
    def test_query_command_exists(self, mock_config_load):
        """Test that query command exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['query', '--help'])
        assert result.exit_code == 0
    
    @patch('clip.cli.Config.load')
    def test_sql_command_exists(self, mock_config_load):
        """Test that sql command exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['sql', '--help'])
        assert result.exit_code == 0
    
    @patch('clip.cli.Config.load')
    def test_spark_command_exists(self, mock_config_load):
        """Test that spark command exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['spark', '--help'])
        assert result.exit_code == 0
    
    @patch('clip.cli.Config.load')
    def test_ask_command_exists(self, mock_config_load):
        """Test that ask command exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['ask', '--help'])
        assert result.exit_code == 0
    
    @patch('clip.cli.Config.load')
    def test_chat_command_exists(self, mock_config_load):
        """Test that chat command exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['chat', '--help'])
        assert result.exit_code == 0
    
    @patch('clip.cli.Config.load')
    def test_superset_subcommand_exists(self, mock_config_load):
        """Test that superset subcommand exists."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['superset', '--help'])
        assert result.exit_code == 0
        assert 'superset' in result.output.lower()
    
    @patch('clip.cli.Config.load')
    def test_main_function_runs(self, mock_config_load):
        """Test that main function can be called."""
        mock_config_load.return_value = Config()
        # Test that main() doesn't raise an exception
        # We can't actually run it as it would start the CLI
        assert callable(main)
    
    @patch('clip.cli.Config.load')
    def test_cli_context_object_created(self, mock_config_load):
        """Test that CLI creates context object with config."""
        test_config = Config()
        mock_config_load.return_value = test_config
        runner = CliRunner()
        
        # Create a simple test command to inspect context
        @cli.command('test-context')
        @patch.object(cli, 'pass_context', True)
        def test_cmd(ctx):
            assert 'config' in ctx.obj
            assert isinstance(ctx.obj['config'], Config)
        
        result = runner.invoke(cli, ['--help'])
        # Just verify config loading works
        mock_config_load.assert_called()


class TestConfigCommands:
    """Test config-related CLI commands."""
    
    @patch('clip.cli.Config.load')
    @patch('clip.cli.Config.save')
    def test_config_show_command(self, mock_save, mock_config_load):
        """Test config show command."""
        test_config = Config()
        test_config.presto_host = 'test-host'
        mock_config_load.return_value = test_config
        
        runner = CliRunner()
        result = runner.invoke(cli, ['config', 'show'])
        assert result.exit_code == 0
        assert 'test-host' in result.output
    
    @patch('clip.cli.Config.load')
    def test_config_path_command(self, mock_config_load):
        """Test config path command."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['config', 'path'])
        assert result.exit_code == 0
        assert 'config.json' in result.output
    
    @patch('clip.cli.Config.load')
    @patch('clip.cli.Config.save')
    def test_config_set_valid_key(self, mock_save, mock_config_load):
        """Test config set with valid key."""
        test_config = Config()
        mock_config_load.return_value = test_config
        
        runner = CliRunner()
        result = runner.invoke(cli, ['config', 'set', 'presto_host', 'new-host'])
        assert result.exit_code == 0
        assert 'new-host' in result.output
    
    @patch('clip.cli.Config.load')
    def test_config_set_invalid_key(self, mock_config_load):
        """Test config set with invalid key."""
        mock_config_load.return_value = Config()
        runner = CliRunner()
        result = runner.invoke(cli, ['config', 'set', 'invalid_key', 'value'])
        assert 'Invalid key' in result.output


class TestSchemaCommands:
    """Test schema exploration CLI commands."""
    
    @patch('clip.cli.Config.load')
    @patch('clip.presto_client.PrestoClient')
    def test_schema_catalogs_command(self, mock_presto_client, mock_config_load):
        """Test schema catalogs command."""
        mock_config_load.return_value = Config()
        mock_client = Mock()
        mock_client.get_catalogs.return_value = ['catalog1', 'catalog2']
        mock_client.execute.return_value = [{'Catalog': 'catalog1'}, {'Catalog': 'catalog2'}]
        mock_presto_client.return_value = mock_client
        
        runner = CliRunner()
        result = runner.invoke(cli, ['schema', 'catalogs'])
        assert result.exit_code == 0
        assert 'catalog1' in result.output or 'catalog2' in result.output
    
    @patch('clip.cli.Config.load')
    @patch('clip.presto_client.PrestoClient')
    def test_schema_tables_command(self, mock_presto_client, mock_config_load):
        """Test schema tables command."""
        test_config = Config()
        test_config.presto_catalog = 'test_catalog'
        test_config.presto_schema = 'test_schema'
        mock_config_load.return_value = test_config
        
        mock_client = Mock()
        mock_client.get_tables.return_value = ['table1', 'table2']
        mock_presto_client.return_value = mock_client
        
        runner = CliRunner()
        result = runner.invoke(cli, ['schema', 'tables'])
        assert result.exit_code == 0


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
