"""
Unit tests for Presto client.

Tests that Presto query execution in the new module path works correctly.
"""

import pytest
from unittest.mock import Mock, patch, MagicMock, PropertyMock
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clip.presto_client import PrestoClient
from clip.config import Config


class TestPrestoClientCreation:
    """Test Presto client instantiation."""
    
    def test_presto_client_import(self):
        """Test that PrestoClient can be imported from clip.presto_client."""
        from clip.presto_client import PrestoClient
        assert PrestoClient is not None
    
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_presto_client_creation_basic(self, mock_patch):
        """Test basic PrestoClient creation."""
        config = Config()
        client = PrestoClient(config)
        assert client.config == config
        assert client._conn is None  # Connection created lazily
    
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_presto_client_patches_ssl_when_insecure(self, mock_patch):
        """Test that client patches SSL when insecure flag is set."""
        config = Config(presto_insecure=True)
        client = PrestoClient(config)
        mock_patch.assert_called_once_with(insecure=True, cookie=None)
    
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_presto_client_patches_for_oauth_cookie(self, mock_patch):
        """Test that client patches for OAuth cookie."""
        config = Config(presto_cookie='test-cookie-value')
        client = PrestoClient(config)
        mock_patch.assert_called_once_with(insecure=False, cookie='test-cookie-value')


class TestPrestoConnection:
    """Test Presto connection management."""
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_connection_lazy_initialization(self, mock_patch, mock_connect):
        """Test that connection is created lazily."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        # Connection not created yet
        assert client._conn is None
        
        # Access conn property to trigger creation
        conn = client.conn
        
        # Now connection should be created
        assert conn == mock_conn
        mock_connect.assert_called_once()
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_connection_config_parameters(self, mock_patch, mock_connect):
        """Test that connection uses correct config parameters."""
        config = Config(
            presto_host='test-host',
            presto_port=9090,
            presto_user='testuser',
            presto_catalog='test_catalog',
            presto_schema='test_schema',
            presto_source='test-source'
        )
        
        client = PrestoClient(config)
        _ = client.conn
        
        mock_connect.assert_called_once()
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['host'] == 'test-host'
        assert call_kwargs['port'] == 9090
        assert call_kwargs['user'] == 'testuser'
        assert call_kwargs['catalog'] == 'test_catalog'
        assert call_kwargs['schema'] == 'test_schema'
        assert call_kwargs['source'] == 'test-source'
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_connection_https_scheme(self, mock_patch, mock_connect):
        """Test that HTTPS scheme is set when configured."""
        config = Config(presto_https=True)
        client = PrestoClient(config)
        _ = client.conn
        
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['http_scheme'] == 'https'
    
    @patch('clip.presto_client.prestodb.auth.BasicAuthentication')
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_connection_with_basic_auth(self, mock_patch, mock_connect, mock_auth):
        """Test connection with basic authentication."""
        mock_auth_instance = Mock()
        mock_auth.return_value = mock_auth_instance
        
        config = Config(
            presto_user='authuser',
            presto_password='authpass'
        )
        client = PrestoClient(config)
        _ = client.conn
        
        mock_auth.assert_called_once_with('authuser', 'authpass')
        call_kwargs = mock_connect.call_args[1]
        assert call_kwargs['auth'] == mock_auth_instance
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_connection_close(self, mock_patch, mock_connect):
        """Test closing Presto connection."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        _ = client.conn
        
        client.close()
        
        mock_conn.close.assert_called_once()
        assert client._conn is None


class TestQueryExecution:
    """Test SQL query execution."""
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_execute_basic_query(self, mock_patch, mock_connect):
        """Test executing basic SQL query."""
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [(1, 'Alice'), (2, 'Bob')]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        results = client.execute("SELECT * FROM users")
        
        assert len(results) == 2
        assert results[0] == {'id': 1, 'name': 'Alice'}
        assert results[1] == {'id': 2, 'name': 'Bob'}
        mock_cursor.execute.assert_called_once()
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_execute_with_limit(self, mock_patch, mock_connect):
        """Test query execution with LIMIT clause."""
        mock_cursor = Mock()
        mock_cursor.description = [('value',)]
        mock_cursor.fetchall.return_value = [(1,), (2,), (3,)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        results = client.execute("SELECT value FROM table", limit=10)
        
        # Should add LIMIT to query
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert 'LIMIT 10' in executed_sql
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_execute_metadata_query_no_limit(self, mock_patch, mock_connect):
        """Test that LIMIT is not added to metadata queries."""
        mock_cursor = Mock()
        mock_cursor.description = [('catalog',)]
        mock_cursor.fetchall.return_value = [('hive',)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        # SHOW commands shouldn't get LIMIT added
        results = client.execute("SHOW CATALOGS", limit=10)
        
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert 'LIMIT' not in executed_sql


class TestSchemaExploration:
    """Test schema exploration methods."""
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_catalogs(self, mock_patch, mock_connect):
        """Test getting list of catalogs."""
        mock_cursor = Mock()
        mock_cursor.description = [('Catalog',)]
        mock_cursor.fetchall.return_value = [('hive',), ('iceberg',)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        catalogs = client.get_catalogs()
        
        assert catalogs == ['hive', 'iceberg']
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_schemas(self, mock_patch, mock_connect):
        """Test getting list of schemas."""
        mock_cursor = Mock()
        mock_cursor.description = [('Schema',)]
        mock_cursor.fetchall.return_value = [('default',), ('staging',)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        schemas = client.get_schemas('hive')
        
        assert schemas == ['default', 'staging']
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert 'SHOW SCHEMAS FROM hive' in executed_sql
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_tables(self, mock_patch, mock_connect):
        """Test getting list of tables."""
        mock_cursor = Mock()
        mock_cursor.description = [('Table',)]
        mock_cursor.fetchall.return_value = [('users',), ('orders',)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        tables = client.get_tables('hive', 'default')
        
        assert tables == ['users', 'orders']
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert 'SHOW TABLES FROM hive.default' in executed_sql
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_columns(self, mock_patch, mock_connect):
        """Test getting column information."""
        mock_cursor = Mock()
        mock_cursor.description = [('Column',), ('Type',)]
        mock_cursor.fetchall.return_value = [
            {'Column': 'id', 'Type': 'bigint'},
            {'Column': 'name', 'Type': 'varchar'}
        ]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        columns = client.get_columns('hive', 'default', 'users')
        
        assert len(columns) == 2
        assert columns[0]['name'] == 'id'
        assert columns[0]['type'] == 'bigint'
        assert columns[1]['name'] == 'name'
        assert columns[1]['type'] == 'varchar'


class TestDDLGeneration:
    """Test DDL generation for LLM context."""
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_table_ddl(self, mock_patch, mock_connect):
        """Test generating DDL for a single table."""
        mock_cursor = Mock()
        mock_cursor.description = [('Column',), ('Type',)]
        mock_cursor.fetchall.return_value = [
            {'Column': 'id', 'Type': 'bigint'},
            {'Column': 'email', 'Type': 'varchar'}
        ]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        ddl = client.get_table_ddl('hive', 'default', 'users')
        
        assert 'CREATE TABLE hive.default.users' in ddl
        assert 'id bigint' in ddl
        assert 'email varchar' in ddl
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_tables_ddl(self, mock_patch, mock_connect):
        """Test generating DDL for multiple tables."""
        mock_cursor = Mock()
        
        # Mock different responses for different tables
        def execute_side_effect(sql):
            if 'users' in sql:
                mock_cursor.fetchall.return_value = [
                    {'Column': 'id', 'Type': 'bigint'}
                ]
            elif 'orders' in sql:
                mock_cursor.fetchall.return_value = [
                    {'Column': 'order_id', 'Type': 'bigint'}
                ]
        
        mock_cursor.execute.side_effect = execute_side_effect
        mock_cursor.description = [('Column',), ('Type',)]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        ddl = client.get_tables_ddl('hive', 'default', ['users', 'orders'])
        
        assert 'users' in ddl
        assert 'orders' in ddl
    
    @patch('clip.presto_client.PrestoClient.get_tables')
    @patch('clip.presto_client.PrestoClient.get_table_ddl')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_schema_ddl(self, mock_patch, mock_get_table_ddl, mock_get_tables):
        """Test generating DDL for entire schema."""
        mock_get_tables.return_value = ['table1', 'table2']
        mock_get_table_ddl.side_effect = [
            'CREATE TABLE table1 (id bigint)',
            'CREATE TABLE table2 (id bigint)'
        ]
        
        config = Config()
        client = PrestoClient(config)
        
        ddl = client.get_schema_ddl('hive', 'default')
        
        assert 'Schema: hive.default' in ddl
        assert 'table1' in ddl
        assert 'table2' in ddl
        assert mock_get_tables.called
    
    @patch('clip.presto_client.PrestoClient.get_tables')
    @patch('clip.presto_client.PrestoClient.get_table_ddl')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_schema_ddl_limits_tables(self, mock_patch, mock_get_table_ddl, mock_get_tables):
        """Test that get_schema_ddl respects max_tables limit."""
        # Return 100 tables
        mock_get_tables.return_value = [f'table{i}' for i in range(100)]
        mock_get_table_ddl.return_value = 'CREATE TABLE ...'
        
        config = Config()
        client = PrestoClient(config)
        
        ddl = client.get_schema_ddl('hive', 'default', max_tables=10)
        
        # Should only call get_table_ddl for first 10 tables
        assert mock_get_table_ddl.call_count == 10


class TestSampleData:
    """Test sample data retrieval."""
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_get_sample_data(self, mock_patch, mock_connect):
        """Test getting sample data from a table."""
        mock_cursor = Mock()
        mock_cursor.description = [('id',), ('name',)]
        mock_cursor.fetchall.return_value = [
            (1, 'Alice'),
            (2, 'Bob'),
            (3, 'Charlie')
        ]
        
        mock_conn = Mock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        config = Config()
        client = PrestoClient(config)
        
        sample = client.get_sample_data('hive', 'default', 'users', limit=5)
        
        assert len(sample) == 3
        executed_sql = mock_cursor.execute.call_args[0][0]
        assert 'LIMIT 5' in executed_sql


class TestPrestoClientImport:
    """Test that PrestoClient can be imported from various locations."""
    
    def test_import_from_clip_presto_client(self):
        """Test importing from clip.presto_client module."""
        from clip.presto_client import PrestoClient
        assert PrestoClient is not None
    
    def test_import_from_clip_package(self):
        """Test importing from clip package __init__."""
        from clip import PrestoClient
        assert PrestoClient is not None


class TestPrestoClientModulePath:
    """Test that Presto client works correctly in new module structure."""
    
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_presto_client_uses_clip_config(self, mock_patch):
        """Test that PrestoClient uses clip.config.Config correctly."""
        from clip.config import Config
        from clip.presto_client import PrestoClient
        
        config = Config(presto_host='test-host')
        client = PrestoClient(config)
        
        assert client.config.presto_host == 'test-host'
    
    @patch('clip.presto_client.prestodb.dbapi.connect')
    @patch('clip.presto_client._patch_prestodb_ssl_and_cookie')
    def test_presto_client_connection_in_new_path(self, mock_patch, mock_connect):
        """Test that connection works with restructured imports."""
        mock_conn = Mock()
        mock_connect.return_value = mock_conn
        
        from clip.config import Config
        from clip.presto_client import PrestoClient
        
        config = Config()
        client = PrestoClient(config)
        conn = client.conn
        
        assert conn is not None
        mock_connect.assert_called_once()


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
