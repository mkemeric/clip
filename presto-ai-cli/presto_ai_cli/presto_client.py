"""
Presto database client for presto-ai-cli.
"""

from typing import List, Dict, Any, Optional
import prestodb
from prestodb.dbapi import Connection

from .config import Config

# Flag to track if we've already patched prestodb
_PRESTODB_PATCHED = False
_PRESTODB_COOKIE = None


def _patch_prestodb_ssl_and_cookie(insecure: bool = True, cookie: Optional[str] = None):
    """Globally patch prestodb to disable SSL verification and add OAuth cookie.
    
    This modifies the prestodb.client.PrestoRequest class to use
    an unverified session for all HTTPS requests and adds OAuth cookie.
    """
    global _PRESTODB_PATCHED, _PRESTODB_COOKIE
    
    # Store cookie globally
    if cookie:
        _PRESTODB_COOKIE = cookie
    
    if _PRESTODB_PATCHED:
        return
    
    import ssl
    import urllib3
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
    
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.ssl_ import create_urllib3_context
    
    class SSLAdapter(HTTPAdapter):
        def init_poolmanager(self, *args, **kwargs):
            ctx = create_urllib3_context()
            ctx.check_hostname = False
            ctx.verify_mode = ssl.CERT_NONE
            kwargs['ssl_context'] = ctx
            return super().init_poolmanager(*args, **kwargs)
        
        def send(self, request, **kwargs):
            kwargs['verify'] = False
            return super().send(request, **kwargs)
    
    # Import the internal PrestoRequest class
    try:
        from prestodb.client import PrestoRequest
        
        # Save original __init__
        original_init = PrestoRequest.__init__
        
        def patched_init(self, *args, **kwargs):
            # Call original init
            original_init(self, *args, **kwargs)
            
            # Immediately patch the http session
            if hasattr(self, '_http_session') and self._http_session is not None:
                if insecure:
                    self._http_session.verify = False
                    self._http_session.mount('https://', SSLAdapter())
                
                # Add OAuth cookie if configured
                if _PRESTODB_COOKIE:
                    self._http_session.headers['Cookie'] = f'_oauth2_proxy={_PRESTODB_COOKIE}'
                
                # Add browser-like headers
                self._http_session.headers['User-Agent'] = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/143.0.0.0 Safari/537.36'
                self._http_session.headers['Accept'] = '*/*'
        
        # Replace the __init__ method
        PrestoRequest.__init__ = patched_init
        _PRESTODB_PATCHED = True
        
    except (ImportError, AttributeError):
        # If we can't patch, we'll try other methods
        pass


class PrestoClient:
    """Client for interacting with Presto."""
    
    def __init__(self, config: Config):
        self.config = config
        self._conn: Optional[Connection] = None
        
        # Patch prestodb globally if SSL verification should be disabled or cookie is needed
        if config.presto_insecure or config.presto_cookie:
            _patch_prestodb_ssl_and_cookie(
                insecure=config.presto_insecure,
                cookie=config.presto_cookie
            )
    
    @property
    def conn(self) -> Connection:
        """Get or create a Presto connection."""
        if self._conn is None:
            connect_kwargs = {
                'host': self.config.presto_host,
                'port': self.config.presto_port,
                'user': self.config.presto_user,
                'catalog': self.config.presto_catalog,
                'schema': self.config.presto_schema,
                'source': self.config.presto_source or 'presto-ai-cli',
            }
            
            # Add HTTPS if configured
            if self.config.presto_https:
                connect_kwargs['http_scheme'] = 'https'
            
            # Add basic auth if configured (alternative to cookie)
            if self.config.presto_password:
                connect_kwargs['auth'] = prestodb.auth.BasicAuthentication(
                    self.config.presto_user,
                    self.config.presto_password
                )
            
            # Create connection
            # SSL verification and OAuth cookie are handled by global patch
            self._conn = prestodb.dbapi.connect(**connect_kwargs)
        return self._conn
    
    def close(self):
        """Close the connection."""
        if self._conn:
            self._conn.close()
            self._conn = None
    
    def execute(self, sql: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Execute a SQL query and return results as list of dicts."""
        cursor = self.conn.cursor()
        
        # Add LIMIT if not present and limit is specified
        # But don't add LIMIT to SHOW/DESCRIBE/EXPLAIN commands
        sql_upper = sql.strip().upper()
        is_metadata_query = any(sql_upper.startswith(cmd) for cmd in ['SHOW', 'DESCRIBE', 'DESC', 'EXPLAIN'])
        
        if limit and 'LIMIT' not in sql_upper and not is_metadata_query:
            sql = f"{sql.rstrip().rstrip(';')} LIMIT {limit}"
        
        cursor.execute(sql)
        
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()
        
        return [dict(zip(columns, row)) for row in rows]
    
    def get_catalogs(self) -> List[str]:
        """Get list of available catalogs."""
        results = self.execute("SHOW CATALOGS")
        catalogs = []
        for r in results:
            # Try different possible column names
            if 'Catalog' in r:
                catalogs.append(r['Catalog'])
            elif 'catalog' in r:
                catalogs.append(r['catalog'])
            elif r:  # If dict is not empty, try first value
                catalogs.append(list(r.values())[0])
        return catalogs
    
    def get_schemas(self, catalog: str) -> List[str]:
        """Get list of schemas in a catalog."""
        results = self.execute(f"SHOW SCHEMAS FROM {catalog}")
        return [r.get('Schema', r.get('schema', list(r.values())[0])) for r in results]
    
    def get_tables(self, catalog: str, schema: str) -> List[str]:
        """Get list of tables in a schema."""
        results = self.execute(f"SHOW TABLES FROM {catalog}.{schema}")
        return [r.get('Table', r.get('table', list(r.values())[0])) for r in results]
    
    def get_columns(self, catalog: str, schema: str, table: str) -> List[Dict[str, str]]:
        """Get column information for a table."""
        results = self.execute(f"DESCRIBE {catalog}.{schema}.{table}")
        
        columns = []
        for r in results:
            # Handle different Presto versions' output formats
            if 'Column' in r:
                columns.append({'name': r['Column'], 'type': r.get('Type', 'unknown')})
            elif 'column' in r:
                columns.append({'name': r['column'], 'type': r.get('type', 'unknown')})
            else:
                vals = list(r.values())
                columns.append({'name': vals[0], 'type': vals[1] if len(vals) > 1 else 'unknown'})
        
        return columns
    
    def get_table_ddl(self, catalog: str, schema: str, table: str) -> str:
        """Generate DDL-like representation of a table for LLM context."""
        columns = self.get_columns(catalog, schema, table)
        
        col_defs = []
        for col in columns:
            col_defs.append(f"    {col['name']} {col['type']}")
        
        ddl = f"CREATE TABLE {catalog}.{schema}.{table} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);"
        
        return ddl
    
    def get_tables_ddl(self, catalog: str, schema: str, table_names: List[str]) -> str:
        """Get DDL for specific tables."""
        ddls = []
        for table in table_names:
            try:
                ddl = self.get_table_ddl(catalog, schema, table)
                ddls.append(ddl)
            except Exception as e:
                ddls.append(f"-- Error getting DDL for {table}: {e}")
        
        return "\n\n".join(ddls)
    
    def get_schema_ddl(self, catalog: str, schema: str, max_tables: int = 50) -> str:
        """Get DDL for all tables in a schema (for LLM context)."""
        tables = self.get_tables(catalog, schema)
        
        if len(tables) > max_tables:
            tables = tables[:max_tables]
        
        ddls = []
        ddls.append(f"-- Schema: {catalog}.{schema}")
        ddls.append(f"-- Tables: {len(tables)}")
        ddls.append("")
        
        for table in tables:
            try:
                ddl = self.get_table_ddl(catalog, schema, table)
                ddls.append(ddl)
            except Exception as e:
                ddls.append(f"-- Error getting DDL for {table}: {e}")
        
        return "\n\n".join(ddls)
    
    def get_sample_data(self, catalog: str, schema: str, table: str, limit: int = 5) -> List[Dict]:
        """Get sample data from a table."""
        return self.execute(f"SELECT * FROM {catalog}.{schema}.{table} LIMIT {limit}")
