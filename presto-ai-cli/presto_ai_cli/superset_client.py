"""
Apache Superset API client for presto-ai-cli.

Supports:
- Creating and executing SQL Lab queries
- Creating saved queries
- Creating datasets from queries
- Basic chart creation
"""

import json
from typing import Optional, Dict, Any, List
from dataclasses import dataclass

try:
    import requests
except ImportError:
    requests = None


@dataclass
class SupersetConfig:
    """Superset connection configuration."""
    base_url: str
    username: Optional[str] = None
    password: Optional[str] = None
    access_token: Optional[str] = None
    database_id: Optional[int] = None  # Default database ID for queries
    
    def __post_init__(self):
        # Remove trailing slash from base_url
        self.base_url = self.base_url.rstrip('/')


class SupersetClient:
    """Client for interacting with Apache Superset API."""
    
    def __init__(self, config: SupersetConfig):
        if requests is None:
            raise ImportError("requests package required. Install with: pip install requests")
        
        self.config = config
        self.session = requests.Session()
        self._access_token = config.access_token
        self._refresh_token = None
        self._csrf_token = None
        
        if not self._access_token and config.username and config.password:
            self._login()
    
    def _login(self) -> None:
        """Authenticate with Superset and obtain access token."""
        login_url = f"{self.config.base_url}/api/v1/security/login"
        
        payload = {
            "username": self.config.username,
            "password": self.config.password,
            "provider": "db",
            "refresh": True
        }
        
        response = self.session.post(login_url, json=payload)
        response.raise_for_status()
        
        data = response.json()
        self._access_token = data.get("access_token")
        self._refresh_token = data.get("refresh_token")
        
        if not self._access_token:
            raise ValueError("Failed to obtain access token from Superset")
    
    def _get_csrf_token(self) -> str:
        """Get CSRF token for write operations."""
        if self._csrf_token:
            return self._csrf_token
        
        url = f"{self.config.base_url}/api/v1/security/csrf_token/"
        response = self._request("GET", url)
        self._csrf_token = response.get("result")
        return self._csrf_token
    
    def _request(
        self, 
        method: str, 
        url: str, 
        json_data: Optional[Dict] = None,
        params: Optional[Dict] = None,
        require_csrf: bool = False
    ) -> Dict[str, Any]:
        """Make an authenticated request to Superset API."""
        headers = {
            "Authorization": f"Bearer {self._access_token}",
            "Content-Type": "application/json",
        }
        
        if require_csrf:
            headers["X-CSRFToken"] = self._get_csrf_token()
        
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            json=json_data,
            params=params
        )
        
        response.raise_for_status()
        return response.json() if response.text else {}
    
    # =========================================================================
    # Database Operations
    # =========================================================================
    
    def list_databases(self) -> List[Dict[str, Any]]:
        """List available databases in Superset."""
        url = f"{self.config.base_url}/api/v1/database/"
        response = self._request("GET", url)
        return response.get("result", [])
    
    def get_database(self, database_id: int) -> Dict[str, Any]:
        """Get database details."""
        url = f"{self.config.base_url}/api/v1/database/{database_id}"
        response = self._request("GET", url)
        return response.get("result", {})
    
    def find_database_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Find a database by name."""
        databases = self.list_databases()
        for db in databases:
            if db.get("database_name", "").lower() == name.lower():
                return db
        return None
    
    # =========================================================================
    # SQL Lab Operations
    # =========================================================================
    
    def execute_sql(
        self, 
        sql: str, 
        database_id: Optional[int] = None,
        schema: Optional[str] = None,
        limit: int = 1000
    ) -> Dict[str, Any]:
        """Execute a SQL query in SQL Lab."""
        url = f"{self.config.base_url}/api/v1/sqllab/execute/"
        
        payload = {
            "database_id": database_id or self.config.database_id,
            "sql": sql,
            "runAsync": False,
            "select_as_cta": False,
            "ctas_method": "TABLE",
            "queryLimit": limit,
        }
        
        if schema:
            payload["schema"] = schema
        
        response = self._request("POST", url, json_data=payload, require_csrf=True)
        return response
    
    def get_query_results(self, query_id: str) -> Dict[str, Any]:
        """Get results for an async query."""
        url = f"{self.config.base_url}/api/v1/sqllab/results/"
        params = {"key": query_id}
        return self._request("GET", url, params=params)
    
    # =========================================================================
    # Saved Query Operations
    # =========================================================================
    
    def create_saved_query(
        self,
        label: str,
        sql: str,
        database_id: Optional[int] = None,
        schema: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a saved query in SQL Lab."""
        url = f"{self.config.base_url}/api/v1/saved_query/"
        
        payload = {
            "label": label,
            "sql": sql,
            "db_id": database_id or self.config.database_id,
        }
        
        if schema:
            payload["schema"] = schema
        if description:
            payload["description"] = description
        
        response = self._request("POST", url, json_data=payload, require_csrf=True)
        return response
    
    def list_saved_queries(self, page: int = 0, page_size: int = 100) -> List[Dict[str, Any]]:
        """List saved queries."""
        url = f"{self.config.base_url}/api/v1/saved_query/"
        params = {"page": page, "page_size": page_size}
        response = self._request("GET", url, params=params)
        return response.get("result", [])
    
    def get_saved_query(self, query_id: int) -> Dict[str, Any]:
        """Get a saved query by ID."""
        url = f"{self.config.base_url}/api/v1/saved_query/{query_id}"
        response = self._request("GET", url)
        return response.get("result", {})
    
    def update_saved_query(
        self,
        query_id: int,
        label: Optional[str] = None,
        sql: Optional[str] = None,
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """Update an existing saved query."""
        url = f"{self.config.base_url}/api/v1/saved_query/{query_id}"
        
        payload = {}
        if label:
            payload["label"] = label
        if sql:
            payload["sql"] = sql
        if description:
            payload["description"] = description
        
        response = self._request("PUT", url, json_data=payload, require_csrf=True)
        return response
    
    def delete_saved_query(self, query_id: int) -> None:
        """Delete a saved query."""
        url = f"{self.config.base_url}/api/v1/saved_query/{query_id}"
        self._request("DELETE", url, require_csrf=True)
    
    # =========================================================================
    # Dataset Operations
    # =========================================================================
    
    def create_dataset_from_sql(
        self,
        name: str,
        sql: str,
        database_id: Optional[int] = None,
        schema: Optional[str] = None
    ) -> Dict[str, Any]:
        """Create a virtual dataset from a SQL query."""
        url = f"{self.config.base_url}/api/v1/dataset/"
        
        payload = {
            "table_name": name,
            "sql": sql,
            "database": database_id or self.config.database_id,
            "is_sqllab_view": True,
        }
        
        if schema:
            payload["schema"] = schema
        
        response = self._request("POST", url, json_data=payload, require_csrf=True)
        return response
    
    def list_datasets(self, page: int = 0, page_size: int = 100) -> List[Dict[str, Any]]:
        """List datasets."""
        url = f"{self.config.base_url}/api/v1/dataset/"
        params = {"page": page, "page_size": page_size}
        response = self._request("GET", url, params=params)
        return response.get("result", [])
    
    # =========================================================================
    # Chart Operations (Basic)
    # =========================================================================
    
    def create_chart(
        self,
        slice_name: str,
        datasource_id: int,
        viz_type: str = "table",
        params: Optional[Dict] = None
    ) -> Dict[str, Any]:
        """Create a basic chart."""
        url = f"{self.config.base_url}/api/v1/chart/"
        
        payload = {
            "slice_name": slice_name,
            "datasource_id": datasource_id,
            "datasource_type": "table",
            "viz_type": viz_type,
            "params": json.dumps(params or {}),
        }
        
        response = self._request("POST", url, json_data=payload, require_csrf=True)
        return response
    
    # =========================================================================
    # Utility Methods
    # =========================================================================
    
    def test_connection(self) -> bool:
        """Test the Superset connection."""
        try:
            self.list_databases()
            return True
        except Exception:
            return False
    
    def get_sqllab_url(self, database_id: Optional[int] = None, sql: Optional[str] = None) -> str:
        """Generate a SQL Lab URL, optionally with pre-filled query."""
        db_id = database_id or self.config.database_id or 1
        url = f"{self.config.base_url}/sqllab?database_id={db_id}"
        
        # Note: SQL Lab doesn't directly support URL-based query injection
        # But we can link to it
        return url
    
    def get_saved_query_url(self, query_id: int) -> str:
        """Get URL to open a saved query in SQL Lab."""
        return f"{self.config.base_url}/sqllab?savedQueryId={query_id}"


def create_superset_client_from_config(
    base_url: str,
    username: Optional[str] = None,
    password: Optional[str] = None,
    access_token: Optional[str] = None,
    database_id: Optional[int] = None
) -> SupersetClient:
    """Factory function to create a Superset client."""
    config = SupersetConfig(
        base_url=base_url,
        username=username,
        password=password,
        access_token=access_token,
        database_id=database_id
    )
    return SupersetClient(config)
