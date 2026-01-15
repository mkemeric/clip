"""
LLM client for presto-ai-cli.

Supports multiple LLM providers: OpenAI, Anthropic, Ollama, and custom endpoints.
"""

import json
import re
from typing import Optional, Dict, List, Any
from abc import ABC, abstractmethod

from .config import Config


class BaseLLMProvider(ABC):
    """Base class for LLM providers."""
    
    @abstractmethod
    def complete(self, messages: List[Dict[str, str]], **kwargs) -> str:
        """Generate a completion from the LLM."""
        pass


class OpenAIProvider(BaseLLMProvider):
    """OpenAI API provider."""
    
    def __init__(self, api_key: str, model: str, api_base: Optional[str] = None):
        try:
            from openai import OpenAI
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai")
        
        kwargs = {'api_key': api_key}
        if api_base:
            kwargs['base_url'] = api_base
        
        self.client = OpenAI(**kwargs)
        self.model = model
    
    def complete(self, messages: List[Dict[str, str]], **kwargs) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=kwargs.get('temperature', 0),
            max_tokens=kwargs.get('max_tokens', 4096),
        )
        return response.choices[0].message.content


class AnthropicProvider(BaseLLMProvider):
    """Anthropic API provider."""
    
    def __init__(self, api_key: str, model: str, api_base: Optional[str] = None):
        try:
            from anthropic import Anthropic
        except ImportError:
            raise ImportError("anthropic package required. Install with: pip install anthropic")
        
        kwargs = {'api_key': api_key}
        if api_base:
            kwargs['base_url'] = api_base
        
        self.client = Anthropic(**kwargs)
        self.model = model
    
    def complete(self, messages: List[Dict[str, str]], **kwargs) -> str:
        # Extract system message if present
        system = ""
        chat_messages = []
        
        for msg in messages:
            if msg['role'] == 'system':
                system = msg['content']
            else:
                chat_messages.append(msg)
        
        response = self.client.messages.create(
            model=self.model,
            system=system,
            messages=chat_messages,
            temperature=kwargs.get('temperature', 0),
            max_tokens=kwargs.get('max_tokens', 4096),
        )
        return response.content[0].text


class OllamaProvider(BaseLLMProvider):
    """Ollama local provider."""
    
    def __init__(self, model: str, api_base: Optional[str] = None):
        try:
            import ollama
        except ImportError:
            raise ImportError("ollama package required. Install with: pip install ollama")
        
        self.client = ollama
        self.model = model
        self.host = api_base or 'http://localhost:11434'
    
    def complete(self, messages: List[Dict[str, str]], **kwargs) -> str:
        response = self.client.chat(
            model=self.model,
            messages=messages,
            options={'temperature': kwargs.get('temperature', 0)}
        )
        return response['message']['content']


class CustomProvider(BaseLLMProvider):
    """Custom OpenAI-compatible API provider."""
    
    def __init__(self, api_key: str, model: str, api_base: str, verify_ssl: bool = True):
        try:
            from openai import OpenAI
            import httpx
        except ImportError:
            raise ImportError("openai package required. Install with: pip install openai")
        
        # Create httpx client with SSL verification control
        if not verify_ssl:
            import ssl
            import urllib3
            urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
            
            # Create SSL context that doesn't verify
            ssl_context = ssl.create_default_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = ssl.CERT_NONE
            
            http_client = httpx.Client(verify=False)
            self.client = OpenAI(api_key=api_key, base_url=api_base, http_client=http_client)
        else:
            self.client = OpenAI(api_key=api_key, base_url=api_base)
        
        self.model = model
    
    def complete(self, messages: List[Dict[str, str]], **kwargs) -> str:
        response = self.client.chat.completions.create(
            model=self.model,
            messages=messages,
            temperature=kwargs.get('temperature', 0),
            max_tokens=kwargs.get('max_tokens', 4096),
        )
        return response.choices[0].message.content


class LLMClient:
    """High-level LLM client with SQL and PySpark generation capabilities."""
    
    def __init__(self, config: Config):
        self.config = config
        self.provider = self._create_provider()
    
    def _create_provider(self) -> BaseLLMProvider:
        """Create the appropriate LLM provider based on config."""
        provider_type = self.config.llm_provider.lower()
        
        if provider_type == 'openai':
            if not self.config.llm_api_key:
                raise ValueError("OpenAI API key required. Set with: presto-ai config set llm_api_key YOUR_KEY")
            return OpenAIProvider(
                api_key=self.config.llm_api_key,
                model=self.config.llm_model,
                api_base=self.config.llm_api_base
            )
        
        elif provider_type == 'anthropic':
            if not self.config.llm_api_key:
                raise ValueError("Anthropic API key required. Set with: presto-ai config set llm_api_key YOUR_KEY")
            return AnthropicProvider(
                api_key=self.config.llm_api_key,
                model=self.config.llm_model,
                api_base=self.config.llm_api_base
            )
        
        elif provider_type == 'ollama':
            return OllamaProvider(
                model=self.config.llm_model,
                api_base=self.config.llm_api_base
            )
        
        elif provider_type == 'custom':
            if not self.config.llm_api_base:
                raise ValueError("Custom API base URL required. Set with: presto-ai config set llm_api_base URL")
            return CustomProvider(
                api_key=self.config.llm_api_key or 'dummy',
                model=self.config.llm_model,
                api_base=self.config.llm_api_base,
                verify_ssl=not self.config.presto_insecure  # Reuse presto_insecure setting for LLM SSL
            )
        
        else:
            raise ValueError(f"Unknown LLM provider: {provider_type}")
    
    def _extract_code(self, response: str, language: str = 'sql') -> str:
        """Extract code from LLM response, handling markdown code blocks."""
        # Try to find code blocks
        patterns = [
            rf'```{language}\n?(.*?)```',
            rf'```\n?(.*?)```',
            r'```sql\n?(.*?)```',
            r'```python\n?(.*?)```',
        ]
        
        for pattern in patterns:
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                code = match.group(1).strip()
                # For SQL, remove trailing semicolons (Presto doesn't need them)
                if language == 'sql':
                    code = code.rstrip(';').strip()
                return code
        
        # If no code block found, return the whole response cleaned up
        code = response.strip()
        if language == 'sql':
            code = code.rstrip(';').strip()
        return code
    
    def generate_sql(self, prompt: str, schema_context: str, dialect: str = 'presto') -> str:
        """Generate SQL from natural language."""
        system_prompt = f"""You are an expert SQL developer specializing in {dialect.upper()} SQL. Generate syntactically correct {dialect.upper()} queries based on the user's request.

Critical {dialect.upper()} Syntax Rules:
- Always use proper column references in GROUP BY (use column names or ordinal positions)
- Date functions: use date_trunc(), date_add(), date_diff() (not DATE_SUB, DATEDIFF)
- String functions: use concat(), substr(), lower(), upper()
- Aggregations: count(), sum(), avg(), max(), min()
- Window functions: ROW_NUMBER() OVER (PARTITION BY ... ORDER BY ...)
- Use LIMIT not TOP for row limiting
- Cast types with CAST(column AS type) or column::type
- Current timestamp: current_timestamp or now()

Query Generation Rules:
- Output ONLY the SQL query, no explanations or markdown
- Use ONLY tables and columns from the provided schema
- Write efficient, well-formatted SQL
- Use appropriate JOINs when needed
- Always qualify column names with table aliases in multi-table queries
- Ensure GROUP BY includes all non-aggregated SELECT columns

Example Correct Queries:

Query: "Top 10 customers by order count"
SQL:
SELECT customer_id, COUNT(*) as order_count
FROM orders
GROUP BY customer_id
ORDER BY order_count DESC
LIMIT 10;

Query: "Revenue by product and month"
SQL:
SELECT 
    product_name,
    date_trunc('month', order_date) as month,
    SUM(amount) as total_revenue
FROM orders o
JOIN products p ON o.product_id = p.id
GROUP BY product_name, date_trunc('month', order_date)
ORDER BY month DESC, total_revenue DESC;

Available Schema:
{schema_context}
"""
        
        messages = [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt}
        ]
        
        response = self.provider.complete(messages, temperature=0)
        return self._extract_code(response, 'sql')
    
    def generate_pyspark(self, prompt: str, schema_context: str, 
                         presto_config: Optional[Dict[str, Any]] = None) -> str:
        """Generate PySpark code from natural language."""
        
        presto_snippet = ""
        if presto_config:
            presto_snippet = f"""
# If reading from Presto, use this configuration:
# presto_host = "{presto_config['host']}"
# presto_port = {presto_config['port']}
# presto_catalog = "{presto_config['catalog']}"
# presto_schema = "{presto_config['schema']}"
"""
        
        system_prompt = f"""You are an expert PySpark developer. Generate PySpark code based on the user's request.

Rules:
- Output ONLY executable Python/PySpark code
- Use 'spark' as the SparkSession variable name
- Include necessary imports
- Write efficient, well-documented code
- Use DataFrame API (not RDDs) unless specifically requested
- Add comments explaining complex transformations
{presto_snippet}

Available Schema:
{schema_context}
"""
        
        messages = [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': prompt}
        ]
        
        response = self.provider.complete(messages, temperature=0)
        return self._extract_code(response, 'python')
    
    def explain_results(self, question: str, sql: str, results: List[Dict]) -> str:
        """Generate a natural language explanation of query results."""
        
        # Limit results for context
        results_preview = json.dumps(results[:10], indent=2, default=str)
        
        system_prompt = """You are a helpful data analyst. Explain query results in plain English.

Rules:
- Be concise and direct
- Highlight key findings
- Use specific numbers from the results
- Keep explanation to 2-3 sentences
"""
        
        user_prompt = f"""Question: {question}

SQL Executed:
{sql}

Results (preview):
{results_preview}

Provide a brief, clear answer to the original question based on these results."""
        
        messages = [
            {'role': 'system', 'content': system_prompt},
            {'role': 'user', 'content': user_prompt}
        ]
        
        response = self.provider.complete(messages, temperature=0.3)
        return response.strip()
