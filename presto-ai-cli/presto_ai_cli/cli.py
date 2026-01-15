#!/usr/bin/env python3
"""
presto-ai-cli: A CLI tool for text-to-SQL and text-to-PySpark generation with Presto.
"""

import click
import os
import json
from pathlib import Path
from typing import Optional

from .config import Config, get_config_path
from .presto_client import PrestoClient
from .llm_client import LLMClient
from .notebook import (
    generate_sql_notebook, 
    generate_pyspark_notebook, 
    generate_analysis_notebook,
    save_notebook
)
from .superset_client import SupersetClient, SupersetConfig


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx):
    """Presto AI CLI - Generate SQL and PySpark from natural language."""
    ctx.ensure_object(dict)
    ctx.obj['config'] = Config.load()


# ============================================================================
# Configuration Commands
# ============================================================================

@cli.command()
@click.option('--method', type=click.Choice(['browser', 'manual']), default='browser', 
              help='Login method: browser (automated) or manual (copy cookie)')
@click.option('--timeout', default=120, help='Timeout in seconds for browser login')
@click.pass_context
def login(ctx, method, timeout):
    """Authenticate with Presto via OAuth2.
    
    Methods:
      browser - Opens browser, waits for login, extracts cookie automatically (requires selenium)
      manual  - Provides instructions to copy cookie from browser DevTools
    """
    config = ctx.obj['config']
    
    if not config.presto_host:
        click.echo("Error: presto_host not configured. Run: presto-ai config set presto_host <hostname>", err=True)
        return
    
    scheme = 'https' if config.presto_https else 'http'
    presto_port = config.presto_port or (443 if config.presto_https else 8080)
    if presto_port in (80, 443):
        base_url = f"{scheme}://{config.presto_host}"
    else:
        base_url = f"{scheme}://{config.presto_host}:{presto_port}"
    
    if method == 'manual':
        _login_manual(config, base_url)
    else:
        _login_browser(config, base_url, timeout)


def _login_manual(config, base_url):
    """Manual login with instructions."""
    import webbrowser
    
    login_url = f"{base_url}/ui/"
    
    click.echo("Manual OAuth Login")
    click.echo("=" * 40)
    click.echo("")
    click.echo("1. Opening browser to Presto UI...")
    webbrowser.open(login_url)
    click.echo("")
    click.echo("2. Complete the OAuth login in your browser")
    click.echo("")
    click.echo("3. After login, open DevTools (F12)")
    click.echo("   → Application tab → Cookies → " + config.presto_host)
    click.echo("   → Copy the '_oauth2_proxy' cookie value")
    click.echo("")
    
    cookie_value = click.prompt("4. Paste the cookie value here", hide_input=False)
    
    if cookie_value:
        config.set('presto_cookie', cookie_value.strip())
        click.echo("")
        click.echo("✓ Cookie saved!")
        click.echo("  Test with: presto-ai query \"SELECT 1\"")


def _login_browser(config, base_url, timeout):
    """Automated browser login using Selenium."""
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
    except ImportError:
        click.echo("Selenium not installed. Install with:")
        click.echo("  pip install selenium")
        click.echo("")
        click.echo("Or use manual method:")
        click.echo("  presto-ai login --method manual")
        return
    
    click.echo("Browser OAuth Login")
    click.echo("=" * 40)
    click.echo("")
    
    # Configure Chrome options
    options = Options()
    # Don't use headless - user needs to interact with OAuth
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1200,800')
    
    # Ignore SSL errors for self-signed certs
    if config.presto_insecure:
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-ssl-errors')
    
    try:
        click.echo("Starting Chrome browser...")
        driver = webdriver.Chrome(options=options)
    except Exception as e:
        click.echo(f"Error starting Chrome: {e}")
        click.echo("")
        click.echo("Make sure Chrome and ChromeDriver are installed.")
        click.echo("Or use manual method: presto-ai login --method manual")
        return
    
    try:
        import time
        login_url = f"{base_url}/ui/"
        click.echo(f"Opening: {login_url}")
        click.echo("")
        click.echo("Please complete the OAuth login in the browser window.")
        click.echo(f"Waiting up to {timeout} seconds...")
        click.echo("")
        
        driver.get(login_url)
        
        # Wait for successful login - indicated by presence of oauth2_proxy cookie
        start_time = time.time()
        cookie_value = None
        
        while time.time() - start_time < timeout:
            # Check if we have the oauth2_proxy cookie
            cookies = driver.get_cookies()
            for cookie in cookies:
                if cookie['name'] == '_oauth2_proxy':
                    cookie_value = cookie['value']
                    break
            
            if cookie_value:
                # Also verify we're on the Presto host (login complete)
                current_url = driver.current_url
                if config.presto_host in current_url:
                    break
                cookie_value = None  # Reset if we're still on OAuth provider
            
            time.sleep(1)
        
        if cookie_value:
            config.set('presto_cookie', cookie_value)
            click.echo("✓ Successfully authenticated!")
            click.echo("  Cookie saved to configuration.")
            click.echo("")
            click.echo("Test with: presto-ai query \"SELECT 1\"")
        else:
            click.echo("Timeout waiting for authentication.")
            click.echo("Try: presto-ai login --method manual")
    
    finally:
        driver.quit()


@cli.group()
def config():
    """Manage configuration settings."""
    pass


@config.command('set')
@click.argument('key')
@click.argument('value')
@click.pass_context
def config_set(ctx, key: str, value: str):
    """Set a configuration value.
    
    Keys: presto_host, presto_port, presto_user, presto_catalog, presto_schema,
          presto_https, presto_insecure, presto_password, presto_cookie, presto_source,
          llm_provider, llm_model, llm_api_key, llm_api_base,
          superset_url, superset_username, superset_password, superset_token, superset_database_id
    """
    cfg = ctx.obj['config']
    
    valid_keys = [
        'presto_host', 'presto_port', 'presto_user', 'presto_catalog', 'presto_schema',
        'presto_https', 'presto_insecure', 'presto_password', 'presto_cookie', 'presto_source',
        'llm_provider', 'llm_model', 'llm_api_key', 'llm_api_base',
        'superset_url', 'superset_username', 'superset_password', 'superset_token', 'superset_database_id'
    ]
    
    if key not in valid_keys:
        click.echo(f"Invalid key: {key}")
        click.echo(f"Valid keys: {', '.join(valid_keys)}")
        return
    
    # Type conversions
    if key in ['presto_port', 'superset_database_id']:
        value = int(value)
    elif key in ['presto_https', 'presto_insecure']:
        value = value.lower() in ('true', '1', 'yes', 'on')
    
    setattr(cfg, key, value)
    cfg.save()
    
    # Mask sensitive values in output
    if 'password' in key or 'api_key' in key or 'token' in key or 'cookie' in key:
        display_value = '***' + str(value)[-4:] if value else '(not set)'
    else:
        display_value = value
    click.echo(f"Set {key} = {display_value}")


@config.command('show')
@click.pass_context
def config_show(ctx):
    """Show current configuration."""
    cfg = ctx.obj['config']
    click.echo("\n=== Presto Configuration ===")
    click.echo(f"  Host:     {cfg.presto_host}")
    click.echo(f"  Port:     {cfg.presto_port}")
    click.echo(f"  HTTPS:    {cfg.presto_https}")
    click.echo(f"  Insecure: {cfg.presto_insecure}")
    click.echo(f"  User:     {cfg.presto_user}")
    pwd_display = '***' if cfg.presto_password else '(not set)'
    click.echo(f"  Password: {pwd_display}")
    cookie_display = '***' + cfg.presto_cookie[-8:] if cfg.presto_cookie else '(not set)'
    click.echo(f"  Cookie:   {cookie_display}")
    click.echo(f"  Source:   {cfg.presto_source or '(default: presto-ai-cli)'}")
    click.echo(f"  Catalog:  {cfg.presto_catalog}")
    click.echo(f"  Schema:   {cfg.presto_schema}")
    
    click.echo("\n=== LLM Configuration ===")
    click.echo(f"  Provider: {cfg.llm_provider}")
    click.echo(f"  Model:    {cfg.llm_model}")
    click.echo(f"  API Base: {cfg.llm_api_base or '(default)'}")
    api_key_display = '***' + cfg.llm_api_key[-4:] if cfg.llm_api_key else '(not set)'
    click.echo(f"  API Key:  {api_key_display}")
    
    click.echo("\n=== Superset Configuration ===")
    click.echo(f"  URL:         {cfg.superset_url or '(not set)'}")
    click.echo(f"  Username:    {cfg.superset_username or '(not set)'}")
    pwd_display = '***' if cfg.superset_password else '(not set)'
    click.echo(f"  Password:    {pwd_display}")
    token_display = '***' + cfg.superset_token[-4:] if cfg.superset_token else '(not set)'
    click.echo(f"  Token:       {token_display}")
    click.echo(f"  Database ID: {cfg.superset_database_id or '(not set)'}")
    click.echo()


@config.command('path')
def config_path():
    """Show configuration file path."""
    click.echo(get_config_path())


# ============================================================================
# Schema Commands
# ============================================================================

@cli.group()
def schema():
    """Explore Presto schema."""
    pass


@schema.command('catalogs')
@click.pass_context
def schema_catalogs(ctx):
    """List available catalogs."""
    cfg = ctx.obj['config']
    
    try:
        click.echo(f"\nConnecting to Presto: {cfg.presto_host}:{cfg.presto_port}")
        click.echo(f"Using HTTPS: {cfg.presto_https}, Insecure: {cfg.presto_insecure}")
        click.echo(f"Cookie configured: {bool(cfg.presto_cookie)}\n")
        
        client = PrestoClient(cfg)
        
        # Debug: show raw query results
        raw_results = client.execute("SHOW CATALOGS")
        click.echo(f"Debug - Raw results from SHOW CATALOGS: {raw_results}\n")
        
        catalogs = client.get_catalogs()
        
        click.echo("Available Catalogs:")
        for cat in catalogs:
            click.echo(f"  - {cat}")
    except Exception as e:
        import traceback
        click.echo(f"\nError: {e}", err=True)
        click.echo(f"\nFull traceback:", err=True)
        traceback.print_exc()


@schema.command('schemas')
@click.option('--catalog', '-c', help='Catalog name (uses config default if not specified)')
@click.pass_context
def schema_schemas(ctx, catalog: Optional[str]):
    """List schemas in a catalog."""
    cfg = ctx.obj['config']
    client = PrestoClient(cfg)
    catalog = catalog or cfg.presto_catalog
    
    try:
        schemas = client.get_schemas(catalog)
        click.echo(f"\nSchemas in {catalog}:")
        for s in schemas:
            click.echo(f"  - {s}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@schema.command('tables')
@click.option('--catalog', '-c', help='Catalog name')
@click.option('--schema', '-s', 'schema_name', help='Schema name')
@click.pass_context
def schema_tables(ctx, catalog: Optional[str], schema_name: Optional[str]):
    """List tables in a schema."""
    cfg = ctx.obj['config']
    client = PrestoClient(cfg)
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    try:
        tables = client.get_tables(catalog, schema_name)
        click.echo(f"\nTables in {catalog}.{schema_name}:")
        for t in tables:
            click.echo(f"  - {t}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@schema.command('columns')
@click.argument('table')
@click.option('--catalog', '-c', help='Catalog name')
@click.option('--schema', '-s', 'schema_name', help='Schema name')
@click.pass_context
def schema_columns(ctx, table: str, catalog: Optional[str], schema_name: Optional[str]):
    """Show columns for a table."""
    cfg = ctx.obj['config']
    client = PrestoClient(cfg)
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    try:
        columns = client.get_columns(catalog, schema_name, table)
        click.echo(f"\nColumns in {catalog}.{schema_name}.{table}:")
        for col in columns:
            click.echo(f"  {col['name']:30} {col['type']}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@schema.command('dump')
@click.option('--catalog', '-c', help='Catalog name')
@click.option('--schema', '-s', 'schema_name', help='Schema name')
@click.option('--output', '-o', type=click.Path(), help='Output file (default: stdout)')
@click.pass_context
def schema_dump(ctx, catalog: Optional[str], schema_name: Optional[str], output: Optional[str]):
    """Dump full schema as DDL for LLM context."""
    cfg = ctx.obj['config']
    client = PrestoClient(cfg)
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    try:
        ddl = client.get_schema_ddl(catalog, schema_name)
        
        if output:
            Path(output).write_text(ddl)
            click.echo(f"Schema written to {output}")
        else:
            click.echo(ddl)
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


# ============================================================================
# Query Commands
# ============================================================================

@cli.command('query')
@click.argument('sql')
@click.option('--limit', '-l', default=100, help='Limit rows returned')
@click.option('--format', '-f', 'output_format', type=click.Choice(['table', 'json', 'csv']), default='table')
@click.pass_context
def query(ctx, sql: str, limit: int, output_format: str):
    """Execute a SQL query against Presto."""
    cfg = ctx.obj['config']
    client = PrestoClient(cfg)
    
    try:
        results = client.execute(sql, limit=limit)
        
        if output_format == 'json':
            click.echo(json.dumps(results, indent=2, default=str))
        elif output_format == 'csv':
            if results:
                click.echo(','.join(results[0].keys()))
                for row in results:
                    click.echo(','.join(str(v) for v in row.values()))
        else:  # table
            if results:
                headers = list(results[0].keys())
                click.echo(' | '.join(f"{h:20}" for h in headers))
                click.echo('-' * (22 * len(headers)))
                for row in results:
                    click.echo(' | '.join(f"{str(v):20}" for v in row.values()))
            click.echo(f"\n({len(results)} rows)")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


# ============================================================================
# AI Generation Commands
# ============================================================================

@cli.command('sql')
@click.argument('prompt')
@click.option('--execute', '-e', is_flag=True, help='Execute the generated SQL')
@click.option('--catalog', '-c', help='Catalog name for schema context')
@click.option('--schema', '-s', 'schema_name', help='Schema name for schema context')
@click.option('--tables', '-t', multiple=True, help='Specific tables to include in context')
@click.option('--notebook', '-n', type=click.Path(), help='Save as Jupyter notebook (.ipynb)')
@click.option('--superset', is_flag=True, help='Save as query in Apache Superset')
@click.option('--superset-name', help='Name for saved Superset query')
@click.pass_context
def generate_sql(ctx, prompt: str, execute: bool, catalog: Optional[str], 
                 schema_name: Optional[str], tables: tuple, notebook: Optional[str],
                 superset: bool, superset_name: Optional[str]):
    """Generate SQL from natural language.
    
    Example: presto-ai sql "Find top 10 customers by total orders"
    """
    cfg = ctx.obj['config']
    presto = PrestoClient(cfg)
    llm = LLMClient(cfg)
    
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    try:
        # Get schema context
        if tables:
            schema_context = presto.get_tables_ddl(catalog, schema_name, list(tables))
        else:
            schema_context = presto.get_schema_ddl(catalog, schema_name)
        
        # Generate SQL
        sql = llm.generate_sql(prompt, schema_context, dialect='presto')
        
        click.echo("\n=== Generated SQL ===\n")
        click.echo(sql)
        click.echo()
        
        results = None
        if execute:
            click.echo("=== Results ===\n")
            results = presto.execute(sql)
            if results:
                headers = list(results[0].keys())
                click.echo(' | '.join(f"{h:20}" for h in headers))
                click.echo('-' * (22 * len(headers)))
                for row in results[:20]:
                    click.echo(' | '.join(f"{str(v):20}" for v in row.values()))
                if len(results) > 20:
                    click.echo(f"... ({len(results)} total rows)")
        
        # Save to notebook if requested
        if notebook:
            presto_config = {
                'host': cfg.presto_host,
                'port': cfg.presto_port,
                'user': cfg.presto_user,
                'catalog': catalog,
                'schema': schema_name
            }
            nb = generate_sql_notebook(prompt, sql, schema_context, presto_config, results)
            save_notebook(nb, notebook)
            click.echo(f"Notebook saved to: {notebook}")
        
        # Push to Superset if requested
        if superset:
            if not cfg.superset_url:
                click.echo("Error: Superset not configured. Run: presto-ai config set superset_url <url>", err=True)
                return
            
            superset_cfg = SupersetConfig(
                base_url=cfg.superset_url,
                username=cfg.superset_username,
                password=cfg.superset_password,
                access_token=cfg.superset_token,
                database_id=cfg.superset_database_id
            )
            ss = SupersetClient(superset_cfg)
            
            query_name = superset_name or f"AI Generated: {prompt[:50]}"
            result = ss.create_saved_query(
                label=query_name,
                sql=sql,
                schema=schema_name,
                description=f"Generated from: {prompt}"
            )
            
            query_id = result.get('id')
            click.echo(f"Saved to Superset: {query_name}")
            if query_id:
                click.echo(f"Open in SQL Lab: {ss.get_saved_query_url(query_id)}")
                
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.command('spark')
@click.argument('prompt')
@click.option('--catalog', '-c', help='Catalog name for schema context')
@click.option('--schema', '-s', 'schema_name', help='Schema name for schema context')
@click.option('--tables', '-t', multiple=True, help='Specific tables to include in context')
@click.option('--output', '-o', type=click.Path(), help='Save to file (.py or .ipynb)')
@click.option('--presto-source', is_flag=True, help='Include Presto as data source in generated code')
@click.pass_context
def generate_spark(ctx, prompt: str, catalog: Optional[str], schema_name: Optional[str], 
                   tables: tuple, output: Optional[str], presto_source: bool):
    """Generate PySpark code from natural language.
    
    Example: presto-ai spark "Read orders, group by customer, calculate total spend"
    
    Use .ipynb extension for --output to generate a Jupyter notebook.
    """
    cfg = ctx.obj['config']
    presto = PrestoClient(cfg)
    llm = LLMClient(cfg)
    
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    try:
        # Get schema context
        if tables:
            schema_context = presto.get_tables_ddl(catalog, schema_name, list(tables))
        else:
            schema_context = presto.get_schema_ddl(catalog, schema_name)
        
        # Build Presto connection info if requested
        presto_config = None
        if presto_source:
            presto_config = {
                'host': cfg.presto_host,
                'port': cfg.presto_port,
                'catalog': catalog,
                'schema': schema_name
            }
        
        # Generate PySpark code
        code = llm.generate_pyspark(prompt, schema_context, presto_config=presto_config)
        
        click.echo("\n=== Generated PySpark Code ===\n")
        click.echo(code)
        click.echo()
        
        if output:
            from pathlib import Path as P
            output_path = P(output)
            
            if output_path.suffix.lower() == '.ipynb':
                # Generate notebook
                nb = generate_pyspark_notebook(
                    prompt, 
                    code, 
                    schema_context, 
                    presto_config if presto_source else None
                )
                save_notebook(nb, output)
                click.echo(f"Notebook saved to: {output}")
            else:
                # Save as plain Python
                output_path.write_text(code)
                click.echo(f"Code saved to: {output}")
                
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@cli.command('ask')
@click.argument('question')
@click.option('--catalog', '-c', help='Catalog name for schema context')
@click.option('--schema', '-s', 'schema_name', help='Schema name for schema context')
@click.option('--notebook', '-n', type=click.Path(), help='Save analysis as Jupyter notebook (.ipynb)')
@click.option('--superset', is_flag=True, help='Save query to Apache Superset')
@click.pass_context
def ask(ctx, question: str, catalog: Optional[str], schema_name: Optional[str],
        notebook: Optional[str], superset: bool):
    """Ask a question about your data (generates and executes SQL, explains results).
    
    Example: presto-ai ask "What were the top selling products last month?"
    """
    cfg = ctx.obj['config']
    presto = PrestoClient(cfg)
    llm = LLMClient(cfg)
    
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    try:
        # Get schema context
        schema_context = presto.get_schema_ddl(catalog, schema_name)
        
        # Generate SQL
        sql = llm.generate_sql(question, schema_context, dialect='presto')
        
        click.echo("\n=== Generated SQL ===\n")
        click.echo(sql)
        
        # Execute
        click.echo("\n=== Executing... ===\n")
        results = presto.execute(sql)
        
        if results:
            headers = list(results[0].keys())
            click.echo(' | '.join(f"{h:20}" for h in headers))
            click.echo('-' * (22 * len(headers)))
            for row in results[:10]:
                click.echo(' | '.join(f"{str(v):20}" for v in row.values()))
            if len(results) > 10:
                click.echo(f"... ({len(results)} total rows)")
        
        # Get explanation
        click.echo("\n=== Answer ===\n")
        explanation = llm.explain_results(question, sql, results[:10])
        click.echo(explanation)
        
        # Save to notebook if requested
        if notebook:
            presto_config = {
                'host': cfg.presto_host,
                'port': cfg.presto_port,
                'user': cfg.presto_user,
                'catalog': catalog,
                'schema': schema_name
            }
            nb = generate_analysis_notebook(
                question, sql, results, explanation, schema_context, presto_config
            )
            save_notebook(nb, notebook)
            click.echo(f"\nNotebook saved to: {notebook}")
        
        # Push to Superset if requested
        if superset:
            if not cfg.superset_url:
                click.echo("\nError: Superset not configured. Run: presto-ai config set superset_url <url>", err=True)
                return
            
            superset_cfg = SupersetConfig(
                base_url=cfg.superset_url,
                username=cfg.superset_username,
                password=cfg.superset_password,
                access_token=cfg.superset_token,
                database_id=cfg.superset_database_id
            )
            ss = SupersetClient(superset_cfg)
            
            query_name = f"AI: {question[:50]}"
            result = ss.create_saved_query(
                label=query_name,
                sql=sql,
                schema=schema_name,
                description=f"Question: {question}\n\nAnswer: {explanation}"
            )
            
            query_id = result.get('id')
            click.echo(f"\nSaved to Superset: {query_name}")
            if query_id:
                click.echo(f"Open in SQL Lab: {ss.get_saved_query_url(query_id)}")
        
    except Exception as e:
        import traceback
        click.echo(f"Error: {e}", err=True)
        if cfg.llm_provider and not cfg.llm_api_key and cfg.llm_provider != 'ollama':
            click.echo(f"\nHint: LLM API key not configured. Run: presto-ai config set llm_api_key YOUR_KEY", err=True)
        # Uncomment below for full traceback during debugging:
        # traceback.print_exc()


# ============================================================================
# Interactive Mode
# ============================================================================

@cli.command('chat')
@click.option('--catalog', '-c', help='Catalog name')
@click.option('--schema', '-s', 'schema_name', help='Schema name')
@click.pass_context
def chat(ctx, catalog: Optional[str], schema_name: Optional[str]):
    """Start an interactive chat session.
    
    Commands in chat:
      /sql <prompt>    - Generate SQL only
      /spark <prompt>  - Generate PySpark code
      /run             - Execute last generated SQL
      /tables          - List available tables
      /quit            - Exit chat
    """
    cfg = ctx.obj['config']
    presto = PrestoClient(cfg)
    llm = LLMClient(cfg)
    
    catalog = catalog or cfg.presto_catalog
    schema_name = schema_name or cfg.presto_schema
    
    click.echo("\n🚀 Presto AI Chat")
    click.echo(f"   Connected to: {cfg.presto_host}:{cfg.presto_port}")
    click.echo(f"   Context: {catalog}.{schema_name}")
    click.echo("   Type /help for commands, /quit to exit\n")
    
    # Load schema context
    try:
        schema_context = presto.get_schema_ddl(catalog, schema_name)
    except Exception as e:
        click.echo(f"Warning: Could not load schema: {e}")
        schema_context = ""
    
    last_sql = None
    
    while True:
        try:
            user_input = click.prompt('You', prompt_suffix=' > ').strip()
        except (EOFError, KeyboardInterrupt):
            click.echo("\nGoodbye!")
            break
        
        if not user_input:
            continue
        
        if user_input.lower() in ['/quit', '/exit', '/q']:
            click.echo("Goodbye!")
            break
        
        if user_input.lower() == '/help':
            click.echo("""
Commands:
  /sql <prompt>    - Generate SQL only (don't execute)
  /spark <prompt>  - Generate PySpark code
  /run             - Execute last generated SQL
  /tables          - List available tables  
  /schema <table>  - Show table schema
  /quit            - Exit chat
  
Or just type a question to generate, execute SQL, and get an answer.
""")
            continue
        
        if user_input.lower() == '/tables':
            try:
                tables = presto.get_tables(catalog, schema_name)
                click.echo("\nAvailable tables:")
                for t in tables:
                    click.echo(f"  - {t}")
                click.echo()
            except Exception as e:
                click.echo(f"Error: {e}")
            continue
        
        if user_input.lower().startswith('/schema '):
            table = user_input[8:].strip()
            try:
                columns = presto.get_columns(catalog, schema_name, table)
                click.echo(f"\n{table}:")
                for col in columns:
                    click.echo(f"  {col['name']:30} {col['type']}")
                click.echo()
            except Exception as e:
                click.echo(f"Error: {e}")
            continue
        
        if user_input.lower() == '/run':
            if last_sql:
                try:
                    results = presto.execute(last_sql)
                    if results:
                        headers = list(results[0].keys())
                        click.echo(' | '.join(f"{h:20}" for h in headers))
                        click.echo('-' * (22 * len(headers)))
                        for row in results[:20]:
                            click.echo(' | '.join(f"{str(v):20}" for v in row.values()))
                except Exception as e:
                    click.echo(f"Error: {e}")
            else:
                click.echo("No SQL generated yet.")
            continue
        
        if user_input.lower().startswith('/sql '):
            prompt = user_input[5:].strip()
            try:
                sql = llm.generate_sql(prompt, schema_context, dialect='presto')
                last_sql = sql
                click.echo(f"\n{sql}\n")
            except Exception as e:
                click.echo(f"Error: {e}")
            continue
        
        if user_input.lower().startswith('/spark '):
            prompt = user_input[7:].strip()
            try:
                code = llm.generate_pyspark(prompt, schema_context)
                click.echo(f"\n{code}\n")
            except Exception as e:
                click.echo(f"Error: {e}")
            continue
        
        # Default: generate SQL, execute, and explain
        try:
            sql = llm.generate_sql(user_input, schema_context, dialect='presto')
            last_sql = sql
            click.echo(f"\n📝 SQL:\n{sql}\n")
            
            results = presto.execute(sql)
            if results:
                headers = list(results[0].keys())
                click.echo(' | '.join(f"{h:15}" for h in headers[:5]))
                click.echo('-' * (17 * min(5, len(headers))))
                for row in results[:5]:
                    vals = list(row.values())[:5]
                    click.echo(' | '.join(f"{str(v):15}" for v in vals))
                if len(results) > 5:
                    click.echo(f"... ({len(results)} rows)")
            
            explanation = llm.explain_results(user_input, sql, results[:10])
            click.echo(f"\n💡 {explanation}\n")
            
        except Exception as e:
            click.echo(f"Error: {e}")


# ============================================================================
# Superset Commands
# ============================================================================

@cli.group()
def superset():
    """Apache Superset integration commands."""
    pass


@superset.command('test')
@click.pass_context
def superset_test(ctx):
    """Test Superset connection."""
    cfg = ctx.obj['config']
    
    if not cfg.superset_url:
        click.echo("Error: Superset URL not configured.")
        click.echo("Run: presto-ai config set superset_url <url>")
        return
    
    try:
        superset_cfg = SupersetConfig(
            base_url=cfg.superset_url,
            username=cfg.superset_username,
            password=cfg.superset_password,
            access_token=cfg.superset_token,
            database_id=cfg.superset_database_id
        )
        ss = SupersetClient(superset_cfg)
        
        if ss.test_connection():
            click.echo("✓ Successfully connected to Superset!")
            dbs = ss.list_databases()
            click.echo(f"  Found {len(dbs)} database(s)")
        else:
            click.echo("✗ Connection failed")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@superset.command('databases')
@click.pass_context
def superset_databases(ctx):
    """List available databases in Superset."""
    cfg = ctx.obj['config']
    
    try:
        superset_cfg = SupersetConfig(
            base_url=cfg.superset_url,
            username=cfg.superset_username,
            password=cfg.superset_password,
            access_token=cfg.superset_token
        )
        ss = SupersetClient(superset_cfg)
        
        databases = ss.list_databases()
        click.echo("\nAvailable Databases:")
        for db in databases:
            marker = " *" if db.get('id') == cfg.superset_database_id else ""
            click.echo(f"  [{db.get('id')}] {db.get('database_name')}{marker}")
        click.echo("\n  * = configured default")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@superset.command('queries')
@click.option('--limit', '-l', default=20, help='Number of queries to show')
@click.pass_context
def superset_queries(ctx, limit: int):
    """List saved queries in Superset."""
    cfg = ctx.obj['config']
    
    try:
        superset_cfg = SupersetConfig(
            base_url=cfg.superset_url,
            username=cfg.superset_username,
            password=cfg.superset_password,
            access_token=cfg.superset_token
        )
        ss = SupersetClient(superset_cfg)
        
        queries = ss.list_saved_queries(page_size=limit)
        click.echo("\nSaved Queries:")
        for q in queries:
            click.echo(f"  [{q.get('id')}] {q.get('label')}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@superset.command('push')
@click.argument('name')
@click.argument('sql')
@click.option('--schema', '-s', help='Schema name')
@click.option('--description', '-d', help='Query description')
@click.pass_context
def superset_push(ctx, name: str, sql: str, schema: Optional[str], description: Optional[str]):
    """Push a SQL query to Superset as a saved query.
    
    Example: presto-ai superset push "My Query" "SELECT * FROM orders LIMIT 10"
    """
    cfg = ctx.obj['config']
    
    try:
        superset_cfg = SupersetConfig(
            base_url=cfg.superset_url,
            username=cfg.superset_username,
            password=cfg.superset_password,
            access_token=cfg.superset_token,
            database_id=cfg.superset_database_id
        )
        ss = SupersetClient(superset_cfg)
        
        result = ss.create_saved_query(
            label=name,
            sql=sql,
            schema=schema or cfg.presto_schema,
            description=description
        )
        
        query_id = result.get('id')
        click.echo(f"Created saved query: {name}")
        if query_id:
            click.echo(f"Open in SQL Lab: {ss.get_saved_query_url(query_id)}")
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@superset.command('execute')
@click.argument('sql')
@click.option('--schema', '-s', help='Schema name')
@click.option('--limit', '-l', default=100, help='Row limit')
@click.pass_context
def superset_execute(ctx, sql: str, schema: Optional[str], limit: int):
    """Execute a SQL query via Superset SQL Lab API.
    
    Example: presto-ai superset execute "SELECT count(*) FROM orders"
    """
    cfg = ctx.obj['config']
    
    try:
        superset_cfg = SupersetConfig(
            base_url=cfg.superset_url,
            username=cfg.superset_username,
            password=cfg.superset_password,
            access_token=cfg.superset_token,
            database_id=cfg.superset_database_id
        )
        ss = SupersetClient(superset_cfg)
        
        result = ss.execute_sql(
            sql=sql,
            schema=schema or cfg.presto_schema,
            limit=limit
        )
        
        # Display results
        data = result.get('data', [])
        columns = result.get('columns', [])
        
        if columns and data:
            col_names = [c.get('name', c.get('column_name', str(i))) for i, c in enumerate(columns)]
            click.echo(' | '.join(f"{h:20}" for h in col_names[:5]))
            click.echo('-' * (22 * min(5, len(col_names))))
            for row in data[:20]:
                click.echo(' | '.join(f"{str(v):20}" for v in row[:5]))
            if len(data) > 20:
                click.echo(f"... ({len(data)} rows)")
        else:
            click.echo("Query executed successfully (no results)")
            
    except Exception as e:
        click.echo(f"Error: {e}", err=True)


@superset.command('open')
@click.option('--query-id', '-q', type=int, help='Open a saved query by ID')
@click.pass_context
def superset_open(ctx, query_id: Optional[int]):
    """Open Superset SQL Lab in browser.
    
    Example: presto-ai superset open
    Example: presto-ai superset open --query-id 42
    """
    cfg = ctx.obj['config']
    
    if not cfg.superset_url:
        click.echo("Error: Superset URL not configured.")
        return
    
    superset_cfg = SupersetConfig(
        base_url=cfg.superset_url,
        database_id=cfg.superset_database_id
    )
    ss = SupersetClient.__new__(SupersetClient)
    ss.config = superset_cfg
    
    if query_id:
        url = ss.get_saved_query_url(query_id)
    else:
        url = ss.get_sqllab_url()
    
    click.echo(f"Opening: {url}")
    
    # Try to open in browser
    try:
        import webbrowser
        webbrowser.open(url)
    except Exception:
        click.echo("(Could not open browser automatically)")


def main():
    cli()


if __name__ == '__main__':
    main()
