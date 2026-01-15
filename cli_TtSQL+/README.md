# Presto AI CLI

A command-line tool for generating SQL and PySpark code from natural language, with native Presto integration.

## Features

- 🔍 **Text-to-SQL**: Generate Presto SQL from natural language questions
- ⚡ **Text-to-PySpark**: Generate PySpark DataFrame code from descriptions
- 🔗 **Presto Integration**: Connect to Presto, explore schemas, execute queries
- 🤖 **Multi-LLM Support**: OpenAI, Anthropic Claude, Ollama, or custom endpoints
- 📓 **Jupyter Notebooks**: Export generated code directly to .ipynb files
- 📊 **Apache Superset**: Push queries to Superset SQL Lab
- 💬 **Interactive Chat**: Conversational interface for data exploration
- 📊 **Schema-Aware**: Automatically uses your database schema for accurate generation

## Installation

### Basic Install

```bash
pip install presto-ai-cli
```

### With LLM Provider Support

```bash
# OpenAI
pip install "presto-ai-cli[openai]"

# Anthropic Claude
pip install "presto-ai-cli[anthropic]"

# Ollama (local models)
pip install "presto-ai-cli[ollama]"

# Superset integration
pip install "presto-ai-cli[superset]"

# All features
pip install "presto-ai-cli[all]"
```

### From Source

```bash
git clone https://github.com/yourusername/presto-ai-cli.git
cd presto-ai-cli
pip install -e ".[all]"
```

## Quick Start

### 1. Configure Presto Connection

```bash
presto-ai config set presto_host your-presto-host.com
presto-ai config set presto_port 8080
presto-ai config set presto_catalog hive
presto-ai config set presto_schema my_database
```

### 2. Configure LLM Provider

**OpenAI:**
```bash
presto-ai config set llm_provider openai
presto-ai config set llm_model gpt-4o
presto-ai config set llm_api_key sk-your-api-key
```

**Anthropic Claude:**
```bash
presto-ai config set llm_provider anthropic
presto-ai config set llm_model claude-sonnet-4-20250514
presto-ai config set llm_api_key your-anthropic-key
```

**Ollama (Local):**
```bash
presto-ai config set llm_provider ollama
presto-ai config set llm_model llama3.2
```

**Custom Endpoint (e.g., Azure, vLLM, etc.):**
```bash
presto-ai config set llm_provider custom
presto-ai config set llm_api_base https://your-endpoint.com/v1
presto-ai config set llm_model your-model-name
presto-ai config set llm_api_key your-api-key
```

### 3. Configure Superset (Optional)

```bash
presto-ai config set superset_url https://your-superset.com
presto-ai config set superset_username admin
presto-ai config set superset_password yourpassword
presto-ai config set superset_database_id 1
```

Or use an API token:
```bash
presto-ai config set superset_token your-api-token
```

### 4. Verify Configuration

```bash
presto-ai config show
```

## Usage

### Generate SQL

```bash
# Simple query
presto-ai sql "Find the top 10 customers by total order value"

# Generate and execute
presto-ai sql "Count orders by status" --execute

# Use specific tables for context
presto-ai sql "Join orders and customers" --tables orders --tables customers

# Save to Jupyter notebook
presto-ai sql "Revenue by month" --notebook analysis.ipynb

# Push to Superset
presto-ai sql "Daily active users" --superset --superset-name "DAU Query"
```

### Generate PySpark Code

```bash
# Basic PySpark generation
presto-ai spark "Read the orders table, filter for orders > $100, group by customer"

# Include Presto as data source
presto-ai spark "ETL pipeline to aggregate daily sales" --presto-source

# Save as Python file
presto-ai spark "Dedup orders by order_id keeping latest" --output dedup_orders.py

# Save as Jupyter notebook
presto-ai spark "Build customer segmentation pipeline" --output pipeline.ipynb
```

### Ask Questions About Your Data

```bash
# Generates SQL, executes it, and explains results
presto-ai ask "What were our top selling products last month?"

# Save the full analysis as a notebook
presto-ai ask "Revenue trend over the past year" --notebook revenue_analysis.ipynb

# Also save to Superset
presto-ai ask "Customer churn rate by segment" --superset
```

### Explore Schema

```bash
# List catalogs
presto-ai schema catalogs

# List schemas in a catalog
presto-ai schema schemas --catalog hive

# List tables
presto-ai schema tables

# Show columns for a table
presto-ai schema columns orders

# Dump full schema (for debugging/context)
presto-ai schema dump --output schema.sql
```

### Execute Raw SQL

```bash
presto-ai query "SELECT count(*) FROM orders"
presto-ai query "SELECT * FROM customers LIMIT 10" --format json
presto-ai query "SELECT status, count(*) FROM orders GROUP BY status" --format csv
```

### Superset Integration

```bash
# Test connection
presto-ai superset test

# List available databases
presto-ai superset databases

# List saved queries
presto-ai superset queries

# Push a query to Superset
presto-ai superset push "My Query" "SELECT * FROM orders LIMIT 100"

# Execute SQL via Superset
presto-ai superset execute "SELECT count(*) FROM orders"

# Open SQL Lab in browser
presto-ai superset open
presto-ai superset open --query-id 42
```

### Interactive Chat Mode

```bash
presto-ai chat
```

In chat mode:
```
You > What tables do I have?
You > /tables
You > Show me orders from last week
You > /sql count orders by customer
You > /spark create a daily aggregation pipeline
You > /run
You > /quit
```

## Output Formats

### Jupyter Notebooks

Use the `--notebook` or `-n` flag to save output as a ready-to-run Jupyter notebook:

```bash
# SQL analysis notebook
presto-ai sql "Revenue by product" --notebook revenue.ipynb

# PySpark pipeline notebook
presto-ai spark "ETL pipeline" --output etl.ipynb

# Full analysis with results and explanation
presto-ai ask "Top customers" --notebook customers.ipynb
```

Notebooks include:
- Setup cells with imports and connections
- Your schema for reference
- Generated code
- Execution cells
- Placeholders for visualization

### Superset Integration

Push generated queries directly to Apache Superset:

```bash
# Save as a query in SQL Lab
presto-ai sql "Complex analytics query" --superset

# With a custom name
presto-ai sql "Query" --superset --superset-name "Weekly Report Query"
```

## Configuration Reference

| Key | Description | Default |
|-----|-------------|---------|
| `presto_host` | Presto coordinator hostname | `localhost` |
| `presto_port` | Presto coordinator port | `8080` |
| `presto_user` | Presto user | `$USER` |
| `presto_catalog` | Default catalog | `hive` |
| `presto_schema` | Default schema | `default` |
| `llm_provider` | LLM provider (`openai`, `anthropic`, `ollama`, `custom`) | `openai` |
| `llm_model` | Model name | `gpt-4o` |
| `llm_api_key` | API key for the provider | - |
| `llm_api_base` | Custom API endpoint URL | - |
| `superset_url` | Superset base URL | - |
| `superset_username` | Superset username | - |
| `superset_password` | Superset password | - |
| `superset_token` | Superset API token (alternative to user/pass) | - |
| `superset_database_id` | Default database ID in Superset | - |

Configuration is stored in `~/.config/presto-ai/config.json`

## Environment Variables

You can also set credentials via environment variables:

```bash
# LLM providers
export OPENAI_API_KEY=sk-...
export ANTHROPIC_API_KEY=sk-ant-...

# Superset
export SUPERSET_URL=https://superset.example.com
export SUPERSET_USERNAME=admin
export SUPERSET_PASSWORD=password
export SUPERSET_TOKEN=your-token
```

## Examples

### Text-to-SQL Examples

```bash
# Aggregation
presto-ai sql "Total revenue by product category for Q4 2024"

# Joins
presto-ai sql "List customers who ordered more than 5 times with their total spend"

# Window functions
presto-ai sql "Rank products by sales within each category"

# Date filtering
presto-ai sql "Orders from the last 30 days"
```

### Text-to-PySpark Examples

```bash
# Basic transformations
presto-ai spark "Read orders, add a column for order_year extracted from order_date"

# Aggregations
presto-ai spark "Group orders by customer_id, calculate total spend and order count"

# Complex pipelines
presto-ai spark "Build an ETL pipeline that reads orders, deduplicates by order_id, joins with customers, and writes partitioned parquet by date"

# With Presto source
presto-ai spark "Read from Presto orders table and write to S3 as parquet" --presto-source
```

### Notebook Workflow

```bash
# Generate and save analysis
presto-ai ask "What's our customer retention rate?" --notebook retention.ipynb

# Open in Jupyter
jupyter notebook retention.ipynb
```

### Superset Workflow

```bash
# Configure once
presto-ai config set superset_url https://superset.company.com
presto-ai config set superset_username analyst
presto-ai config set superset_password secret
presto-ai superset databases  # Find your database ID
presto-ai config set superset_database_id 3

# Push queries
presto-ai sql "Weekly sales report" --superset --superset-name "Weekly Sales"

# Open in browser
presto-ai superset open --query-id 42
```

## Tips for Better Results

1. **Be specific**: Instead of "show me sales", try "show total sales by product category for the last quarter"

2. **Reference tables**: The tool uses your schema, but mentioning specific tables helps

3. **Use the right command**: 
   - `sql` for database queries
   - `spark` for data pipelines and transformations
   - `ask` for quick answers with automatic execution

4. **Check your schema first**: Use `presto-ai schema tables` and `presto-ai schema columns <table>` to see what's available

5. **Use notebooks for exploration**: Save to .ipynb when you want to iterate on the generated code

## Troubleshooting

### Connection Issues

```bash
# Test Presto connection
presto-ai schema catalogs

# Test Superset connection
presto-ai superset test

# Check configuration
presto-ai config show
```

### LLM Issues

```bash
# Verify API key is set
presto-ai config show

# Try with a simple prompt first
presto-ai sql "SELECT 1"
```

## License

MIT License - see LICENSE file for details.
