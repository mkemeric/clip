"""
Jupyter notebook generation for presto-ai-cli.
"""

import json
from typing import List, Optional, Dict, Any
from pathlib import Path


def create_code_cell(source: str, execution_count: Optional[int] = None) -> Dict[str, Any]:
    """Create a code cell for a Jupyter notebook."""
    if isinstance(source, str):
        lines = source.split('\n')
        # Each line should end with \n except the last one
        formatted_lines = [line + '\n' for line in lines[:-1]]
        if lines[-1]:  # Add last line without newline if not empty
            formatted_lines.append(lines[-1])
        source_lines = formatted_lines
    else:
        source_lines = source
    
    return {
        "cell_type": "code",
        "execution_count": execution_count,
        "metadata": {},
        "outputs": [],
        "source": source_lines
    }


def create_markdown_cell(source: str) -> Dict[str, Any]:
    """Create a markdown cell for a Jupyter notebook."""
    lines = source.split('\n') if isinstance(source, str) else source
    # Ensure each line ends with \n except the last
    formatted_lines = [line + '\n' if i < len(lines) - 1 else line for i, line in enumerate(lines)]
    return {
        "cell_type": "markdown",
        "metadata": {},
        "source": formatted_lines
    }


def create_notebook(cells: List[Dict[str, Any]], kernel: str = "python3") -> Dict[str, Any]:
    """Create a Jupyter notebook structure."""
    return {
        "cells": cells,
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": kernel
            },
            "language_info": {
                "name": "python",
                "version": "3.10.0"
            }
        },
        "nbformat": 4,
        "nbformat_minor": 5
    }


def save_notebook(notebook: Dict[str, Any], path: str) -> None:
    """Save a notebook to a file."""
    Path(path).write_text(json.dumps(notebook, indent=1))


def generate_sql_notebook(
    prompt: str,
    sql: str,
    schema_context: str,
    presto_config: Optional[Dict[str, Any]] = None,
    results: Optional[List[Dict]] = None
) -> Dict[str, Any]:
    """Generate a notebook for SQL analysis."""
    
    cells = []
    
    # Title
    cells.append(create_markdown_cell(f"# SQL Analysis\n\n**Question:** {prompt}"))
    
    # Setup cell
    setup_code = """# Setup
import pandas as pd
from IPython.display import display, HTML

# Optional: Install presto-python-client if needed
# !pip install presto-python-client"""
    
    if presto_config:
        setup_code += f"""

# Presto connection
import prestodb

conn = prestodb.dbapi.connect(
    host='{presto_config.get("host", "localhost")}',
    port={presto_config.get("port", 8080)},
    user='{presto_config.get("user", "presto")}',
    catalog='{presto_config.get("catalog", "hive")}',
    schema='{presto_config.get("schema", "default")}',
)
cursor = conn.cursor()"""
    
    cells.append(create_code_cell(setup_code))
    
    # Schema context (collapsed markdown)
    cells.append(create_markdown_cell(f"## Schema Reference\n\n<details>\n<summary>Click to expand schema</summary>\n\n```sql\n{schema_context}\n```\n\n</details>"))
    
    # SQL Query
    cells.append(create_markdown_cell("## Generated SQL"))
    
    sql_code = f'''sql = """
{sql}
"""

print(sql)'''
    cells.append(create_code_cell(sql_code))
    
    # Execute query
    if presto_config:
        cells.append(create_markdown_cell("## Execute Query"))
        exec_code = """# Execute the query
cursor.execute(sql)
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()

# Convert to DataFrame
df = pd.DataFrame(rows, columns=columns)
display(df)"""
        cells.append(create_code_cell(exec_code))
    
    # Include sample results if provided
    if results:
        cells.append(create_markdown_cell("## Sample Results"))
        results_code = f"""# Sample results from generation
sample_data = {json.dumps(results[:10], indent=2, default=str)}

df_sample = pd.DataFrame(sample_data)
display(df_sample)"""
        cells.append(create_code_cell(results_code))
    
    # Analysis placeholder
    cells.append(create_markdown_cell("## Analysis\n\nAdd your analysis here..."))
    cells.append(create_code_cell("# Your analysis code here\n# df.describe()\n# df.groupby('column').agg(...)"))
    
    return create_notebook(cells)


def generate_pyspark_notebook(
    prompt: str,
    code: str,
    schema_context: str,
    presto_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Generate a notebook for PySpark code."""
    
    cells = []
    
    # Title
    cells.append(create_markdown_cell(f"# PySpark Pipeline\n\n**Task:** {prompt}"))
    
    # Spark setup
    setup_code = """# Spark Session Setup
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Create or get Spark session
spark = SparkSession.builder \\
    .appName("PrestoAI_Generated") \\
    .getOrCreate()

# Optional: Configure for your environment
# spark.conf.set("spark.sql.shuffle.partitions", "200")
"""
    cells.append(create_code_cell(setup_code))
    
    # Presto connector setup if needed
    if presto_config:
        presto_setup = f"""# Presto/JDBC Connection Setup
# Note: Requires Presto JDBC driver in Spark classpath

PRESTO_HOST = "{presto_config.get('host', 'localhost')}"
PRESTO_PORT = {presto_config.get('port', 8080)}
PRESTO_CATALOG = "{presto_config.get('catalog', 'hive')}"
PRESTO_SCHEMA = "{presto_config.get('schema', 'default')}"

jdbc_url = f"jdbc:presto://{{PRESTO_HOST}}:{{PRESTO_PORT}}/{{PRESTO_CATALOG}}/{{PRESTO_SCHEMA}}"

def read_presto_table(table_name):
    \"\"\"Read a table from Presto via JDBC.\"\"\"
    return spark.read \\
        .format("jdbc") \\
        .option("url", jdbc_url) \\
        .option("dbtable", table_name) \\
        .option("driver", "com.facebook.presto.jdbc.PrestoDriver") \\
        .load()
"""
        cells.append(create_code_cell(presto_setup))
    
    # Schema context
    cells.append(create_markdown_cell(f"## Schema Reference\n\n<details>\n<summary>Click to expand schema</summary>\n\n```sql\n{schema_context}\n```\n\n</details>"))
    
    # Generated code
    cells.append(create_markdown_cell("## Generated PySpark Code"))
    cells.append(create_code_cell(code))
    
    # Execution / Display
    cells.append(create_markdown_cell("## View Results"))
    view_code = """# Display results (uncomment as needed)
# df.show(20, truncate=False)
# df.printSchema()
# display(df.toPandas())  # For Databricks or Jupyter with pandas"""
    cells.append(create_code_cell(view_code))
    
    # Write output placeholder
    cells.append(create_markdown_cell("## Write Output"))
    write_code = """# Write output (uncomment and configure as needed)

# Parquet
# df.write.mode("overwrite").parquet("s3://bucket/path/output.parquet")

# Partitioned Parquet
# df.write.mode("overwrite").partitionBy("date").parquet("s3://bucket/path/")

# Delta Lake
# df.write.mode("overwrite").format("delta").save("s3://bucket/path/delta/")

# Hive Table
# df.write.mode("overwrite").saveAsTable("schema.table_name")
"""
    cells.append(create_code_cell(write_code))
    
    return create_notebook(cells)


def generate_analysis_notebook(
    question: str,
    sql: str,
    results: List[Dict],
    explanation: str,
    schema_context: str,
    presto_config: Optional[Dict[str, Any]] = None
) -> Dict[str, Any]:
    """Generate a complete analysis notebook with query, results, and visualization."""
    
    cells = []
    
    # Title
    cells.append(create_markdown_cell(f"# Data Analysis\n\n**Question:** {question}"))
    
    # Setup
    setup_code = """# Setup
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.display import display, Markdown

# Configure plotting
plt.style.use('seaborn-v0_8-whitegrid')
%matplotlib inline"""
    cells.append(create_code_cell(setup_code))
    
    # SQL and explanation
    cells.append(create_markdown_cell(f"## SQL Query\n\n```sql\n{sql}\n```"))
    cells.append(create_markdown_cell(f"## Answer\n\n{explanation}"))
    
    # Results as DataFrame
    cells.append(create_markdown_cell("## Results"))
    results_code = f"""# Query results
data = {json.dumps(results, indent=2, default=str)}

df = pd.DataFrame(data)
display(df)"""
    cells.append(create_code_cell(results_code))
    
    # Basic analysis
    cells.append(create_markdown_cell("## Quick Analysis"))
    analysis_code = """# Summary statistics
print("Shape:", df.shape)
print("\\nColumn types:")
print(df.dtypes)
print("\\nSummary:")
display(df.describe(include='all'))"""
    cells.append(create_code_cell(analysis_code))
    
    # Visualization placeholder
    cells.append(create_markdown_cell("## Visualization"))
    viz_code = """# Create visualizations (customize based on your data)

# Example: Bar chart (uncomment and modify)
# fig, ax = plt.subplots(figsize=(10, 6))
# df.plot(kind='bar', x='column_name', y='value_column', ax=ax)
# plt.title('Your Chart Title')
# plt.tight_layout()
# plt.show()

# Example: Line chart
# df.plot(kind='line', x='date_column', y='metric_column', figsize=(12, 6))

# Example: Distribution
# df['numeric_column'].hist(bins=30, figsize=(10, 6))
"""
    cells.append(create_code_cell(viz_code))
    
    # Re-run with Presto
    if presto_config:
        cells.append(create_markdown_cell("## Re-run Query (Live Data)"))
        presto_code = f"""# Connect to Presto for live data
import prestodb

conn = prestodb.dbapi.connect(
    host='{presto_config.get("host", "localhost")}',
    port={presto_config.get("port", 8080)},
    user='{presto_config.get("user", "presto")}',
    catalog='{presto_config.get("catalog", "hive")}',
    schema='{presto_config.get("schema", "default")}',
)

sql = \"\"\"
{sql}
\"\"\"

cursor = conn.cursor()
cursor.execute(sql)
columns = [desc[0] for desc in cursor.description]
rows = cursor.fetchall()
df_live = pd.DataFrame(rows, columns=columns)
display(df_live)"""
        cells.append(create_code_cell(presto_code))
    
    return create_notebook(cells)
