"""
Unit tests for notebook creation and export.

Tests that notebook creation and export functions operate properly post-restructure.
"""

import pytest
import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from clip.notebook import (
    create_code_cell,
    create_markdown_cell,
    create_notebook,
    save_notebook,
    generate_sql_notebook,
    generate_pyspark_notebook,
    generate_analysis_notebook
)


class TestCellCreation:
    """Test individual cell creation functions."""
    
    def test_create_code_cell_basic(self):
        """Test basic code cell creation."""
        cell = create_code_cell("print('hello')")
        assert cell['cell_type'] == 'code'
        assert cell['execution_count'] is None
        assert 'print' in str(cell['source'])
    
    def test_create_code_cell_with_execution_count(self):
        """Test code cell with execution count."""
        cell = create_code_cell("x = 1", execution_count=5)
        assert cell['execution_count'] == 5
    
    def test_create_code_cell_multiline(self):
        """Test code cell with multiple lines."""
        code = "import pandas as pd\ndf = pd.DataFrame()\nprint(df)"
        cell = create_code_cell(code)
        assert cell['cell_type'] == 'code'
        assert len(cell['source']) > 1
    
    def test_create_markdown_cell_basic(self):
        """Test basic markdown cell creation."""
        cell = create_markdown_cell("# Title")
        assert cell['cell_type'] == 'markdown'
        assert '# Title' in str(cell['source'])
    
    def test_create_markdown_cell_multiline(self):
        """Test markdown cell with multiple lines."""
        markdown = "# Header\n\nSome text\n\n* List item"
        cell = create_markdown_cell(markdown)
        assert cell['cell_type'] == 'markdown'
        assert isinstance(cell['source'], list)


class TestNotebookCreation:
    """Test notebook structure creation."""
    
    def test_create_notebook_empty(self):
        """Test creating empty notebook."""
        nb = create_notebook([])
        assert 'cells' in nb
        assert 'metadata' in nb
        assert 'nbformat' in nb
        assert nb['nbformat'] == 4
    
    def test_create_notebook_with_cells(self):
        """Test creating notebook with cells."""
        cells = [
            create_code_cell("x = 1"),
            create_markdown_cell("# Title")
        ]
        nb = create_notebook(cells)
        assert len(nb['cells']) == 2
        assert nb['cells'][0]['cell_type'] == 'code'
        assert nb['cells'][1]['cell_type'] == 'markdown'
    
    def test_create_notebook_kernel_spec(self):
        """Test notebook has correct kernel specification."""
        nb = create_notebook([])
        assert 'kernelspec' in nb['metadata']
        assert nb['metadata']['kernelspec']['name'] == 'python3'
        assert nb['metadata']['kernelspec']['language'] == 'python'
    
    def test_create_notebook_custom_kernel(self):
        """Test creating notebook with custom kernel."""
        nb = create_notebook([], kernel='pyspark3')
        assert nb['metadata']['kernelspec']['name'] == 'pyspark3'


class TestNotebookSave:
    """Test notebook saving functionality."""
    
    def test_save_notebook_creates_file(self):
        """Test that save_notebook creates a file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nb_path = Path(tmpdir) / 'test.ipynb'
            nb = create_notebook([create_code_cell("print('test')")])
            
            save_notebook(nb, str(nb_path))
            
            assert nb_path.exists()
    
    def test_save_notebook_valid_json(self):
        """Test that saved notebook is valid JSON."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nb_path = Path(tmpdir) / 'test.ipynb'
            nb = create_notebook([create_code_cell("x = 1")])
            
            save_notebook(nb, str(nb_path))
            
            # Should be loadable as JSON
            with open(nb_path) as f:
                loaded = json.load(f)
            assert loaded['nbformat'] == 4
    
    def test_save_notebook_preserves_content(self):
        """Test that saving preserves notebook content."""
        with tempfile.TemporaryDirectory() as tmpdir:
            nb_path = Path(tmpdir) / 'test.ipynb'
            cells = [
                create_code_cell("import pandas as pd"),
                create_markdown_cell("# Analysis")
            ]
            nb = create_notebook(cells)
            
            save_notebook(nb, str(nb_path))
            
            with open(nb_path) as f:
                loaded = json.load(f)
            assert len(loaded['cells']) == 2
            assert 'pandas' in str(loaded['cells'][0]['source'])
            assert '# Analysis' in str(loaded['cells'][1]['source'])


class TestSQLNotebookGeneration:
    """Test SQL notebook generation."""
    
    def test_generate_sql_notebook_basic(self):
        """Test generating basic SQL notebook."""
        nb = generate_sql_notebook(
            prompt="Get all users",
            sql="SELECT * FROM users",
            schema_context="CREATE TABLE users (id INT, name VARCHAR)"
        )
        assert 'cells' in nb
        assert len(nb['cells']) > 0
        # Should contain SQL
        nb_str = json.dumps(nb)
        assert 'SELECT * FROM users' in nb_str
    
    def test_generate_sql_notebook_with_presto_config(self):
        """Test SQL notebook with Presto connection config."""
        presto_config = {
            'host': 'presto.example.com',
            'port': 8080,
            'user': 'testuser',
            'catalog': 'hive',
            'schema': 'default'
        }
        nb = generate_sql_notebook(
            prompt="Test query",
            sql="SELECT 1",
            schema_context="",
            presto_config=presto_config
        )
        nb_str = json.dumps(nb)
        assert 'presto.example.com' in nb_str
        assert 'prestodb' in nb_str
    
    def test_generate_sql_notebook_with_results(self):
        """Test SQL notebook includes sample results."""
        results = [
            {'id': 1, 'name': 'Alice'},
            {'id': 2, 'name': 'Bob'}
        ]
        nb = generate_sql_notebook(
            prompt="Get users",
            sql="SELECT * FROM users",
            schema_context="",
            results=results
        )
        nb_str = json.dumps(nb)
        assert 'Alice' in nb_str or 'sample' in nb_str.lower()
    
    def test_generate_sql_notebook_has_analysis_section(self):
        """Test SQL notebook includes analysis placeholder."""
        nb = generate_sql_notebook(
            prompt="Query",
            sql="SELECT 1",
            schema_context=""
        )
        nb_str = json.dumps(nb).lower()
        assert 'analysis' in nb_str


class TestPySparkNotebookGeneration:
    """Test PySpark notebook generation."""
    
    def test_generate_pyspark_notebook_basic(self):
        """Test generating basic PySpark notebook."""
        code = "df = spark.read.csv('data.csv')\ndf.show()"
        nb = generate_pyspark_notebook(
            prompt="Read CSV",
            code=code,
            schema_context=""
        )
        assert 'cells' in nb
        nb_str = json.dumps(nb)
        assert 'spark.read.csv' in nb_str
    
    def test_generate_pyspark_notebook_has_setup(self):
        """Test PySpark notebook includes Spark setup."""
        nb = generate_pyspark_notebook(
            prompt="Test",
            code="df.count()",
            schema_context=""
        )
        nb_str = json.dumps(nb)
        assert 'SparkSession' in nb_str or 'spark' in nb_str.lower()
    
    def test_generate_pyspark_notebook_with_presto_config(self):
        """Test PySpark notebook with Presto JDBC config."""
        presto_config = {
            'host': 'presto.example.com',
            'port': 8080,
            'catalog': 'hive',
            'schema': 'default'
        }
        nb = generate_pyspark_notebook(
            prompt="Read from Presto",
            code="df = read_presto_table('users')",
            schema_context="",
            presto_config=presto_config
        )
        nb_str = json.dumps(nb)
        assert 'jdbc:presto' in nb_str
        assert 'presto.example.com' in nb_str
    
    def test_generate_pyspark_notebook_has_output_section(self):
        """Test PySpark notebook includes output/write section."""
        nb = generate_pyspark_notebook(
            prompt="Transform data",
            code="df = df.filter(col('age') > 18)",
            schema_context=""
        )
        nb_str = json.dumps(nb).lower()
        assert 'write' in nb_str or 'output' in nb_str


class TestAnalysisNotebookGeneration:
    """Test analysis notebook generation."""
    
    def test_generate_analysis_notebook_basic(self):
        """Test generating basic analysis notebook."""
        results = [{'id': 1, 'count': 10}]
        nb = generate_analysis_notebook(
            question="How many records?",
            sql="SELECT COUNT(*) as count FROM table",
            results=results,
            explanation="There are 10 records.",
            schema_context=""
        )
        assert 'cells' in nb
        nb_str = json.dumps(nb)
        assert 'COUNT(*)' in nb_str
        assert '10 records' in nb_str or 'There are 10' in nb_str
    
    def test_generate_analysis_notebook_includes_question(self):
        """Test analysis notebook includes original question."""
        nb = generate_analysis_notebook(
            question="What is the total revenue?",
            sql="SELECT SUM(amount) FROM sales",
            results=[{'sum': 1000}],
            explanation="Total revenue is $1000",
            schema_context=""
        )
        nb_str = json.dumps(nb)
        assert 'total revenue' in nb_str.lower()
    
    def test_generate_analysis_notebook_includes_explanation(self):
        """Test analysis notebook includes LLM explanation."""
        explanation = "The data shows increasing trend over time."
        nb = generate_analysis_notebook(
            question="Show trends",
            sql="SELECT * FROM trends",
            results=[],
            explanation=explanation,
            schema_context=""
        )
        nb_str = json.dumps(nb)
        assert 'increasing trend' in nb_str
    
    def test_generate_analysis_notebook_has_visualization(self):
        """Test analysis notebook includes visualization placeholders."""
        nb = generate_analysis_notebook(
            question="Plot data",
            sql="SELECT date, value FROM metrics",
            results=[{'date': '2024-01-01', 'value': 100}],
            explanation="Values over time",
            schema_context=""
        )
        nb_str = json.dumps(nb).lower()
        assert 'visualization' in nb_str or 'plot' in nb_str
    
    def test_generate_analysis_notebook_with_presto_config(self):
        """Test analysis notebook with Presto reconnection section."""
        presto_config = {
            'host': 'presto.local',
            'port': 8080,
            'user': 'analyst',
            'catalog': 'hive',
            'schema': 'default'
        }
        nb = generate_analysis_notebook(
            question="Analyze data",
            sql="SELECT 1",
            results=[{'value': 1}],
            explanation="Result is 1",
            schema_context="",
            presto_config=presto_config
        )
        nb_str = json.dumps(nb)
        assert 'presto.local' in nb_str
        # Should have section for re-running with live data
        assert 'live' in nb_str.lower() or 're-run' in nb_str.lower()


class TestNotebookImports:
    """Test that notebook functions can be imported correctly."""
    
    def test_import_from_clip_notebook(self):
        """Test importing from clip.notebook module."""
        from clip.notebook import (
            generate_sql_notebook,
            generate_pyspark_notebook,
            generate_analysis_notebook,
            save_notebook
        )
        assert callable(generate_sql_notebook)
        assert callable(generate_pyspark_notebook)
        assert callable(generate_analysis_notebook)
        assert callable(save_notebook)
    
    def test_import_from_clip_package(self):
        """Test importing from clip package __init__."""
        from clip import (
            generate_sql_notebook,
            generate_pyspark_notebook,
            generate_analysis_notebook,
            save_notebook
        )
        assert callable(generate_sql_notebook)
        assert callable(generate_pyspark_notebook)
        assert callable(generate_analysis_notebook)
        assert callable(save_notebook)


class TestNotebookStructure:
    """Test that generated notebooks have valid Jupyter structure."""
    
    def test_sql_notebook_jupyter_compatible(self):
        """Test SQL notebook is Jupyter-compatible."""
        nb = generate_sql_notebook(
            prompt="Test",
            sql="SELECT 1",
            schema_context=""
        )
        # Must have required Jupyter fields
        assert 'nbformat' in nb
        assert 'nbformat_minor' in nb
        assert 'metadata' in nb
        assert 'cells' in nb
        assert nb['nbformat'] == 4
    
    def test_pyspark_notebook_jupyter_compatible(self):
        """Test PySpark notebook is Jupyter-compatible."""
        nb = generate_pyspark_notebook(
            prompt="Test",
            code="print('test')",
            schema_context=""
        )
        assert nb['nbformat'] == 4
        assert 'kernelspec' in nb['metadata']
    
    def test_analysis_notebook_jupyter_compatible(self):
        """Test analysis notebook is Jupyter-compatible."""
        nb = generate_analysis_notebook(
            question="Test",
            sql="SELECT 1",
            results=[],
            explanation="Test",
            schema_context=""
        )
        assert nb['nbformat'] == 4
        assert isinstance(nb['cells'], list)


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
