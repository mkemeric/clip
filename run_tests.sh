#!/bin/bash
# Convenience script for running tests

set -e

echo "========================================"
echo "Running Unit Tests for presto-ai-cli"
echo "========================================"
echo ""

# Change to project directory
cd "$(dirname "$0")"

# Check if pytest is installed
if ! command -v pytest &> /dev/null; then
    echo "Error: pytest not found. Installing pytest..."
    pip install pytest pytest-cov
fi

# Parse command line arguments
case "${1:-all}" in
    "all")
        echo "Running all tests..."
        pytest tests/ -v
        ;;
    "cli")
        echo "Running CLI tests..."
        pytest tests/test_cli.py -v
        ;;
    "config")
        echo "Running configuration tests..."
        pytest tests/test_config.py -v
        ;;
    "llm")
        echo "Running LLM client tests..."
        pytest tests/test_llm_client.py -v
        ;;
    "notebook")
        echo "Running notebook tests..."
        pytest tests/test_notebook.py -v
        ;;
    "presto")
        echo "Running Presto client tests..."
        pytest tests/test_presto_client.py -v
        ;;
    "coverage")
        echo "Running tests with coverage..."
        pytest tests/ --cov=clip --cov-report=html --cov-report=term-missing
        echo ""
        echo "HTML coverage report generated in htmlcov/index.html"
        ;;
    "quick")
        echo "Running quick test (no verbose output)..."
        pytest tests/ -q
        ;;
    *)
        echo "Usage: $0 [all|cli|config|llm|notebook|presto|coverage|quick]"
        echo ""
        echo "Options:"
        echo "  all       - Run all tests (default)"
        echo "  cli       - Run CLI tests only"
        echo "  config    - Run configuration tests only"
        echo "  llm       - Run LLM client tests only"
        echo "  notebook  - Run notebook tests only"
        echo "  presto    - Run Presto client tests only"
        echo "  coverage  - Run all tests with coverage report"
        echo "  quick     - Run quick test without verbose output"
        exit 1
        ;;
esac

echo ""
echo "========================================"
echo "Tests completed!"
echo "========================================"
