# Unit Tests for presto-ai-cli

This directory contains comprehensive unit tests for the presto-ai-cli project after restructuring the codebase into the `clip` package.

## Test Coverage

The test suite covers the following areas:

### 1. CLI Main Function (`test_cli.py`)
- Tests that CLI executes successfully after restructuring
- Verifies all CLI commands and subcommands are accessible
- Tests command-line argument parsing
- Validates help and version output
- Tests config, schema, query, sql, spark, ask, chat, and superset commands

### 2. Configuration Management (`test_config.py`)
- Tests configuration loading and saving with new config paths
- Verifies Config creation with defaults and custom values
- Tests environment variable loading (OpenAI, Anthropic, Superset credentials)
- Validates save/load roundtrips
- Tests the `set()` method for updating configuration
- Validates `get_presto_url()` method

### 3. LLM Client (`test_llm_client.py`)
- Tests that LLM client handles requests correctly after renaming
- Verifies provider creation (OpenAI, Anthropic, Ollama, Custom)
- Tests code extraction from LLM responses
- Validates SQL generation functionality
- Tests PySpark code generation
- Validates results explanation functionality
- Tests handling of missing dependencies

### 4. Notebook Functions (`test_notebook.py`)
- Tests that notebook creation and export functions operate properly post-restructure
- Validates cell creation (code and markdown)
- Tests notebook structure creation
- Validates notebook saving to disk
- Tests SQL notebook generation
- Tests PySpark notebook generation
- Tests analysis notebook generation with visualizations
- Verifies Jupyter compatibility

### 5. Presto Client (`test_presto_client.py`)
- Tests that Presto query execution in the new module path works correctly
- Validates client instantiation and configuration
- Tests connection management (lazy initialization, SSL patching, OAuth cookies)
- Tests query execution with and without LIMIT clauses
- Validates schema exploration (catalogs, schemas, tables, columns)
- Tests DDL generation for LLM context
- Validates sample data retrieval

## Running the Tests

### Install Test Dependencies

First, install the development dependencies:

```bash
pip install -e ".[dev]"
```

Or install pytest directly:

```bash
pip install pytest pytest-cov
```

### Run All Tests

```bash
pytest
```

### Run Specific Test Files

```bash
# Test CLI functionality
pytest tests/test_cli.py

# Test configuration
pytest tests/test_config.py

# Test LLM client
pytest tests/test_llm_client.py

# Test notebook generation
pytest tests/test_notebook.py

# Test Presto client
pytest tests/test_presto_client.py
```

### Run Specific Test Classes or Functions

```bash
# Run a specific test class
pytest tests/test_config.py::TestConfigSaveLoad

# Run a specific test function
pytest tests/test_cli.py::TestCLIMain::test_cli_help_command
```

### Run with Coverage

```bash
# Generate coverage report
pytest --cov=clip --cov-report=html --cov-report=term

# View HTML coverage report
open htmlcov/index.html
```

### Run with Verbose Output

```bash
pytest -v
```

### Run and Show Print Statements

```bash
pytest -s
```

## Test Structure

All tests follow these conventions:

1. **Mocking**: Tests use `unittest.mock` to mock external dependencies (Presto connections, LLM APIs, file I/O)
2. **Isolation**: Each test is independent and doesn't rely on external services
3. **Naming**: Test functions are descriptive and follow the pattern `test_<what_is_being_tested>`
4. **Organization**: Tests are organized into classes that group related functionality
5. **Assertions**: Tests use clear assertions to verify expected behavior

## Test Requirements

The tests are designed to run without requiring:
- Active Presto database connection
- LLM API keys
- External services

All external dependencies are mocked to ensure tests can run in any environment.

## Continuous Integration

These tests are suitable for CI/CD pipelines and should be run:
- Before merging pull requests
- On every commit to main branch
- Before creating releases

## Adding New Tests

When adding new features, follow these guidelines:

1. Create tests in the appropriate test file
2. Use descriptive test names that explain what is being tested
3. Mock all external dependencies
4. Include both positive and negative test cases
5. Test edge cases and error conditions
6. Keep tests focused on a single aspect of functionality

## Troubleshooting

### Import Errors

If you encounter import errors, ensure:
- You're running tests from the project root directory
- The `clip` package is importable (run `pip install -e .`)
- Python path includes the project directory

### Mock-Related Issues

If mocks aren't working as expected:
- Verify patch paths match the module structure (use `clip.module_name`)
- Check that patches are applied in the correct order
- Ensure mock return values match expected types

## Coverage Goals

Target test coverage metrics:
- Overall coverage: >80%
- Critical paths (CLI, config, query execution): >90%
- New code: 100%

Run `pytest --cov=clip` to check current coverage levels.
