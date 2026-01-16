# Testing Quick Reference

## Test Files Overview

| Test File | Purpose | Test Count |
|-----------|---------|------------|
| `test_cli.py` | CLI main function execution | 17 tests |
| `test_config.py` | Configuration loading/saving | 22 tests |
| `test_llm_client.py` | LLM client request handling | 26 tests |
| `test_notebook.py` | Notebook creation/export | 27 tests |
| `test_presto_client.py` | Presto query execution | 23 tests |

## Quick Commands

```bash
# Run all tests
pytest

# Run with verbose output
pytest -v

# Run specific test file
pytest tests/test_cli.py

# Run specific test class
pytest tests/test_config.py::TestConfigSaveLoad

# Run specific test function
pytest tests/test_cli.py::TestCLIMain::test_cli_help_command

# Run with coverage
pytest --cov=clip --cov-report=term-missing

# Run with coverage HTML report
pytest --cov=clip --cov-report=html
open htmlcov/index.html

# Using the convenience script
./run_tests.sh all        # All tests
./run_tests.sh cli        # CLI tests only
./run_tests.sh config     # Config tests only
./run_tests.sh llm        # LLM client tests only
./run_tests.sh notebook   # Notebook tests only
./run_tests.sh presto     # Presto client tests only
./run_tests.sh coverage   # With coverage report
./run_tests.sh quick      # Quick run
```

## Test Requirements Checklist

✅ All 5 test cases covered:
1. CLI main function executes successfully after restructuring
2. Configuration loading and saving works with new config paths
3. LLM client handles requests correctly after renaming
4. Notebook creation and export functions operate properly post-restructure
5. Presto query execution in the new module path works correctly

## What the Tests Verify

### 1. Import Paths
- All modules can be imported from `clip.*`
- Cross-module imports work correctly
- Package structure is valid

### 2. Functionality
- All functions work as expected after restructuring
- Configuration persistence works
- CLI commands execute correctly
- Database queries execute properly
- Notebooks are generated correctly

### 3. Error Handling
- Invalid inputs are handled gracefully
- Missing dependencies are caught
- Error messages are appropriate

### 4. Integration
- Modules work together correctly
- Config is used properly across modules
- Data flows correctly between components

## Troubleshooting

### Problem: Tests fail with import errors
**Solution**: Install the package in development mode
```bash
pip install -e .
```

### Problem: pytest not found
**Solution**: Install pytest
```bash
pip install pytest pytest-cov
```

### Problem: Mock patches don't work
**Solution**: Verify patch paths use `clip.` prefix
```python
# Correct
@patch('clip.config.Config.load')

# Incorrect
@patch('config.Config.load')
```

### Problem: Tests run but fail
**Solution**: Check that the `clip` package structure matches the test expectations
```
clip/
  __init__.py
  cli.py
  config.py
  llm_client.py
  notebook.py
  presto_client.py
  superset_client.py
```

## Test Execution Flow

1. **Setup**: Mocks are created for external dependencies
2. **Execute**: Test function runs with mocked dependencies
3. **Assert**: Results are verified against expected outcomes
4. **Teardown**: Mocks are cleaned up automatically

## Best Practices

1. **Run tests before committing**: `pytest`
2. **Check coverage regularly**: `pytest --cov=clip`
3. **Run specific tests during development**: `pytest tests/test_config.py -v`
4. **Use the convenience script for quick checks**: `./run_tests.sh quick`
5. **Generate HTML coverage for detailed analysis**: `./run_tests.sh coverage`

## CI/CD Integration

The test suite is designed to run in CI/CD pipelines:
- No external dependencies required
- Fast execution (all mocked)
- Deterministic results
- Exit codes indicate pass/fail

Example GitHub Actions workflow:
```yaml
- name: Run tests
  run: |
    pip install pytest pytest-cov
    pytest --cov=clip --cov-report=xml
```

Example GitLab CI:
```yaml
test:
  script:
    - pip install pytest pytest-cov
    - pytest --cov=clip --cov-report=xml
  coverage: '/TOTAL.*\s+(\d+%)$/'
```

## Coverage Goals

| Component | Target Coverage |
|-----------|----------------|
| Overall | >80% |
| CLI | >90% |
| Config | >90% |
| LLM Client | >85% |
| Notebook | >85% |
| Presto Client | >90% |

Check current coverage:
```bash
pytest --cov=clip --cov-report=term-missing
```

## Additional Resources

- **Full Documentation**: `tests/README.md`
- **Test Summary**: `TEST_SUMMARY.md`
- **Pytest Config**: `pytest.ini`
- **Run Script**: `run_tests.sh`
