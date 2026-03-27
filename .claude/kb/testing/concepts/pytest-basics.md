# pytest Basics

> **Purpose**: Core pytest 8+ conventions, test discovery, markers, async, and CLI usage
> **Confidence**: 0.95
> **MCP Validated**: 2026-03-26

## Overview

pytest is the standard Python testing framework. It discovers tests automatically by scanning
for files matching `test_*.py` or `*_test.py`, classes prefixed with `Test`, and functions
prefixed with `test_`. It provides rich assertion introspection (no need for `assertEqual`),
powerful fixtures, markers for metadata, and a plugin ecosystem.

## The Pattern

```python
import pytest


# Simple test function -- pytest discovers this automatically
def test_addition():
    assert 1 + 1 == 2


# Test with descriptive assertion messages
def test_parse_invoice_number():
    raw = "  inv-001  "
    result = raw.strip().upper()
    assert result == "INV-001", f"Expected 'INV-001', got '{result}'"


# Group related tests in a class (no inheritance needed)
class TestStringProcessor:
    def test_strip_whitespace(self):
        assert "  hello  ".strip() == "hello"

    def test_uppercase(self):
        assert "hello".upper() == "HELLO"

    def test_empty_string(self):
        assert "".strip() == ""
```

## Markers

```python
import pytest

# Built-in markers
@pytest.mark.skip(reason="Not implemented yet")
def test_future_feature():
    pass

@pytest.mark.skipif(
    condition=True, reason="Only on Linux"
)
def test_linux_only():
    pass

@pytest.mark.xfail(reason="Known bug #123")
def test_known_failure():
    assert 1 == 2

# Custom markers (register in pyproject.toml)
@pytest.mark.slow
def test_large_dataset_processing():
    pass

@pytest.mark.integration
def test_api_endpoint():
    pass
```

## Test Discovery Rules

| Convention | Example | Discovered? |
|------------|---------|-------------|
| File: `test_*.py` | `test_utils.py` | Yes |
| File: `*_test.py` | `utils_test.py` | Yes |
| Function: `test_*` | `def test_parse():` | Yes |
| Class: `Test*` | `class TestParser:` | Yes |
| Method: `test_*` | `def test_valid(self):` | Yes |
| File: `utils.py` | `utils.py` | No |
| Function: `helper_*` | `def helper_parse():` | No |

## Expected Exceptions

```python
import pytest

def test_raises_value_error():
    with pytest.raises(ValueError, match="must be positive"):
        parse_amount(-5)

def test_raises_type_error():
    with pytest.raises(TypeError):
        parse_amount("not a number")
```

## Common Mistakes

### Wrong

```python
# Using unittest assertions in pytest
import unittest

class TestBad(unittest.TestCase):
    def test_value(self):
        self.assertEqual(1 + 1, 2)  # Works but loses pytest features
```

### Correct

```python
# Plain assert with pytest -- better output on failure
def test_value():
    assert 1 + 1 == 2
```

## Async Testing (pytest-asyncio)

```python
import pytest
import httpx


# Mark individual test as async
@pytest.mark.asyncio
async def test_fetch_data():
    async with httpx.AsyncClient() as client:
        response = await client.get("https://api.example.com/data")
    assert response.status_code == 200


# Async fixture with teardown
@pytest.fixture
async def async_db():
    conn = await create_connection("test://localhost/db")
    yield conn
    await conn.close()


@pytest.mark.asyncio
async def test_query(async_db):
    result = await async_db.fetch("SELECT 1")
    assert result == [(1,)]
```

### pyproject.toml for auto mode

```toml
[tool.pytest.ini_options]
asyncio_mode = "auto"  # all async tests auto-detected, no marker needed
```

## Property-Based Testing (Hypothesis)

```python
from hypothesis import given, strategies as st, assume


@given(st.lists(st.integers()))
def test_sort_is_idempotent(xs):
    """Sorting twice gives the same result as sorting once."""
    assert sorted(sorted(xs)) == sorted(xs)


@given(st.text(min_size=1), st.text(min_size=1))
def test_concat_length(a, b):
    """Concatenation length equals sum of parts."""
    assert len(a + b) == len(a) + len(b)


@given(st.integers(min_value=1, max_value=1000))
def test_positive_division(n):
    """Division by self always yields 1."""
    assume(n != 0)
    assert n / n == 1.0
```

## pytest 8+ New Features

| Feature | Description |
|---------|-------------|
| `pytest.HIDDEN_PARAM` (8.4+) | Hide parameter from test name in parametrize |
| Improved `--tb` output | Better traceback formatting and diffs |
| `--override-ini` | Override pyproject.toml settings from CLI |
| Native `pathlib` support | Fixtures return `Path` objects by default |

## CLI Quick Reference

| Flag | Purpose |
|------|---------|
| `-v` | Verbose: show each test name |
| `-x` | Exit on first failure |
| `-s` | Show print/stdout output |
| `--lf` | Re-run only last-failed tests |
| `--ff` | Run failures first, then the rest |
| `-k "expr"` | Filter tests by name expression |
| `-m "marker"` | Run only tests with marker |
| `--co` | Collect and list tests, do not run |
| `--sw` | Stepwise: stop on failure, resume next run |
| `-n auto` | Parallel execution (pytest-xdist) |

## Related

- [Fixtures](../concepts/fixtures.md)
- [Parametrize](../concepts/parametrize.md)
- [Unit Test Patterns](../patterns/unit-test-patterns.md)
