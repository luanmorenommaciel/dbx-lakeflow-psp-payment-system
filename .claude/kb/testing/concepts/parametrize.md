# Parametrize

> **Purpose**: Data-driven testing with @pytest.mark.parametrize for multiple input/output combos
> **Confidence**: 0.95
> **MCP Validated**: 2026-02-17

## Overview

`@pytest.mark.parametrize` runs the same test function with different arguments, generating
a separate test case for each parameter set. This eliminates duplicated test logic, improves
edge case coverage, and produces clear per-case failure reports. Parameters can be simple
values, tuples, or generated from fixtures.

## The Pattern

```python
import pytest


@pytest.mark.parametrize("input_val, expected", [
    ("INV-001", "INV-001"),
    ("  inv-002  ", "INV-002"),
    ("inv-003", "INV-003"),
    ("  INV-004", "INV-004"),
])
def test_normalize_invoice_number(input_val, expected):
    result = input_val.strip().upper()
    assert result == expected


@pytest.mark.parametrize("amount, currency, expected_str", [
    (100.00, "USD", "$100.00"),
    (99.99, "USD", "$99.99"),
    (0.01, "USD", "$0.01"),
    (1000000.00, "USD", "$1,000,000.00"),
])
def test_format_currency(amount, currency, expected_str):
    result = format_currency(amount, currency)
    assert result == expected_str
```

## Edge Case Coverage Pattern

```python
@pytest.mark.parametrize("value, should_raise", [
    # Happy path
    ("valid-input", False),
    # Boundary values
    ("", True),
    ("x" * 255, False),
    ("x" * 256, True),
    # None / null
    (None, True),
    # Special characters
    ("hello world!", False),
    ("drop table;--", False),
    # Type edge cases
    (123, True),
    ([], True),
])
def test_validate_input(value, should_raise):
    if should_raise:
        with pytest.raises((ValueError, TypeError)):
            validate_input(value)
    else:
        result = validate_input(value)
        assert result is not None
```

## Multiple Parametrize Decorators (Cartesian Product)

```python
@pytest.mark.parametrize("x", [1, 2, 3])
@pytest.mark.parametrize("y", [10, 20])
def test_multiply(x, y):
    """Generates 6 tests: (1,10), (1,20), (2,10), (2,20), (3,10), (3,20)"""
    result = x * y
    assert result == x * y
```

## Named Test Cases with IDs

```python
@pytest.mark.parametrize("data, expected", [
    pytest.param(
        {"name": "Acme", "amount": 100},
        True,
        id="valid-invoice"
    ),
    pytest.param(
        {"name": "", "amount": 100},
        False,
        id="empty-vendor-name"
    ),
    pytest.param(
        {"name": "Acme", "amount": -5},
        False,
        id="negative-amount"
    ),
    pytest.param(
        {},
        False,
        id="empty-payload"
    ),
], ids=str)
def test_validate_invoice(data, expected):
    assert is_valid_invoice(data) == expected
```

## Common Mistakes

### Wrong

```python
# Duplicating test logic instead of parametrizing
def test_parse_positive():
    assert parse_int("42") == 42

def test_parse_negative():
    assert parse_int("-1") == -1

def test_parse_zero():
    assert parse_int("0") == 0
```

### Correct

```python
@pytest.mark.parametrize("input_str, expected", [
    ("42", 42),
    ("-1", -1),
    ("0", 0),
])
def test_parse_int(input_str, expected):
    assert parse_int(input_str) == expected
```

## Related

- [pytest Basics](../concepts/pytest-basics.md)
- [Unit Test Patterns](../patterns/unit-test-patterns.md)
- [Fixture Factories](../patterns/fixture-factories.md)
