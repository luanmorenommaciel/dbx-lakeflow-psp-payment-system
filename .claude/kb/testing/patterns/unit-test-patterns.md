# Unit Test Patterns

> **Purpose**: Structured unit test practices using AAA pattern, edge case coverage, and assertion best practices
> **MCP Validated**: 2026-02-17

## When to Use

- Testing a single function or method in isolation
- Verifying business logic without external dependencies
- Covering happy path, edge cases, and error conditions
- Fast feedback during development (< 1 second per test)

## Implementation

```python
import pytest
from unittest.mock import patch, Mock
from myapp.invoice import InvoiceProcessor, InvalidInvoiceError


class TestInvoiceProcessor:
    """Unit tests following Arrange-Act-Assert (AAA) pattern."""

    # --- Happy Path ---

    def test_process_valid_invoice(self):
        # Arrange
        processor = InvoiceProcessor()
        data = {
            "invoice_number": "INV-001",
            "vendor": "Acme Corp",
            "amount": 1500.00,
            "currency": "USD",
        }

        # Act
        result = processor.process(data)

        # Assert
        assert result.invoice_number == "INV-001"
        assert result.vendor == "Acme Corp"
        assert result.amount == 1500.00

    # --- Edge Cases ---

    def test_process_strips_whitespace(self):
        processor = InvoiceProcessor()
        data = {"invoice_number": "  INV-002  ", "vendor": "  Acme  ", "amount": 100}

        result = processor.process(data)

        assert result.invoice_number == "INV-002"
        assert result.vendor == "Acme"

    def test_process_zero_amount_raises(self):
        processor = InvoiceProcessor()
        data = {"invoice_number": "INV-003", "vendor": "Acme", "amount": 0}

        with pytest.raises(InvalidInvoiceError, match="amount must be positive"):
            processor.process(data)

    def test_process_none_vendor_raises(self):
        processor = InvoiceProcessor()
        data = {"invoice_number": "INV-004", "vendor": None, "amount": 100}

        with pytest.raises(InvalidInvoiceError):
            processor.process(data)

    def test_process_missing_field_raises(self):
        processor = InvoiceProcessor()

        with pytest.raises(KeyError):
            processor.process({})

    # --- Boundary Values ---

    @pytest.mark.parametrize("amount", [0.01, 0.001, 999999.99])
    def test_process_boundary_amounts(self, amount):
        processor = InvoiceProcessor()
        data = {"invoice_number": "INV-005", "vendor": "Acme", "amount": amount}

        result = processor.process(data)
        assert result.amount == amount

    # --- Mocked Dependencies ---

    @patch("myapp.invoice.send_notification")
    def test_process_sends_notification(self, mock_notify):
        processor = InvoiceProcessor()
        data = {"invoice_number": "INV-006", "vendor": "Acme", "amount": 100}

        processor.process(data)

        mock_notify.assert_called_once_with(
            invoice_number="INV-006",
            message="Invoice processed successfully"
        )
```

## Edge Case Checklist

| Category | Test Cases |
|----------|------------|
| **Empty values** | `""`, `[]`, `{}`, `None` |
| **Boundaries** | `0`, `0.01`, `MAX_INT`, `-1` |
| **Types** | Wrong type passed (str instead of int) |
| **Whitespace** | Leading, trailing, only whitespace |
| **Unicode** | Accented chars, emojis, CJK |
| **Large input** | 10K items, 1MB string |
| **Duplicates** | Repeated values in lists |
| **Ordering** | Already sorted, reverse sorted, random |

## Assertion Best Practices

```python
# Prefer specific assertions
assert result.status == "completed"           # Good: specific
assert result is not None                     # Good: explicit check
assert len(result.items) == 3                 # Good: exact count
assert "error" in result.message.lower()      # Good: content check

# Use pytest.approx for floats
assert result.total == pytest.approx(99.99, abs=0.01)

# Check types
assert isinstance(result, Invoice)

# Check collections
assert set(result.keys()) == {"id", "name", "amount"}
assert all(item.valid for item in result.items)
```

## Test Naming Convention

```python
# Pattern: test_<method>_<scenario>_<expected_behavior>
def test_parse_valid_json_returns_dict():        ...
def test_parse_empty_string_raises_value_error(): ...
def test_parse_none_input_returns_none():         ...
def test_calculate_total_with_tax_includes_tax(): ...
```

## Example Usage

```python
# Run only unit tests (by marker or directory)
# pytest tests/unit/ -v
# pytest -m "not integration and not slow"

# Run with coverage
# pytest tests/unit/ --cov=myapp --cov-report=term-missing
```

## See Also

- [pytest Basics](../concepts/pytest-basics.md)
- [Mocking](../concepts/mocking.md)
- [Parametrize](../concepts/parametrize.md)
- [Integration Tests](../patterns/integration-tests.md)
