# Fixture Factories

> **Purpose**: Factory function patterns for generating reusable, customizable test data
> **MCP Validated**: 2026-02-17

## When to Use

- Tests need similar but slightly different data objects
- Avoiding repetitive test data setup across many test files
- Building complex object graphs with sensible defaults
- Testing with randomized or boundary values
- Creating domain-specific test data builders

## Implementation

```python
import pytest
from datetime import date, timedelta
from typing import Optional
from dataclasses import dataclass


# --- Domain Model ---

@dataclass
class Invoice:
    invoice_number: str
    vendor: str
    amount: float
    currency: str = "USD"
    issue_date: date = None
    status: str = "pending"

    def __post_init__(self):
        if self.issue_date is None:
            self.issue_date = date.today()


# --- Factory Fixture ---

@pytest.fixture
def make_invoice():
    """Factory fixture: returns a function that creates Invoices."""
    counter = 0

    def _factory(
        invoice_number: Optional[str] = None,
        vendor: str = "Acme Corp",
        amount: float = 100.00,
        currency: str = "USD",
        issue_date: Optional[date] = None,
        status: str = "pending",
    ) -> Invoice:
        nonlocal counter
        counter += 1
        return Invoice(
            invoice_number=invoice_number or f"INV-{counter:04d}",
            vendor=vendor,
            amount=amount,
            currency=currency,
            issue_date=issue_date or date.today(),
            status=status,
        )

    return _factory


# --- Usage in Tests ---

class TestInvoiceProcessing:
    def test_process_single_invoice(self, make_invoice):
        invoice = make_invoice(amount=500.00)
        result = process_invoice(invoice)
        assert result.status == "processed"

    def test_process_multiple_vendors(self, make_invoice):
        invoices = [
            make_invoice(vendor="Acme Corp", amount=100),
            make_invoice(vendor="Beta Inc", amount=200),
            make_invoice(vendor="Gamma LLC", amount=300),
        ]
        results = process_batch(invoices)
        assert len(results) == 3
        assert sum(r.amount for r in results) == 600

    def test_process_overdue_invoice(self, make_invoice):
        invoice = make_invoice(
            issue_date=date.today() - timedelta(days=90),
            status="overdue",
        )
        result = process_invoice(invoice)
        assert result.flagged is True

    def test_process_zero_amount_rejected(self, make_invoice):
        invoice = make_invoice(amount=0.0)
        with pytest.raises(ValueError):
            process_invoice(invoice)
```

## Dict Factory Pattern

```python
@pytest.fixture
def make_invoice_dict():
    """Factory returning raw dicts (for API/JSON testing)."""
    counter = 0

    def _factory(**overrides):
        nonlocal counter
        counter += 1
        defaults = {
            "invoice_number": f"INV-{counter:04d}",
            "vendor_name": "Default Vendor",
            "total_amount": 100.00,
            "currency": "USD",
            "line_items": [],
        }
        defaults.update(overrides)
        return defaults

    return _factory


def test_api_create_invoice(api_client, make_invoice_dict):
    payload = make_invoice_dict(
        vendor_name="Custom Vendor",
        total_amount=999.99,
    )
    response = api_client.post("/invoices", json=payload)
    assert response.status_code == 201
```

## Spark DataFrame Factory

```python
@pytest.fixture
def make_spark_df(spark):
    """Factory for creating test DataFrames."""
    def _factory(data=None, schema=None):
        if data is None:
            data = [
                ("INV-001", "Acme", 100.0),
                ("INV-002", "Beta", 200.0),
            ]
        if schema is None:
            schema = ["invoice_id", "vendor", "amount"]
        return spark.createDataFrame(data, schema=schema)

    return _factory


def test_transform_with_factory(make_spark_df):
    df = make_spark_df(data=[
        ("INV-X", "Test", 50.0),
        ("INV-Y", "Test", 150.0),
    ])
    result = aggregate_by_vendor(df)
    assert result.count() == 1
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| Auto-increment IDs | Enabled | Each call gets unique IDs |
| Scope | `function` | Fresh factory per test |
| Defaults | Sensible values | Override only what matters |

## Example Usage

```python
# Place factories in conftest.py for project-wide reuse
# tests/conftest.py  -- shared factories
# tests/unit/conftest.py  -- unit-specific factories
# tests/integration/conftest.py  -- integration factories
```

## See Also

- [Fixtures](../concepts/fixtures.md)
- [Parametrize](../concepts/parametrize.md)
- [Unit Test Patterns](../patterns/unit-test-patterns.md)
- [Spark Testing](../patterns/spark-testing.md)
