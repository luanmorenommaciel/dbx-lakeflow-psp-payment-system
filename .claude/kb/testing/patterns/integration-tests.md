# Integration Test Patterns

> **Purpose**: Test patterns for verifying interactions across service boundaries, APIs, databases, and async services
> **MCP Validated**: 2026-03-26

## When to Use

- Verifying that components work together correctly
- Testing real database queries and transactions
- Validating API endpoint request/response cycles
- Testing message queue producers and consumers
- Verifying file I/O and cloud storage operations

## Implementation

```python
import pytest
import json
import tempfile
import os
from unittest.mock import patch


# --- Database Integration Tests ---

@pytest.fixture(scope="session")
def db_engine():
    """Session-scoped database engine."""
    from sqlalchemy import create_engine
    engine = create_engine("sqlite:///test_integration.db")
    yield engine
    engine.dispose()
    os.unlink("test_integration.db")


@pytest.fixture
def db_session(db_engine):
    """Function-scoped session with rollback."""
    from sqlalchemy.orm import Session
    connection = db_engine.connect()
    transaction = connection.begin()
    session = Session(bind=connection)
    yield session
    session.close()
    transaction.rollback()
    connection.close()


@pytest.mark.integration
class TestInvoiceRepository:
    def test_save_and_retrieve(self, db_session):
        repo = InvoiceRepository(db_session)
        invoice = Invoice(number="INV-001", vendor="Acme", amount=100.0)

        repo.save(invoice)
        result = repo.find_by_number("INV-001")

        assert result is not None
        assert result.vendor == "Acme"
        assert result.amount == 100.0

    def test_find_nonexistent_returns_none(self, db_session):
        repo = InvoiceRepository(db_session)

        result = repo.find_by_number("DOES-NOT-EXIST")

        assert result is None


# --- API Integration Tests ---

@pytest.fixture
def api_client():
    """Test client for FastAPI/Flask app."""
    from myapp.main import app
    from fastapi.testclient import TestClient
    client = TestClient(app)
    yield client


@pytest.mark.integration
class TestInvoiceAPI:
    def test_create_invoice_returns_201(self, api_client):
        payload = {
            "invoice_number": "INV-100",
            "vendor": "Test Corp",
            "amount": 500.00,
        }

        response = api_client.post("/invoices", json=payload)

        assert response.status_code == 201
        data = response.json()
        assert data["invoice_number"] == "INV-100"

    def test_get_invoice_not_found_returns_404(self, api_client):
        response = api_client.get("/invoices/NONEXISTENT")

        assert response.status_code == 404

    def test_create_invalid_invoice_returns_422(self, api_client):
        payload = {"vendor": "Test Corp"}  # missing required fields

        response = api_client.post("/invoices", json=payload)

        assert response.status_code == 422


# --- File I/O Integration Tests ---

@pytest.fixture
def temp_data_dir():
    """Create a temporary directory with test files."""
    with tempfile.TemporaryDirectory() as tmpdir:
        # Seed with test data
        data = [
            {"id": 1, "name": "Acme", "amount": 100},
            {"id": 2, "name": "Beta", "amount": 200},
        ]
        path = os.path.join(tmpdir, "invoices.json")
        with open(path, "w") as f:
            json.dump(data, f)
        yield tmpdir


@pytest.mark.integration
def test_process_files_end_to_end(temp_data_dir):
    input_path = os.path.join(temp_data_dir, "invoices.json")
    output_path = os.path.join(temp_data_dir, "output.json")

    process_invoice_file(input_path, output_path)

    assert os.path.exists(output_path)
    with open(output_path) as f:
        results = json.load(f)
    assert len(results) == 2
    assert all(r["processed"] for r in results)
```

## Configuration

| Setting | Default | Description |
|---------|---------|-------------|
| `@pytest.mark.integration` | -- | Marker to separate from unit tests |
| `scope="session"` | -- | Share expensive fixtures across tests |
| `--run-integration` | disabled | Custom flag to enable integration tests |

## Integration Test Isolation Strategies

| Strategy | When to Use | Trade-off |
|----------|-------------|-----------|
| Transaction rollback | Database tests | Fast, no residue |
| Temp directory | File I/O tests | Auto-cleanup |
| Docker containers | Full service tests | Slow, realistic |
| Test database | SQL integration | Requires setup |
| Mock external only | Hybrid approach | Balanced isolation |

## conftest.py for Integration Tests

```python
# tests/integration/conftest.py
import pytest


def pytest_addoption(parser):
    parser.addoption(
        "--run-integration",
        action="store_true",
        default=False,
        help="Run integration tests",
    )


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--run-integration"):
        skip = pytest.mark.skip(reason="Need --run-integration to run")
        for item in items:
            if "integration" in item.keywords:
                item.add_marker(skip)
```

## Async Integration Tests

```python
import pytest
import httpx


@pytest.fixture
async def async_api_client():
    """Async test client for FastAPI apps."""
    from httpx import ASGITransport
    from myapp.main import app

    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client


@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_create_invoice(async_api_client):
    payload = {"invoice_number": "INV-200", "vendor": "Async Corp", "amount": 750.00}
    response = await async_api_client.post("/invoices", json=payload)
    assert response.status_code == 201


@pytest.mark.asyncio
@pytest.mark.integration
async def test_async_health_check(async_api_client):
    response = await async_api_client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
```

## Property-Based Integration Tests (Hypothesis)

```python
from hypothesis import given, strategies as st, settings
import pytest


@pytest.mark.integration
@settings(max_examples=50)
@given(
    vendor=st.text(min_size=1, max_size=100),
    amount=st.floats(min_value=0.01, max_value=1_000_000, allow_nan=False),
)
def test_invoice_roundtrip(api_client, vendor, amount):
    """Any valid vendor/amount should survive a create-then-fetch cycle."""
    payload = {"invoice_number": "INV-HYP", "vendor": vendor, "amount": round(amount, 2)}
    create = api_client.post("/invoices", json=payload)
    if create.status_code == 201:
        fetched = api_client.get(f"/invoices/{create.json()['id']}")
        assert fetched.status_code == 200
        assert fetched.json()["vendor"] == vendor
```

## See Also

- [Fixtures](../concepts/fixtures.md)
- [Unit Test Patterns](../patterns/unit-test-patterns.md)
- [Spark Testing](../patterns/spark-testing.md)
