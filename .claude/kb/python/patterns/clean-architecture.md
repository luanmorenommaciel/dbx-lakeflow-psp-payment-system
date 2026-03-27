# Clean Architecture

> **Purpose**: Clean code structure, naming conventions, module organization, uv for Python projects
> **MCP Validated:** 2026-03-26

## When to Use

- Starting a new Python project or module
- Refactoring legacy code for maintainability
- Establishing team coding standards
- Structuring packages with clear boundaries

## Implementation

### Project Structure

```text
src/
  my_project/
    __init__.py
    domain/              # Business logic, no external dependencies
      __init__.py
      models.py          # Dataclasses, domain entities
      errors.py          # Custom exception hierarchy
      services.py        # Core business rules
    adapters/            # External integrations
      __init__.py
      database.py        # DB access layer
      api_client.py      # HTTP clients
      file_reader.py     # File I/O
    application/         # Use cases, orchestration
      __init__.py
      use_cases.py       # Application-level workflows
      interfaces.py      # Protocols / ABCs
    config.py            # Settings, environment
tests/
  conftest.py
  test_domain/
  test_adapters/
  test_application/
pyproject.toml
```

### Dependency Rule

```text
domain  <--  application  <--  adapters
(pure)       (orchestrates)     (implements)

Inner layers NEVER import from outer layers.
Adapters depend on application interfaces.
Domain has zero external dependencies.
```

## Naming Conventions

| Element | Convention | Example |
|---------|-----------|---------|
| Module | `snake_case.py` | `file_reader.py` |
| Class | `PascalCase` | `InvoiceParser` |
| Function | `snake_case` | `parse_invoice()` |
| Constant | `UPPER_SNAKE` | `MAX_RETRIES = 3` |
| Private | `_leading_underscore` | `_validate_input()` |
| Type alias | `PascalCase` | `type RecordMap = dict[str, str]` |
| Boolean | `is_/has_/can_` prefix | `is_valid`, `has_errors` |

## Function Design

```python
from collections.abc import Iterator
from dataclasses import dataclass


# GOOD: single responsibility, clear types, descriptive name
def extract_active_users(
    records: Iterator[dict[str, str]],
    *,
    min_age: int = 18,
) -> list[str]:
    """Extract usernames of active users above minimum age."""
    return [
        r["username"]
        for r in records
        if r.get("status") == "active"
        and int(r.get("age", 0)) >= min_age
    ]


# BAD: vague name, no types, multiple responsibilities
def process(data, flag=True):
    results = []
    for d in data:
        if flag:
            if d.get("status") == "active":
                results.append(d["username"])
        else:
            results.append(d)
    return results
```

## Interface Segregation with Protocols

```python
from typing import Protocol, runtime_checkable
from collections.abc import Iterator


@runtime_checkable
class Reader(Protocol):
    def read(self, path: str) -> Iterator[dict]: ...


@runtime_checkable
class Writer(Protocol):
    def write(self, records: list[dict], path: str) -> int: ...


class CSVReader:
    """Implements Reader protocol without explicit inheritance."""

    def read(self, path: str) -> Iterator[dict]:
        import csv
        with open(path, newline="") as fh:
            yield from csv.DictReader(fh)


def process_data(reader: Reader, writer: Writer, src: str, dst: str) -> int:
    """Orchestrate read-transform-write using protocol interfaces."""
    records = [row for row in reader.read(src)]
    return writer.write(records, dst)
```

## Configuration Pattern

```python
from dataclasses import dataclass, field
import os


@dataclass(frozen=True, slots=True)
class AppConfig:
    """Immutable application configuration from environment."""
    db_host: str = field(default_factory=lambda: os.getenv("DB_HOST", "localhost"))
    db_port: int = field(default_factory=lambda: int(os.getenv("DB_PORT", "5432")))
    debug: bool = field(default_factory=lambda: os.getenv("DEBUG", "").lower() == "true")
    max_workers: int = field(default_factory=lambda: int(os.getenv("MAX_WORKERS", "4")))

    def __post_init__(self) -> None:
        if self.max_workers < 1:
            raise ValueError("max_workers must be >= 1")
```

## Module Docstrings

```python
"""Invoice processing domain models.

This module defines the core data structures for invoice extraction.
No external dependencies -- pure Python dataclasses and enums.

Typical usage:
    from my_project.domain.models import Invoice, LineItem
    invoice = Invoice(number="INV-001", items=[LineItem(...)])
"""
```

## Modern Project Setup with uv

```bash
# Create project with uv (replaces pip, poetry, pyenv, virtualenv)
uv init my_pipeline --python 3.13
cd my_pipeline

# Add dependencies
uv add pydantic sqlalchemy
uv add pytest ruff --dev

# Run tests
uv run pytest

# Lock dependencies for reproducibility
uv lock
```

### pyproject.toml (uv-managed)

```toml
[project]
name = "my-pipeline"
version = "0.1.0"
requires-python = ">=3.13"
dependencies = [
    "pydantic>=2.0",
    "sqlalchemy>=2.0",
]

[dependency-groups]
dev = [
    "pytest>=8.0",
    "ruff>=0.8",
]

[tool.ruff]
target-version = "py313"
line-length = 99

[tool.pytest.ini_options]
testpaths = ["tests"]
markers = ["slow", "integration"]
```

## Anti-Patterns to Avoid

| Anti-Pattern | Problem | Clean Alternative |
|-------------|---------|-------------------|
| God class (1000+ lines) | Untestable, unclear responsibility | Split into focused classes |
| Magic numbers | Unclear intent | Named constants |
| Deep nesting (3+ levels) | Hard to read | Early returns, extract functions |
| Star imports `from x import *` | Namespace pollution | Explicit imports |
| Circular imports | Architectural issue | Dependency inversion |
| Print debugging | Not production-safe | `logging` module |
| `pip install` + `requirements.txt` | Fragile, no lockfile | `uv add` + `uv lock` |
| Manual venv creation | Error-prone | `uv sync` (auto-creates venv) |

## See Also

- [Dataclasses](../concepts/dataclasses.md)
- [Error Handling](../patterns/error-handling.md)
- [Type Hints](../concepts/type-hints.md)
