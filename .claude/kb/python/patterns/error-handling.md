# Error Handling

> **Purpose**: Exception hierarchy, custom errors, and recovery patterns for robust Python code
> **MCP Validated:** 2026-02-17

## When to Use

- Defining domain-specific exception hierarchies
- Building resilient data pipelines with recovery
- Wrapping third-party library errors
- Implementing retry and fallback patterns

## Implementation

### Custom Exception Hierarchy

```python
class AppError(Exception):
    """Base exception for all application errors."""
    def __init__(self, message: str, *, code: str = "UNKNOWN") -> None:
        self.code = code
        super().__init__(message)


class ValidationError(AppError):
    def __init__(self, message: str, *, field_name: str = "") -> None:
        self.field_name = field_name
        super().__init__(message, code="VALIDATION_ERROR")


class NotFoundError(AppError):
    def __init__(self, resource: str, identifier: str) -> None:
        self.resource = resource
        self.identifier = identifier
        super().__init__(f"{resource} not found: {identifier}", code="NOT_FOUND")


class ExternalServiceError(AppError):
    def __init__(self, service: str, status_code: int | None = None) -> None:
        self.service = service
        self.status_code = status_code
        super().__init__(f"Service {service} failed (status={status_code})", code="EXTERNAL_ERROR")
```

### Exception Handling Best Practices

```python
import logging
logger = logging.getLogger(__name__)

def process_record(record: dict) -> dict:
    try:
        validated = validate(record)
        return enrich(validated)
    except ValidationError as e:
        logger.warning("Validation failed for field %s: %s", e.field_name, e)
        raise
    except ExternalServiceError as e:
        logger.error("Service %s unavailable: %s", e.service, e)
        raise
    except Exception as e:
        logger.exception("Unexpected error processing record: %s", e)
        raise AppError(f"Processing failed: {e}", code="INTERNAL") from e
```

## Exception Chaining (from)

```python
def load_config(path: str) -> dict:
    try:
        with open(path) as fh:
            import json
            return json.load(fh)
    except FileNotFoundError as e:
        raise NotFoundError("config_file", path) from e
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON in {path}: {e}", field_name="config") from e
```

## Retry Pattern

```python
import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

def retry(
    fn: Callable[..., T], *args,
    max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0,
    catch: tuple[type[Exception], ...] = (Exception,),
) -> T:
    """Retry a callable with exponential backoff."""
    last_error: Exception | None = None
    for attempt in range(1, max_attempts + 1):
        try:
            return fn(*args)
        except catch as e:
            last_error = e
            if attempt == max_attempts:
                break
            wait = delay * (backoff ** (attempt - 1))
            time.sleep(wait)
    raise last_error
```

## Result Pattern (No Exceptions)

```python
from dataclasses import dataclass
from typing import Generic, TypeVar

T = TypeVar("T")

@dataclass(frozen=True, slots=True)
class Success(Generic[T]):
    value: T

@dataclass(frozen=True, slots=True)
class Failure:
    error: str
    code: str = "ERROR"

type Result[T] = Success[T] | Failure

def parse_int(raw: str) -> Result[int]:
    try:
        return Success(int(raw))
    except ValueError:
        return Failure(f"Cannot parse '{raw}' as int", code="PARSE_ERROR")

# Usage with pattern matching
match parse_int("42"):
    case Success(value=v):
        print(f"Parsed: {v}")
    case Failure(error=msg):
        print(f"Error: {msg}")
```

## Common Mistakes

### Wrong

```python
try:
    result = compute()
except:  # bare except catches KeyboardInterrupt, SystemExit
    pass
```

### Correct

```python
try:
    result = compute()
except (ValueError, TypeError) as e:
    logger.warning("Computation failed: %s", e)
    result = default_value
```

## Exception Quick Reference

| Principle | Practice |
|-----------|----------|
| Catch specific | `except ValueError` not `except Exception` |
| Chain always | `raise NewError() from original` |
| Log at boundaries | Log once where you handle, not where you raise |
| Fail fast | Validate inputs early, raise immediately |
| Custom hierarchy | One base `AppError`, specific subclasses |
| Never bare except | Always specify exception type |

## See Also

- [Context Managers](../concepts/context-managers.md)
- [Clean Architecture](../patterns/clean-architecture.md)
- [File Parser](../patterns/file-parser.md)
