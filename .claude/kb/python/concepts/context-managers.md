# Context Managers

> **Purpose**: with statement, __enter__/__exit__, and contextlib patterns for resource management
> **Confidence**: 0.95
> **MCP Validated:** 2026-02-17

## Overview

Context managers guarantee resource cleanup through the `with` statement, ensuring that
`__exit__` is called even when exceptions occur. Python provides class-based
(`__enter__`/`__exit__`), decorator-based (`@contextmanager`), and async variants.

## The Pattern

```python
from contextlib import contextmanager
from collections.abc import Generator
import time


@contextmanager
def timer(label: str) -> Generator[None, None, None]:
    """Context manager that measures and prints elapsed time."""
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        print(f"{label}: {elapsed:.3f}s")


with timer("data processing"):
    data = [x ** 2 for x in range(1_000_000)]
```

## Class-Based Context Manager

```python
from types import TracebackType


class DatabaseConnection:
    def __init__(self, connection_string: str) -> None:
        self.connection_string = connection_string
        self.conn = None

    def __enter__(self) -> "DatabaseConnection":
        self.conn = self._connect(self.connection_string)
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self.conn:
            if exc_type is not None:
                self.conn.rollback()
            else:
                self.conn.commit()
            self.conn.close()
        return False  # do not suppress exceptions
```

## contextlib Utilities

| Utility | Purpose | Example |
|---------|---------|---------|
| `@contextmanager` | Generator-based CM | `yield` inside `try/finally` |
| `suppress(*exc)` | Ignore exceptions | `with suppress(FileNotFoundError):` |
| `redirect_stdout(f)` | Capture print output | `with redirect_stdout(buffer):` |
| `ExitStack()` | Dynamic CM management | Open variable number of files |
| `closing(thing)` | Call `.close()` on exit | `with closing(urlopen(url)):` |
| `nullcontext(val)` | No-op CM | Conditional context managers |

## ExitStack for Dynamic Resources

```python
from contextlib import ExitStack
from pathlib import Path


def merge_files(paths: list[Path], output: Path) -> None:
    with ExitStack() as stack:
        files = [stack.enter_context(open(p)) for p in paths]
        with open(output, "w") as out:
            for fh in files:
                out.write(fh.read())
```

## Common Mistakes

### Wrong (no cleanup on exception)

```python
fh = open("data.csv")
data = fh.read()  # if this raises, file is never closed
fh.close()
```

### Correct (guaranteed cleanup)

```python
with open("data.csv") as fh:
    data = fh.read()  # file always closed, even on exception
```

## Multiple Context Managers (3.10+)

```python
with (
    open("input.csv") as infile,
    open("output.csv", "w") as outfile,
):
    for line in infile:
        outfile.write(line.upper())
```

## Related

- [Generators](../concepts/generators.md)
- [File Parser](../patterns/file-parser.md)
- [Error Handling](../patterns/error-handling.md)
