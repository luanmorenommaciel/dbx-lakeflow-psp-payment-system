# Generators

> **Purpose**: Generator functions, yield, send, and generator expressions for lazy evaluation
> **Confidence**: 0.95
> **MCP Validated:** 2026-02-17

## Overview

Generators are functions that use `yield` to produce a sequence of values lazily, one at
a time, without loading the entire dataset into memory. They implement the iterator
protocol automatically. Generator expressions provide inline syntax for simple cases.

## The Pattern

```python
from collections.abc import Generator
from pathlib import Path


def read_lines(path: Path) -> Generator[str, None, None]:
    """Yield non-empty, stripped lines from a file."""
    with open(path) as fh:
        for line in fh:
            stripped = line.strip()
            if stripped:
                yield stripped


def filter_comments(lines: Generator[str, None, None]) -> Generator[str, None, None]:
    for line in lines:
        if not line.startswith("#"):
            yield line


# Pipeline: compose generators without loading file into memory
clean_lines = filter_comments(read_lines(Path("config.txt")))
```

## Generator vs Iterator vs Iterable

| Type | Has `__iter__` | Has `__next__` | Reusable |
|------|:-:|:-:|:-:|
| Iterable (list, str) | Yes | No | Yes |
| Iterator | Yes | Yes | No |
| Generator | Yes | Yes | No |

## Generator Expression vs List Comprehension

```python
squares_list = [x ** 2 for x in range(1_000_000)]  # ~8MB in memory
squares_gen = (x ** 2 for x in range(1_000_000))    # ~120 bytes
```

## yield from (Delegation)

```python
from collections.abc import Generator


def flatten(nested: list[list[int]]) -> Generator[int, None, None]:
    for sublist in nested:
        yield from sublist

result = list(flatten([[1, 2], [3, 4], [5]]))  # [1, 2, 3, 4, 5]
```

## send() and Generator Protocol

```python
from collections.abc import Generator


def accumulator() -> Generator[float, float, str]:
    """Generator that accepts values via send() and tracks running total."""
    total = 0.0
    while True:
        value = yield total
        if value is None:
            break
        total += value
    return f"Final total: {total}"
```

## Type Hints for Generators

```python
from collections.abc import Generator, Iterator

# Simple generator (yield only)
def count_up(n: int) -> Iterator[int]:
    for i in range(n):
        yield i

# Full protocol: Generator[YieldType, SendType, ReturnType]
def stateful() -> Generator[str, int, None]:
    value = yield "ready"
    yield f"received: {value}"
```

## Common Mistakes

### Wrong (loading everything into memory)

```python
def get_all_records(db) -> list[dict]:
    return db.fetch_all()  # loads millions of rows
```

### Correct (streaming with generator)

```python
from collections.abc import Iterator

def stream_records(db) -> Iterator[dict]:
    cursor = db.execute("SELECT * FROM records")
    while row := cursor.fetchone():
        yield dict(row)
```

## Generator Pipelines

```python
from collections.abc import Iterator

def read_csv_rows(path: str) -> Iterator[list[str]]:
    with open(path) as fh:
        for line in fh:
            yield line.strip().split(",")

def parse_amounts(rows: Iterator[list[str]]) -> Iterator[float]:
    for row in rows:
        try:
            yield float(row[2])
        except (IndexError, ValueError):
            continue

# Compose pipeline -- nothing executes until iteration
total = sum(parse_amounts(read_csv_rows("transactions.csv")))
```

## Related

- [File Parser](../patterns/file-parser.md)
- [Context Managers](../concepts/context-managers.md)
- [Functional Patterns](../patterns/functional-patterns.md)
