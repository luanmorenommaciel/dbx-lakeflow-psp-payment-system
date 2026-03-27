# File Parser

> **Purpose**: File parsing patterns using generators and context managers for memory-efficient processing
> **MCP Validated:** 2026-02-17

## When to Use

- Processing large files that do not fit in memory
- Streaming CSV, JSON Lines, or log files
- Building ETL pipelines with transformation stages

## Implementation

```python
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
import csv
import json


@dataclass(frozen=True, slots=True)
class ParsedRecord:
    """Immutable record from file parsing."""
    source: str
    line_number: int
    data: dict[str, str]


def parse_csv(path: Path) -> Iterator[ParsedRecord]:
    """Stream CSV rows as ParsedRecord objects."""
    with open(path, newline="") as fh:
        reader = csv.DictReader(fh)
        for line_num, row in enumerate(reader, start=2):
            yield ParsedRecord(source=str(path), line_number=line_num, data=dict(row))


def parse_jsonl(path: Path) -> Iterator[ParsedRecord]:
    """Stream JSON Lines file as ParsedRecord objects."""
    with open(path) as fh:
        for line_num, line in enumerate(fh, start=1):
            stripped = line.strip()
            if stripped:
                yield ParsedRecord(source=str(path), line_number=line_num, data=json.loads(stripped))


def parse_log(path: Path, delimiter: str = " | ") -> Iterator[ParsedRecord]:
    """Stream structured log file with custom delimiter."""
    with open(path) as fh:
        for line_num, line in enumerate(fh, start=1):
            parts = line.strip().split(delimiter)
            if len(parts) >= 3:
                yield ParsedRecord(
                    source=str(path), line_number=line_num,
                    data={"timestamp": parts[0], "level": parts[1], "message": delimiter.join(parts[2:])},
                )
```

## Transformation Pipeline

```python
from collections.abc import Iterator


def filter_records(records: Iterator[ParsedRecord], key: str, value: str) -> Iterator[ParsedRecord]:
    for record in records:
        if record.data.get(key) == value:
            yield record


def batch_records(records: Iterator[ParsedRecord], batch_size: int = 100) -> Iterator[list[ParsedRecord]]:
    batch: list[ParsedRecord] = []
    for record in records:
        batch.append(record)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch
```

## Example Usage

```python
from pathlib import Path

def process_sales_report(input_path: Path, output_path: Path) -> int:
    records = parse_csv(input_path)
    active = filter_records(records, key="status", value="active")
    count = 0
    with open(output_path, "w") as out:
        for batch in batch_records(active, batch_size=50):
            for record in batch:
                out.write(json.dumps(record.data) + "\n")
                count += 1
    return count
```

## Multi-Format Parser

```python
from pathlib import Path
from collections.abc import Iterator

PARSERS = {".csv": parse_csv, ".jsonl": parse_jsonl, ".log": parse_log}

def auto_parse(path: Path) -> Iterator[ParsedRecord]:
    parser = PARSERS.get(path.suffix.lower())
    if parser is None:
        raise ValueError(f"No parser registered for {path.suffix}")
    yield from parser(path)

def parse_directory(directory: Path, pattern: str = "*") -> Iterator[ParsedRecord]:
    for path in sorted(directory.glob(pattern)):
        if path.suffix.lower() in PARSERS:
            yield from auto_parse(path)
```

## Error Recovery

```python
import logging
from collections.abc import Iterator

logger = logging.getLogger(__name__)

def safe_parse(path: Path) -> Iterator[ParsedRecord]:
    with open(path) as fh:
        for line_num, line in enumerate(fh, start=1):
            try:
                data = json.loads(line.strip())
                yield ParsedRecord(source=str(path), line_number=line_num, data=data)
            except json.JSONDecodeError as e:
                logger.warning("Skipping line %d in %s: %s", line_num, path, e)
                continue
```

## See Also

- [Generators](../concepts/generators.md)
- [Context Managers](../concepts/context-managers.md)
- [Error Handling](../patterns/error-handling.md)
