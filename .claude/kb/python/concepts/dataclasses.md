# Dataclasses

> **Purpose**: @dataclass decorator patterns with slots, frozen, kw_only for Python 3.11+
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

Python dataclasses provide a declarative way to create data containers with automatic
`__init__`, `__repr__`, and `__eq__` generation. Python 3.10+ added `slots`, `kw_only`,
and `match_args` parameters. Combining `frozen=True` with `slots=True` produces immutable,
memory-efficient, hashable objects suitable for configs, DTOs, and value objects.

## The Pattern

```python
from dataclasses import dataclass, field
from datetime import datetime
from typing import Self


@dataclass(frozen=True, slots=True)
class Metric:
    """Immutable, memory-efficient metric data point."""
    name: str
    value: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    tags: tuple[str, ...] = ()

    def with_tag(self, tag: str) -> Self:
        """Return new Metric with additional tag (immutable pattern)."""
        return Metric(
            name=self.name,
            value=self.value,
            timestamp=self.timestamp,
            tags=(*self.tags, tag),
        )
```

## Decorator Parameters

| Parameter | Default | Effect |
|-----------|---------|--------|
| `slots=True` | `False` | Generates `__slots__`, 15-20% less memory |
| `frozen=True` | `False` | Immutable instances, enables hashing |
| `kw_only=True` | `False` | Forces keyword-only constructor arguments |
| `order=True` | `False` | Generates `__lt__`, `__le__`, `__gt__`, `__ge__` |
| `match_args=True` | `True` | Generates `__match_args__` for pattern matching |
| `eq=True` | `True` | Generates `__eq__` and `__ne__` |

## Field Options

```python
from dataclasses import dataclass, field


@dataclass(slots=True)
class Pipeline:
    name: str
    steps: list[str] = field(default_factory=list)
    max_retries: int = field(default=3, repr=False)
    _internal: str = field(default="", init=False, repr=False)
```

| Field Parameter | Purpose |
|----------------|---------|
| `default_factory` | Callable for mutable defaults (list, dict, set) |
| `repr=False` | Exclude from `__repr__` output |
| `init=False` | Exclude from `__init__`, set in `__post_init__` |
| `compare=False` | Exclude from equality comparisons |
| `hash=False` | Exclude from hash computation |
| `kw_only=True` | Make this specific field keyword-only |

## Common Mistakes

### Wrong (mutable default)

```python
@dataclass
class Config:
    items: list[str] = []  # BUG: shared mutable default
```

### Correct (default_factory)

```python
@dataclass(slots=True)
class Config:
    items: list[str] = field(default_factory=list)
```

## Pattern Matching (3.10+)

```python
from dataclasses import dataclass


@dataclass(slots=True)
class Point:
    x: float
    y: float


@dataclass(slots=True)
class Circle:
    center: Point
    radius: float


def describe(shape) -> str:
    match shape:
        case Circle(center=Point(0, 0), radius=r):
            return f"Circle at origin with radius {r}"
        case Circle(center=c, radius=r) if r > 10:
            return f"Large circle at ({c.x}, {c.y})"
        case Point(x, y):
            return f"Point at ({x}, {y})"
        case _:
            return "Unknown shape"
```

## Post-Init Processing

```python
@dataclass(slots=True)
class FilePath:
    raw: str
    resolved: str = field(init=False)

    def __post_init__(self) -> None:
        self.resolved = self.raw.strip().replace("\\", "/")
```

## When to Use What

| Need | Use |
|------|-----|
| Plain data container | `@dataclass(slots=True)` |
| Immutable value object | `@dataclass(frozen=True, slots=True)` |
| Data with runtime validation | Pydantic `BaseModel` |
| Dict-like typed structure | `TypedDict` |
| Named constant group | `enum.Enum` |
| Advanced attrs features (converters, on_setattr) | `attrs` with `@define` |

## Dataclass vs attrs vs Pydantic (2025)

| Feature | dataclass | attrs | Pydantic |
|---------|-----------|-------|----------|
| Stdlib (no install) | Yes | No | No |
| Slots | `slots=True` | `@define` (default) | No (uses `__dict__`) |
| Runtime validation | No | Opt-in validators | Yes (automatic) |
| Type coercion | No | Opt-in converters | Yes (automatic) |
| JSON serialization | No | `cattrs` needed | Built-in |
| Performance (creation) | Fastest | Fast | Slower (validation cost) |
| JSON Schema | No | No | Built-in |
| Best for | Internal data, DTOs | Complex internal data | API boundaries, LLM output |

```python
# attrs comparison -- @define uses slots by default
import attrs

@attrs.define
class Metric:
    name: str
    value: float = attrs.field(validator=attrs.validators.gt(0))
    tags: tuple[str, ...] = ()
```

## Related

- [Type Hints](../concepts/type-hints.md)
- [Clean Architecture](../patterns/clean-architecture.md)
- [Functional Patterns](../patterns/functional-patterns.md)
