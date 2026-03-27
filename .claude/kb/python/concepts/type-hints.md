# Type Hints

> **Purpose**: Type annotations, generics, Self, TypeIs, TypeForm, and Python 3.12-3.14 syntax
> **Confidence**: 0.95
> **MCP Validated:** 2026-03-26

## Overview

Python type hints enable static analysis, IDE support, and documentation without runtime
overhead. Python 3.10+ introduced `X | Y` union syntax, 3.11 added `Self` and
`LiteralString`, 3.12 introduced the `type` statement and native generic syntax,
3.13 added `TypeIs` and `TypeForm`, and 3.14 brings deferred annotation evaluation.

## The Pattern

```python
from dataclasses import dataclass
from typing import Self


@dataclass(slots=True)
class TreeNode:
    value: int
    children: list[Self] | None = None

    def add_child(self, value: int) -> Self:
        if self.children is None:
            self.children = []
        child = TreeNode(value=value)
        self.children.append(child)
        return child
```

## Modern Syntax Reference

| Old (pre-3.10) | Modern (3.10+) | Version |
|-----------------|----------------|---------|
| `Optional[str]` | `str \| None` | 3.10+ |
| `Union[int, str]` | `int \| str` | 3.10+ |
| `List[int]` | `list[int]` | 3.9+ |
| `Dict[str, int]` | `dict[str, int]` | 3.9+ |
| `Tuple[int, ...]` | `tuple[int, ...]` | 3.9+ |

## Python 3.11+ Features

```python
from typing import Self, LiteralString, Never

# Self: annotate methods returning their own class
class Builder:
    def set_name(self, name: str) -> Self:
        self.name = name
        return self

# LiteralString: must be a literal, not arbitrary string
def run_query(sql: LiteralString) -> list[dict]: ...

# Never: function that never returns
def fail(msg: str) -> Never:
    raise RuntimeError(msg)
```

## Python 3.12+ Generics

```python
# OLD: verbose TypeVar boilerplate
from typing import TypeVar
T = TypeVar("T")
def first_old(items: list[T]) -> T:
    return items[0]

# NEW: native generic syntax (3.12+)
def first[T](items: list[T]) -> T:
    return items[0]

# NEW: type alias statement (3.12+)
type Vector = list[float]
type Callback[T] = Callable[[T], None]
```

## TypedDict for Structured Dicts

```python
from typing import TypedDict, Required, NotRequired

class APIResponse(TypedDict):
    status: Required[int]
    data: Required[dict]
    error: NotRequired[str]
```

## Common Mistakes

### Wrong (legacy imports)

```python
from typing import Optional, List, Dict, Union
def process(items: Optional[List[Dict[str, Union[int, str]]]]) -> None: ...
```

### Correct (modern syntax)

```python
def process(items: list[dict[str, int | str]] | None) -> None: ...
```

## Protocols (Structural Typing)

```python
from typing import Protocol, runtime_checkable

@runtime_checkable
class Serializable(Protocol):
    def to_dict(self) -> dict: ...
    def to_json(self) -> str: ...

def save(obj: Serializable) -> None:
    data = obj.to_dict()
    ...
```

## Python 3.13+ TypeIs (PEP 742)

```python
from typing import TypeIs

# TypeIs narrows types in BOTH if and else branches
# (TypeGuard only narrowed the if branch)
def is_str_list(val: list[int | str]) -> TypeIs[list[str]]:
    return all(isinstance(x, str) for x in val)

def process(items: list[int | str]) -> None:
    if is_str_list(items):
        # Type checker knows: items is list[str]
        print(items[0].upper())
    else:
        # Type checker knows: items is list[int | str] (not narrowed away)
        pass
```

## Python 3.13+ TypeForm (PEP 747)

```python
from typing import TypeForm

# TypeForm lets you annotate type objects passed as values
def validate(data: object, expected: TypeForm) -> bool:
    return isinstance(data, expected)

validate("hello", str)    # OK
validate(42, int)          # OK
```

## Python 3.14+ Deferred Annotations (PEP 649)

```python
# In 3.14+, annotations are evaluated lazily by default.
# No more need for `from __future__ import annotations`.
# Forward references just work:
class Tree:
    left: Tree | None = None   # No quotes needed
    right: Tree | None = None

# Access annotations programmatically:
import annotationlib
annotations = annotationlib.get_annotations(Tree)
```

## Python 3.14+ Template Strings (PEP 750)

```python
from string.templatelib import Template

# t-strings create Template objects instead of strings
name = "world"
template: Template = t"Hello {name}"

# Templates give access to parts before rendering
# Useful for SQL injection prevention, HTML escaping, i18n
def safe_sql(template: Template) -> str:
    # Sanitize interpolated values before combining
    ...
```

## Quick Reference

| Type | Use Case | Version |
|------|----------|---------|
| `str \| None` | Optional value | 3.10+ |
| `Self` | Method returns own class | 3.11+ |
| `Never` | Function never returns | 3.11+ |
| `LiteralString` | SQL injection prevention | 3.11+ |
| `Literal["a", "b"]` | Enum-like string constraint | 3.8+ |
| `Protocol` | Structural subtyping (duck typing) | 3.8+ |
| `Annotated[int, Gt(0)]` | Type with metadata | 3.9+ |
| `TypeIs` | Narrow types in both branches | 3.13+ |
| `TypeForm` | Annotate type objects as values | 3.13+ |
| `type X = ...` | Type alias statement | 3.12+ |
| `def fn[T](x: T)` | Native generic syntax | 3.12+ |

## Related

- [Dataclasses](../concepts/dataclasses.md)
- [Clean Architecture](../patterns/clean-architecture.md)
- [Error Handling](../patterns/error-handling.md)
