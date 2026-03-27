# Python Clean Code Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-03-26

## Dataclass Options (3.10+)

| Parameter | Default | Effect |
|-----------|---------|--------|
| `slots=True` | `False` | Generate `__slots__`, faster attribute access, less memory |
| `frozen=True` | `False` | Immutable instances, hashable |
| `kw_only=True` | `False` | All fields require keyword arguments |
| `match_args=True` | `True` | Enable structural pattern matching |
| `order=True` | `False` | Generate comparison methods |
| `eq=True` | `True` | Generate `__eq__` and `__ne__` |

## Type Hint Patterns (3.11-3.14)

| Pattern | Syntax | Version |
|---------|--------|---------|
| Union (modern) | `int \| str` | 3.10+ |
| Optional (modern) | `str \| None` | 3.10+ |
| Self return | `-> Self` | 3.11+ |
| TypeVar (modern) | `def fn[T](x: T) -> T:` | 3.12+ |
| Type alias (modern) | `type Vector = list[float]` | 3.12+ |
| TypedDict | `class Config(TypedDict):` | 3.8+ |
| Literal | `Literal["read", "write"]` | 3.8+ |
| TypeIs (narrow) | `def is_str(v: object) -> TypeIs[str]:` | 3.13+ |
| TypeForm | `def check(t: TypeForm) -> bool:` | 3.13+ |
| TypeVar defaults | `class C[T = int]:` | 3.13+ |
| Deferred annotations | Annotations evaluated lazily (PEP 649) | 3.14+ |

## Generator vs List Comprehension

| Use Case | Choose | Why |
|----------|--------|-----|
| Need all items in memory | List comprehension `[x for x in items]` | Random access |
| Large/infinite dataset | Generator expression `(x for x in items)` | Lazy evaluation |
| Transform + filter pipeline | Generator chaining | Memory efficient |
| Need to iterate once | Generator | Lower memory |
| Need `len()` or indexing | List | Generators have no length |

## Context Manager Patterns

| Pattern | Use Case | Module |
|---------|----------|--------|
| `with open(f) as fh:` | File I/O | builtin |
| `@contextmanager` | Simple resource management | `contextlib` |
| `class + __enter__/__exit__` | Complex state machines | builtin |
| `suppress(ExceptionType)` | Ignore specific exceptions | `contextlib` |
| `ExitStack()` | Dynamic number of resources | `contextlib` |

## Python 3.13-3.14 New Features

| Feature | Version | Description |
|---------|---------|-------------|
| Free-threading (no-GIL) | 3.13 exp, 3.14 supported | True multi-core parallelism with `python3.14t` build |
| JIT compiler | 3.13 exp, 3.14 exp | Copy-and-patch JIT for performance gains |
| Template strings (t-strings) | 3.14+ | `t"Hello {name}"` returns `Template` object for custom processing |
| Deferred annotations (PEP 649) | 3.14+ | Annotations evaluated lazily, reduces import-time overhead |
| `TypeIs` (PEP 742) | 3.13+ | Narrow types in both if/else branches (replaces TypeGuard) |
| `TypeForm` (PEP 747) | 3.13+ | Annotate type forms passed as values |
| Improved REPL | 3.13+ | Multi-line editing, color output, better history |
| `annotationlib` module | 3.14+ | Programmatic access to deferred annotations |

## uv Package Manager

| Command | Purpose |
|---------|---------|
| `uv init myproject` | Create new project with `pyproject.toml` |
| `uv add requests` | Add dependency |
| `uv add pytest --dev` | Add dev dependency |
| `uv sync` | Install/sync all dependencies |
| `uv lock` | Create/update lockfile |
| `uv run pytest` | Run command in project environment |
| `uv python install 3.14` | Install Python version |
| `uv python pin 3.13` | Pin project Python version |
| `uv tool install ruff` | Install CLI tool globally |
| `uv run script.py` | Run standalone script with inline deps |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Plain data container | `@dataclass(slots=True)` |
| Immutable config | `@dataclass(frozen=True, slots=True)` |
| Data with validation | Pydantic BaseModel |
| Data container + attrs features | `attrs` with `@define` |
| Return type is self | `-> Self` (3.11+) |
| Parse large file line by line | Generator with `yield` |
| Manage resource lifecycle | Context manager |
| Chain transformations | Generator pipeline |
| Catch specific errors only | `except SpecificError` |
| Narrow types in conditionals | `TypeIs` (3.13+, prefer over TypeGuard) |
| Package management | `uv` (replaces pip, poetry, pyenv) |
| True multi-threading | Free-threaded build `python3.14t` |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| `except Exception:` (bare) | `except (ValueError, TypeError) as e:` |
| Mutable default `field=[]` | `field(default_factory=list)` |
| `type(x) == str` | `isinstance(x, str)` |
| `from typing import Optional` | `str \| None` (3.10+) |
| Return `None` implicitly | Use explicit return type `-> None` |
| Nested list comprehensions (3+) | Extract to named generator function |
| `pip install` + `venv` manually | `uv add` + `uv sync` (faster, unified) |
| `TypeGuard` for narrowing | `TypeIs` (3.13+, narrows both branches) |
| `threading` for CPU-bound work | Free-threaded build or `multiprocessing` |

## Related Documentation

| Topic | Path |
|-------|------|
| Dataclasses deep dive | `concepts/dataclasses.md` |
| Type hints guide | `concepts/type-hints.md` |
| Error handling patterns | `patterns/error-handling.md` |
| Full Index | `index.md` |
