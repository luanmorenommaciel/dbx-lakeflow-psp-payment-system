# Python Clean Code Knowledge Base

> **Purpose**: Clean code patterns for Python 3.11+ -- dataclasses, type hints, generators, async, uv
> **MCP Validated:** 2026-03-26

## Quick Navigation

### Concepts (< 150 lines each)

| File | Purpose |
|------|---------|
| [concepts/dataclasses.md](concepts/dataclasses.md) | @dataclass with slots, frozen, kw_only, field factories |
| [concepts/type-hints.md](concepts/type-hints.md) | Type annotations, generics, Self, TypeVar, TypeIs, 3.12-3.14 syntax |
| [concepts/generators.md](concepts/generators.md) | Generator functions, yield, send, generator expressions |
| [concepts/context-managers.md](concepts/context-managers.md) | with statement, __enter__/__exit__, contextlib |

### Patterns (< 200 lines each)

| File | Purpose |
|------|---------|
| [patterns/file-parser.md](patterns/file-parser.md) | File parsing with generators and context managers |
| [patterns/clean-architecture.md](patterns/clean-architecture.md) | Clean code structure, naming, module organization, uv |
| [patterns/error-handling.md](patterns/error-handling.md) | Exception hierarchy, custom errors, recovery patterns |
| [patterns/functional-patterns.md](patterns/functional-patterns.md) | Comprehensions, map, filter, reduce, functools |

### Specs (Machine-Readable)

| File | Purpose |
|------|---------|
| [specs/python-standards.yaml](specs/python-standards.yaml) | Code standards, linting rules, project conventions |

---

## Quick Reference

- [quick-reference.md](quick-reference.md) - Fast lookup tables

---

## Key Concepts

| Concept | Description |
|---------|-------------|
| **Dataclasses** | Declarative data containers with validation, immutability, and slots |
| **Type Hints** | Static typing with generics, unions, TypeIs, TypeForm, 3.12-3.14 syntax |
| **Generators** | Lazy evaluation for memory-efficient data processing |
| **Context Managers** | Resource lifecycle management with guaranteed cleanup |
| **Python 3.13** | Free-threading (no-GIL experimental), improved REPL, TypeIs, TypeForm |
| **Python 3.14** | Free-threading officially supported, JIT compiler, t-strings, deferred annotations |
| **uv** | Rust-based package/project manager replacing pip, pyenv, poetry, virtualenv |

---

## Learning Path

| Level | Files |
|-------|-------|
| **Beginner** | concepts/dataclasses.md, concepts/type-hints.md |
| **Intermediate** | concepts/generators.md, patterns/clean-architecture.md |
| **Advanced** | patterns/file-parser.md, patterns/functional-patterns.md |

---

## Agent Usage

| Agent | Primary Files | Use Case |
|-------|---------------|----------|
| python-developer | All files | Write clean, idiomatic Python 3.11-3.14 code |
| code-reviewer | patterns/clean-architecture.md, specs/python-standards.yaml | Review code quality |
| test-generator | patterns/error-handling.md, concepts/dataclasses.md | Generate typed test fixtures |
