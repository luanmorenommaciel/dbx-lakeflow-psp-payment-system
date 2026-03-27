# Functional Patterns

> **Purpose**: Comprehensions, map, filter, reduce, functools for clean data transformations
> **MCP Validated:** 2026-02-17

## When to Use

- Transforming collections without mutation
- Building declarative data processing pipelines
- Replacing verbose loops with concise expressions
- Composing small functions into complex operations

## Implementation

### Comprehensions

```python
# List comprehension: transform + filter
names = [user["name"].title() for user in users if user["active"]]

# Dict comprehension: build lookup
user_map = {u["id"]: u["name"] for u in users}

# Set comprehension: unique values
domains = {email.split("@")[1] for email in emails}

# Nested comprehension (keep it simple -- max 2 levels)
flat = [cell for row in matrix for cell in row]

# Conditional expression in comprehension
labels = ["pass" if score >= 70 else "fail" for score in scores]
```

### Generator Expressions in Functions

```python
# sum, any, all, min, max accept generator expressions directly
total = sum(item.price * item.qty for item in order.items)
has_errors = any(r.status == "error" for r in results)
all_valid = all(len(name) > 0 for name in names)
```

## Map, Filter, Reduce

```python
from functools import reduce

amounts = list(map(float, raw_amounts))
positive = list(filter(lambda x: x > 0, amounts))
total = reduce(lambda acc, x: acc + x, amounts, 0.0)
```

### When to Prefer Comprehensions Over map/filter

| Situation | Use | Why |
|-----------|-----|-----|
| Simple transform | List comprehension | More readable |
| Existing named function | `map(fn, items)` | Cleaner than `[fn(x) for x in items]` |
| Transform + filter | List comprehension | Single expression |
| Need lazy evaluation | Generator expression | Memory efficient |

## functools Essentials

### lru_cache (Memoization)

```python
from functools import lru_cache

@lru_cache(maxsize=256)
def fibonacci(n: int) -> int:
    if n < 2:
        return n
    return fibonacci(n - 1) + fibonacci(n - 2)
```

### partial (Function Currying)

```python
from functools import partial

def power(base: float, exponent: float) -> float:
    return base ** exponent

square = partial(power, exponent=2)
cube = partial(power, exponent=3)
squares = list(map(square, range(10)))
```

## Pipeline Pattern

```python
from functools import reduce
from typing import TypeVar
from collections.abc import Callable

T = TypeVar("T")

def pipe(value: T, *functions: Callable) -> T:
    """Apply a sequence of functions to a value, left to right."""
    return reduce(lambda v, fn: fn(v), functions, value)

clean = pipe("  Hello, World!  ", str.strip, str.lower)
```

## Immutable Transformations

```python
from dataclasses import dataclass

@dataclass(frozen=True, slots=True)
class Order:
    items: tuple[str, ...]
    total: float

def add_item(order: Order, item: str, price: float) -> Order:
    return Order(items=(*order.items, item), total=order.total + price)

def apply_discount(order: Order, percent: float) -> Order:
    return Order(items=order.items, total=round(order.total * (1 - percent / 100), 2))

order = Order(items=(), total=0.0)
order = add_item(order, "Widget", 29.99)
order = apply_discount(order, 10)
```

## itertools Highlights

```python
from itertools import chain, groupby, islice, batched

all_items = list(chain(list_a, list_b, list_c))

from operator import itemgetter
sorted_data = sorted(records, key=itemgetter("category"))
for category, group in groupby(sorted_data, key=itemgetter("category")):
    print(f"{category}: {len(list(group))} items")

first_10 = list(islice(huge_generator(), 10))

# batched: split into chunks (3.12+)
for chunk in batched(range(10), 3):
    print(chunk)  # (0,1,2), (3,4,5), (6,7,8), (9,)
```

## Common Mistakes

### Wrong (mutation-heavy)

```python
results = []
for item in items:
    if item.active:
        results.append(item.name.upper())
```

### Correct (declarative)

```python
results = [item.name.upper() for item in items if item.active]
```

## See Also

- [Generators](../concepts/generators.md)
- [Dataclasses](../concepts/dataclasses.md)
- [File Parser](../patterns/file-parser.md)
