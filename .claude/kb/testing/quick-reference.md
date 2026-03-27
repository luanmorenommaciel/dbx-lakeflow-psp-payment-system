# Python Testing Quick Reference

> Fast lookup tables. For code examples, see linked files.
> **MCP Validated:** 2026-03-26

## pytest CLI Commands (pytest 8+)

| Command | Purpose | Notes |
|---------|---------|-------|
| `pytest` | Run all tests | Auto-discovers test_*.py |
| `pytest tests/test_foo.py` | Run single file | Specific module |
| `pytest -k "name"` | Filter by name | Substring match |
| `pytest -m "marker"` | Filter by marker | Run marked tests only |
| `pytest -x` | Stop on first failure | Fast feedback |
| `pytest -v` | Verbose output | Show each test name |
| `pytest --tb=short` | Short tracebacks | Cleaner output |
| `pytest --co` | Collect only | List tests without running |
| `pytest -n auto` | Parallel execution | Requires pytest-xdist |
| `pytest --lf` | Re-run last failed | Fast iteration |
| `pytest --ff` | Failures first | Run failed first, then rest |
| `pytest --sw` | Stepwise | Stop on fail, resume next run |

## Fixture Scopes

| Scope | Lifecycle | Use Case |
|-------|-----------|----------|
| `function` | Per test (default) | Independent test state |
| `class` | Per test class | Shared class state |
| `module` | Per .py file | Expensive module setup |
| `package` | Per package | Package-level resources |
| `session` | Entire run | DB connections, Spark sessions |

## Mock Patterns

| Pattern | Syntax | Use Case |
|---------|--------|----------|
| Patch function | `@mock.patch("module.func")` | Replace a function |
| Patch method | `@mock.patch.object(Class, "method")` | Replace a method |
| Return value | `mock_obj.return_value = X` | Control output |
| Side effect | `mock_obj.side_effect = [1, 2, 3]` | Sequential returns |
| Exception | `mock_obj.side_effect = ValueError` | Simulate errors |
| Monkeypatch | `monkeypatch.setattr(obj, "attr", val)` | pytest-native patching |
| Env vars | `monkeypatch.setenv("KEY", "val")` | Override env variables |

## pytest-asyncio Patterns

| Pattern | Syntax | Notes |
|---------|--------|-------|
| Async test | `@pytest.mark.asyncio` + `async def test_x():` | Requires `pytest-asyncio` |
| Async fixture | `@pytest.fixture` + `async def setup():` | Supports yield for teardown |
| Auto mode | `asyncio_mode = "auto"` in pyproject.toml | All async tests auto-detected |
| Strict mode | `asyncio_mode = "strict"` | Requires explicit marker |
| Custom event loop | Override `event_loop` fixture | Session-scoped loop |

## Hypothesis (Property-Based Testing)

| Pattern | Syntax | Notes |
|---------|--------|-------|
| Basic | `@given(st.integers())` | Auto-generates test inputs |
| Text | `@given(st.text(min_size=1))` | Random strings |
| Lists | `@given(st.lists(st.integers()))` | Random lists |
| Composite | `@st.composite` | Build complex strategies |
| Assume | `assume(x > 0)` | Filter invalid inputs |
| Settings | `@settings(max_examples=500)` | Control test count |
| Reproduce | `@example(42)` | Pin specific case |

## Decision Matrix

| Use Case | Choose |
|----------|--------|
| Isolate a single function | Unit test + mock dependencies |
| Test API/DB interaction | Integration test + fixtures |
| Same logic, many inputs | `@pytest.mark.parametrize` |
| Reusable test objects | Factory fixtures |
| Test PySpark transforms | SparkSession fixture + DataFrame assertions |
| Slow/external resource | Mock it or mark with `@pytest.mark.slow` |
| Test async code | `pytest-asyncio` with `@pytest.mark.asyncio` |
| Find edge cases automatically | Hypothesis `@given` with strategies |
| Speed up CI test suite | `pytest-xdist` with `-n auto` |
| Hide param from test name | `pytest.HIDDEN_PARAM` (pytest 8.4+) |

## Common Pitfalls

| Don't | Do |
|-------|-----|
| Patch where defined | Patch where imported |
| Share mutable state across tests | Use function-scoped fixtures |
| Assert on mock without `assert_called` | Use `mock.assert_called_once_with(...)` |
| Hard-code test data everywhere | Use factory fixtures or parametrize |
| Skip edge cases (None, empty, huge) | Always test boundary conditions |
| `asyncio.run()` in tests | `@pytest.mark.asyncio` with async test |
| Manually write 100 edge cases | `@given(st.integers())` with Hypothesis |
| Sequential CI when tests are independent | `pytest -n auto` with xdist |

## Related Documentation

| Topic | Path |
|-------|------|
| pytest basics | `concepts/pytest-basics.md` |
| Fixture patterns | `concepts/fixtures.md` |
| Mocking deep dive | `concepts/mocking.md` |
| Full Index | `index.md` |
