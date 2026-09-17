# FunctionManager

The `FunctionManager` maintains a catalogue of executable Python functions, split into two categories:

1. **Primitives** – System action methods (the `primitives.*` namespaces) exposed for direct invocation
2. **Compositional Functions** – User-specific functions stored with their source code

## Architecture

### Two Separate Contexts

Functions are stored in two dedicated Unify contexts to ensure stable IDs:

| Context | Purpose | ID Assignment |
|---------|---------|---------------|
| `Functions/Primitives` | System primitives | Explicit stable IDs (code-defined) |
| `Functions/Compositional` | User-specific functions | Auto-incrementing (store-managed) |

This separation guarantees:
- Primitive IDs are consistent across all users
- Compositional function IDs are never affected by primitive changes
- No ID collisions between the two namespaces

### Primitive ID Stability

Primitives receive stable IDs derived from a hash of their fully-qualified name (e.g., "_ActorRunner.act" → deterministic integer). This means:
- IDs are consistent across all deployments
- Adding/removing methods doesn't affect other primitives' IDs
- No manual ID management required

Primitive methods are auto-discovered from `@abstractmethod` definitions on base classes, or from an explicit `_PRIMITIVE_METHODS` constant, minus an explicit exclusion list for non-primitive methods like `clear()`.

---

## The `primitives` Object

The `primitives` object provides lazy access to the primitive namespaces. Instantiation only happens when accessed:

```python
async def delegate(topic: str):
    # The actor runner is constructed on first access
    handle = await primitives.actor.act(request=f"Research {topic}")
    return await handle.result()
```

### Available Properties

| Property | Class | Methods |
|----------|-------|---------|
| `primitives.actor` | `_ActorRunner` | `act` |

Guidance is **not** a primitive. It is a typed catalogue exposed as top-level Actor JSON tools (`GuidanceManager_*`).

---

## Primitive Synchronization

Primitives are lazily synchronized to the database:

1. On first access (e.g., `list_primitives()`, `search_functions()`), the manager calls `sync_primitives()`
2. A hash of all primitive signatures/docstrings is compared against the stored hash
3. If changed, all primitives are deleted and re-inserted with their stable IDs
4. The hash is stored in `Functions/Meta` for future comparisons

This ensures primitives stay in sync with the codebase while avoiding unnecessary database writes.

---

## Writing Compositional Functions

Compositional functions are stored with their full source code. They may be created by:
- The Actor generating and saving a function during execution
- Pre-provisioning functions for a specific user/client
- Direct API calls to `add_functions()`

### Format Requirements

Each implementation string must contain **exactly one function definition** starting at column 0:

```python
# ✓ Correct
async def my_function(path: str):
    import csv
    with open(path) as fh:
        return sum(1 for _ in csv.reader(fh))

# ✗ Wrong - indented
    async def my_function():
        pass

# ✗ Wrong - multiple functions
def helper():
    pass

def main():
    helper()
```

### Imports: Optional (Pre-Injected Globals Available)

Functions are executed in a sandboxed environment with pre-injected globals. Common modules are already available, but **imports do work** if needed:

```python
# ✓ Works - using pre-injected globals (preferred for common modules)
async def example_no_import():
    data = json.dumps({"key": "value"})
    return data

# ✓ Also works - explicit import (useful for non-standard packages)
async def example_with_import():
    import numpy as np
    x = np.array([1, 2, 3])
    return x.sum()
```

The sandbox includes `__import__`, so any package installed in the environment can be imported.

### Best Practice: Domain Types & Type Hints (Avoid Surprise `NameError`s)

Compositional functions are often used by the Actor/CodeActActor by **retrieving a callable** (e.g. via `search_functions(..., return_callable=True)`) and executing it in a fresh sandbox namespace.

Here’s the simple rule:
- If you reference a symbol in the **function body**, **import/define it** in the function (don’t assume it exists in globals).
- If you reference a symbol only in **annotations** (including forward-ref strings), that’s usually fine.

```python
# ✅ OK: "User" only appears in the annotation (as a forward-ref string).
# The function body doesn't need the User symbol at runtime.
async def greet(user: "User") -> str:
    return f"Hello {user.name}"


# ⚠️ NOT OK: Role is used at runtime, so it MUST exist (import/define it).
async def is_admin(role: "Role") -> bool:
    return role == Role.ADMIN


# ✅ Preferred: import the runtime type inside the function.
async def is_admin(role: "Role") -> bool:
    from my_app.types import Role
    return role == Role.ADMIN
```

Note: if your code resolves type hints at runtime (e.g. `typing.get_type_hints(...)` or Pydantic model building),
then all referenced names must be resolvable. `FunctionManager` makes annotation-resolution more robust, but it cannot
guess the *real* domain objects for runtime logic—imports/definitions are the reliable solution.

#### Pre-Injected by Sandbox

These are always available (from `create_execution_globals()`):

| Category | Available Names |
|----------|-----------------|
| **Builtins** | `print`, `len`, `str`, `int`, `float`, `list`, `dict`, `set`, `range`, `isinstance`, `issubclass`, `hasattr`, `getattr`, `enumerate`, `zip`, `sorted`, `min`, `max`, `sum`, `any`, `all`, etc. |
| **Modules** | `asyncio`, `re`, `json`, `datetime`, `collections`, `statistics`, `functools` |
| **Typing** | `typing`, `Any`, `Callable`, `Dict`, `List`, `Optional`, `Tuple`, `Set`, `Union`, `Literal` |
| **Pydantic** | `pydantic`, `BaseModel`, `Field` |
| **Primitives** | `primitives` – lazy access to the primitive namespaces |
| **Steerable** | `SteerableToolHandle` |

#### Injected by Actor at Runtime

When functions are executed via an Actor (`CodeActActor`), additional objects are injected:

| Name | Description |
|------|-------------|
| `request_clarification` | Ask the user for clarification during execution |

---

## Example: Complete Function

```python
async def summarize_csv(path: str, question: str) -> str:
    """
    Answer a question about a CSV file with a focused LLM call.

    Args:
        path: Absolute path of the CSV file in the workspace.
        question: What to answer about the rows.

    Returns:
        The model's answer, grounded in the file's first rows.
    """
    import csv

    with open(path, newline="") as fh:
        rows = list(csv.DictReader(fh))[:200]

    return await query_llm(
        f"Rows:\n{json.dumps(rows)}\n\nQuestion: {question}",
        temperature=0.0,
    )
```

---

## Verification and Trust

Every compositional function carries a **verification ledger** (`unify/function_manager/verification/`):

- **Effect class** — detected from the AST as a lower bound (`classify.py`): the maximum over the primitives it calls (`PRIMITIVE_EFFECT_CLASSES` covers every primitive; unknown names are `unsafe_effectful` and logged), the classes of its compositional dependencies, and its third-party imports. `safe_noop` (pure) < `read_only` < `idempotent_effectful` < `unsafe_effectful`. A librarian may confirm a class within that bound via `confirm_side_effect_class` (raise freely, lower only to the detected bound).
- **Trust hash** (`ledger.function_trust_hash`) — over the normalised source, every compositional dependency's own trust hash, the venv and its pyproject, and the language. Any component changing changes the hash and invalidates trust, dependents included.
- **Contract** (`contracts.py`) — JSON schemas for inputs and output derived from type hints, plus postconditions authored via `add_functions(contracts={name: {"postconditions": [...]}})` (boolean expressions over `result` and `kwargs`, restricted to an allowlist). Tier-0 checks run on every call while untrusted and stay on for trusted read/effectful functions.
- **Fixtures** (`fixtures.py`) — recorded `(args, result)` pairs for `safe_noop` functions, captured on passing calls or authored via `add_functions(fixtures=...)`, replayed whenever the function's content changes; a mismatch rejects the change with `FixtureRegressionError`.
- **Ledger** — one append-only row per verdict in `Functions/Verifications` (`record_verification`); the per-function summary is refolded from the rows for the current hash on every write, and `verify` is derived from it by `policy.derive_verify` — the only writer of that flag. `set_verification_policy` lets a librarian raise the bar for a function; nothing lowers it.

Rows stored before the ledger existed are classified once on first read.

## Steerable Functions

Compositional functions can optionally return a **steerable handle** instead of a final result. This allows the calling layer (e.g., `CodeActActor`) to forward steering operations (interject, pause, stop) into the running function.

### What is a Steerable Function?

A steerable function is one that:
1. Starts a background task (e.g., an async tool loop, an actor)
2. Returns a `SteerableToolHandle` immediately (before the task completes)
3. Allows the caller to interact with the running task via the handle

### Runtime Detection

Steerability is detected at **runtime** via `isinstance(result, SteerableToolHandle)`:

```python
from unity.common.async_tool_loop import SteerableToolHandle

result = await my_function()

if isinstance(result, SteerableToolHandle):
    # Function returned a steerable handle - can forward steering operations
    await result.interject("Please also check for errors")
    final_result = await result.result()
else:
    # Function returned a plain value - no steering possible
    final_result = result
```

### Writing a Steerable Function

Return a handle from a nested actor:

```python
async def research(topic: str) -> SteerableToolHandle:
    """
    Start a research sub-task and return its handle.

    The caller can interject, pause, or stop the research while it runs.
    """
    return await primitives.actor.act(request=f"Research {topic}")
```

### Available Infrastructure

These are injected into the execution globals by `create_execution_globals()`:

| Name | Purpose |
|------|---------|
| `SteerableToolHandle` | Base ABC for steerable handles and runtime `isinstance` checks |

### Handle Methods

Steerable handles provide these methods:

| Method | Description |
|--------|-------------|
| `await handle.result()` | Wait for and return the final result |
| `await handle.interject(message)` | Inject a message into the running task |
| `await handle.pause()` | Pause the task (in-flight operations continue) |
| `await handle.resume()` | Resume a paused task |
| `handle.stop(reason)` | Cancel the task immediately |
| `await handle.ask(question)` | Query the task's status without modifying it |

### Example: Delegated Actor Workflow

```python
async def delegated_research(topic: str) -> SteerableToolHandle:
    """
    Delegate research to an actor and return the running handle.

    The caller can interject to refine the search, or stop early
    if enough information has been gathered.
    """
    return await primitives.actor.act(
        request=f"Research the following topic thoroughly: {topic}",
        prompt_functions=["summarize_csv"],
        can_store=False,
        timeout=300,
    )
```

### Non-Steerable Functions

Regular functions that return plain values are **not** steerable:

```python
async def word_count(path: str) -> int:
    """A simple function - returns a plain value, not steerable."""
    with open(path) as fh:
        return len(fh.read().split())  # Plain int, not a handle
```

The execution layer will detect this via `isinstance` and handle it normally.

---

## API Summary

### Primitives

```python
fm = FunctionManager()

# Ensure primitives are synced
fm.sync_primitives()

# List all primitives
fm.list_primitives()

# Search includes primitives by default
fm.search_functions(query="delegate a sub-task", include_primitives=True)
```

### Compositional Functions

```python
# Add functions
fm.add_functions(implementations=["async def foo(): pass"])

# List functions
fm.list_functions(include_implementations=False)

# Search by similarity
fm.search_functions(query="csv summary", n=5)

# Delete
fm.delete_function(function_id=1)
```
