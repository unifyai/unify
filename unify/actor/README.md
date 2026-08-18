# Actor Module

This directory contains different "actor" implementations, which are responsible for taking a high-level user goal and breaking it down into a series of actions to be executed.

## Available Actors

### CodeActActor

A conversational actor that uses a stateful code execution sandbox. It operates in a reactive, turn-based loop, maintaining a chat history with an LLM and executing Python code blocks to accomplish tasks.

-   **Plan Representation**: Implicit in conversation history.
-   **State Management**: Managed by the code execution sandbox which preserves variables between calls.
-   **Correction**: Reactive (requires user/LLM interjection).
-   **Execution Model**: `LLM -> Execute Python Code -> LLM -> Execute Python Code ...`

#### Symbolic runs and verification

When `act()` is given an `entrypoint` (a stored function id, as `TaskScheduler.execute` does for a bound task), the CodeAct loop is bypassed and the function runs directly in the sandbox. Until every function in the entrypoint's closure is trusted, the run is supervised (`unify/actor/verification_runtime.py`): untrusted callables are wrapped with per-call verifier passes, effectful leaves wait for every earlier verdict, a failed verdict rewinds and repairs the blamed leaf and re-invokes the root with a memo so no effect repeats, and an unsettled verdict holds the run with an owner notification. Trust is earned from the ledger by policy and never granted by the librarian or the repair loop; a fully trusted closure runs bare. Every LLM client the actor creates is tagged with a `purpose` — `planning`, `verification` or `repair` — and the run's token split is recorded on the execution row.

## How to Run an Actor

First, ensure you have the necessary setup:

1.  **Environment Variables**: Create a `.env` file in the project root with your `OPENAI_API_KEY`. Optionally set `UNIFY_MODEL` to override the default LLM model.
2.  **Dependencies**: Install all required packages (`pip install -r requirements.txt`).
4.  **Playwright**: Install automation binaries with `playwright install`.
