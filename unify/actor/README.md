# Actor Module

This directory contains different "actor" implementations, which are responsible for taking a high-level user goal and breaking it down into a series of actions to be executed.

## Available Actors

### CodeActActor

A conversational actor that uses a stateful code execution sandbox. It operates in a reactive, turn-based loop, maintaining a chat history with an LLM and executing Python code blocks to accomplish tasks.

-   **Plan Representation**: Implicit in conversation history.
-   **State Management**: Managed by the code execution sandbox which preserves variables between calls.
-   **Correction**: Reactive (requires user/LLM interjection).
-   **Execution Model**: `LLM -> Execute Python Code -> LLM -> Execute Python Code ...`

## How to Run an Actor

First, ensure you have the necessary setup:

1.  **Environment**: copy `.env.example` to `.env` in the project root and add one LLM provider key.
2.  **Dependencies**: `uv sync --all-groups` creates `.venv/` with everything the actor needs.

Then chat with the assistant, which dispatches its work to the actor:

```bash
.venv/bin/python -m unify
```
