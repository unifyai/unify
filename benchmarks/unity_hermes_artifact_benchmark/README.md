# Unity vs Hermes Artifact Benchmark

This package contains two related benchmarks for the recurring-task question:

> Classify every email received since yesterday, and write draft replies where needed.

The primary metric is artifact quality, not first-run token count. The benchmark
checks whether a system converges to a reusable unit that future runs can invoke
directly, without rereading or reinterpreting procedural prompt material.

## Deterministic Reference Benchmark

The reference benchmark is intentionally explicit: it uses benchmark arm specs,
known traces, expected outputs, and scoring baselines so changes to the rubric are
fast and stable.

Run the deterministic reference benchmark:

```bash
python -m benchmarks.unity_hermes_artifact_benchmark.runner --out /tmp/unity-hermes-benchmark
```

The output directory contains:

- `benchmark.json`: full corpus, arm specs, reference traces, scores, and analysis.
- `corpus.json`: synthetic inbox batches and expected classifications/drafts.
- `arms.json`: Unity and Hermes prompt/checklist specifications.
- `report.md`: compact comparison report.

Trace measurements include token counts, estimated costs, tool calls before
useful execution, and the number of cheap LLM calls inside the reusable artifact.

## Live Production-Like Benchmark

The live harness tests production-like agent behavior. The agent-facing setup
prompt is just:

```text
Could you please check my emails every morning at 9am, and then draft basic replies to each of them?
```

Synthetic mailbox selection, expected outputs, side-effect safety, scheduler
capture, and scoring live outside the prompt. Unity receives a normal-looking
`get_emails(...)` FunctionManager helper backed by hidden activation context and
a production-shaped `primitives.tasks.update(...)` scheduler sink that records
tasks in memory. Hermes runs in an isolated workspace/home with the same local
mailbox adapter and no prompt instructions about fixture paths or output files.

Run the live harness:

```bash
python -m benchmarks.unity_hermes_artifact_benchmark.daily_email_live --out /tmp/unity-hermes-live
```

The live output writes `live_benchmark.json` with:

- `agent_visible_prompt` for every turn.
- `harness_context` kept separate from prompts.
- hidden mailbox activation context and expected-output hiding status.
- scheduler records from the mock Unity task sink.
- externally inspected artifacts and artifact-quality scores.
- token and cost summaries from Unity `unillm` cost events and Hermes session counters.
