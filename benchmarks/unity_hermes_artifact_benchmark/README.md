# Unity vs Hermes Artifact Benchmark

This is a controlled, dry-run benchmark for the recurring-task question:

> Classify every email received since yesterday, and write draft replies where needed.

The primary metric is artifact quality, not first-run token count. The benchmark
checks whether a system converges to a reusable unit that future runs can invoke
directly, without rereading or reinterpreting procedural prompt material.

Run the deterministic reference benchmark:

```bash
python -m benchmarks.unity_hermes_artifact_benchmark.runner --out /tmp/unity-hermes-benchmark
```

The output directory contains:

- `benchmark.json`: full corpus, arm specs, reference traces, scores, and analysis.
- `corpus.json`: synthetic inbox batches and expected classifications/drafts.
- `arms.json`: Unity and Hermes prompt/checklist specifications.
- `report.md`: compact comparison report.

The reference traces are baselines for the design comparison. To run a live
experiment, keep the same corpus/output contract and replace the reference
traces with real Unity/Hermes traces before scoring. Trace measurements include
token counts, estimated costs, tool calls before useful execution, and the number
of cheap LLM calls inside the reusable artifact.
