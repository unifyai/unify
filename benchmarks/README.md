# Benchmarks

Standalone, open-sourceable benchmarks that back the claims in
[Why we split skills into functions and guidance](https://unify.ai/blog/why-we-split-skills-into-functions-and-guidance)
with real numbers instead of hand-waving.

The core claim under test: for recurring automations set up from a single
natural-language request, an architecture with a first-class scheduler→function
binding (Unify: `TaskScheduler` + `FunctionManager` entrypoints) converges to a
**zero-LLM-token steady state**, while prose-first skill architectures
(e.g. hermes-agent) pay **at least one full agent invocation per firing,
forever** — because their only trigger path runs through the model.

## Design principles

- **Real inference, real numbers.** `UNILLM_CACHE=false`. Every LLM call is
  recorded (model, prompt/completion tokens, provider cost) via unillm's
  global LLM event hook. Raw ledgers ship with results.
- **Architectural floors, not model smarts.** Each system receives the
  *identical* natural-language utterance and self-organizes unattended. We
  measure what the design converges to, not what a hand-tuned config can do.
  (A hand-tuned "ceiling" arm is planned as a secondary protocol.)
- **Exact ground truth.** Fixtures are seeded and deterministic; the harness
  independently recomputes the correct output for every run. No LLM judges.
- **Reproducible.** Local fixture servers, no third-party accounts, no live
  web state. Anyone can rerun with their own keys.
- **Honest accounting.** Setup/first-run costs for Unify include *everything*:
  the CodeAct plan, the storage/librarian pass, the entrypoint review, and any
  repair loops. Failures and non-attachments are reported, not hidden.

## Experiments

| Experiment | Status | Question |
|---|---|---|
| [`recurring_weekly_report`](recurring_weekly_report/) | active | Weekly metrics digest from NL: setup cost, per-run cost over N simulated weeks, correctness, entrypoint-attach behavior |

Planned follow-ons (see the experiment README for rationale): a
high-frequency change-detection monitor (hermes's own documented blueprint),
a semantic-substep tier (inbox triage with golden labels), and the
many-to-many family/maintenance phase (marginal cost of related task N+1 and
one cross-cutting guidance change).

## Running

Each experiment is standalone: its own README, fixture, harness, and launcher.
Nothing here imports the test suite. Experiments run against **staging
Orchestra** by default (`https://api.staging.internal.saas.unify.ai/v0`) in an
isolated context tree (`benchmarks/<experiment>/<run-id>/...`), so they never
touch a real assistant's data.

```bash
bash benchmarks/recurring_weekly_report/run.sh
```
