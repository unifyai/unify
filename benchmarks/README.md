# Benchmarks

This suite now lives in its own repository:
**[unifyai/colleague](https://github.com/unifyai/colleague)**.

It moved out because it stopped being about unify. It hosts drivers for
harnesses that have nothing to do with this runtime — hermes-agent, OpenClaw,
OpenCode — and shipping those from inside one of the arms it measures was the
wrong shape for something meant to be re-run by other people.

The four experiments that used to live here are its `standing` track:

| Was | Is |
|---|---|
| `benchmarks/recurring_weekly_report/` | `colleague/tracks/standing/recurring_report/` |
| `benchmarks/drift_recovery/` | `colleague/tracks/standing/drift_recovery/` |
| `benchmarks/semantic_triage/` | `colleague/tracks/standing/semantic_triage/` |
| `benchmarks/policy_propagation/` | `colleague/tracks/standing/policy_propagation/` |

Committed results, run NOTEs and raw ledgers moved with them. The shared arm
toolkits, which used to live inside `recurring_weekly_report/` and be imported
from there, are now peers in `colleague/arms/`.

The measured findings are written up in
[Recurring automation, measured](https://unify.ai/blog/recurring-automation-measured).
