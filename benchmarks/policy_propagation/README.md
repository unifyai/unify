# Policy propagation

**Question:** three recurring automations share one business policy, stated
verbatim in each of the three verbal requests that created them. One verbal
policy-update message later — what does the change cost, and does every
automation actually behave per the new policy afterwards?

## Task family

One seeded inquiry stream, three automations with separate sinks/cursors:
hourly **triage** (category + priority per policy), daily **digest**
(urgent counts by category), weekly **audit** (urgent totals/fraction).
The escalation policy: urgent when a charge/billing amount ≥ $500 is
involved, or the customer is blocked from working. The change: threshold
drops to $250. Golden labels are mechanical (amounts and blocked-phrasing
are generator-controlled) and validated to 100% agreement with the
benchmark model at both thresholds across repeated samples; every
post-change scoring window contains at least one item whose priority flips.

## Measured results (2026-07-31, gpt-5.6-sol@openrouter)

![policy propagation](results/policy_propagation.svg)

| | Unify | hermes |
|---|---|---|
| correctness (15 fires, both epochs) | **15/15** | **15/15** |
| change propagated to all three automations | yes (3 function edits) | yes (3 prompt edits) |
| change-application cost | 2.63M tokens / $10.13 | **1.14M tokens** |
| steady state, whole family per round | **~2.5k tokens** | ~57k tokens |
| payback of the change-cost gap | from ~round 28 after the change | — |

An honestly mixed result. Both architectures found and updated every copy
of the policy — at three automations, propagation correctness tied.
hermes's change session was ~2.3× cheaper. Unify's steady state is ~23×
cheaper for the family (one focused `query_llm` call per automation per
round vs an agent boot each), which repays the change-cost gap within ~28
rounds and diverges linearly after.

**The product finding:** this run did not produce the design's intended
many-to-many shape. The storage reviews embedded the policy *inside each of
the three functions* (three copies) rather than factoring it into one
guidance entry linked to the three functions — so the change session had to
discover and rewrite three functions, which is exactly why it cost more
than editing three prompts. (The calibration round showed the factoring is
*reachable* — that run spontaneously created a shared
`_run_customer_inquiry_policy_job` helper — but it is not reliably chosen.)
Making the review recognize repeated policy text across tasks and factor it
into linked guidance is the next prompt-engineering target; the linkage
would then make change application a guidance edit plus mechanical
regeneration of the linked functions.

## Notes

- The calibration round (earlier result directories, kept with NOTE.md)
  had two ambiguous golden templates; both arms produced byte-identical
  score dents on them — the systems agreed with each other and disagreed
  with the labels, which is direct evidence the harness treats both arms
  equally. The fixture was recalibrated before the definitive runs.
- Running the unify arm surfaced two staging-infrastructure fixes along
  the way: internal-domain accounts are now exempt from the trial
  anti-abuse gates (orchestra `40d69258`), and the shared tenant's credit
  balance was topped up via the existing `create_recharge` admin promo.

```bash
bash benchmarks/policy_propagation/run_unify.sh
PP_PORT=8133 PP_PROXY_PORT=8134 bash benchmarks/policy_propagation/run_hermes.sh
.venv/bin/python -m benchmarks.policy_propagation.plot
```
