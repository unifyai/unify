# Rebuild marker

Bumping the brain SHA to roll a new assistant image out to running pods.

- 2026-08-11: force rebuild so pods pick up gateway-capable `unillm`
  (LLM gateway routing, unillm#112) — a dependency-only change does not change
  the brain SHA on its own, so the SHA-keyed image rollout needs this nudge.
