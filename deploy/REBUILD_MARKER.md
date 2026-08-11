# Rebuild marker

Bumping the brain SHA to roll a new assistant image out to running pods.

- 2026-08-11: force rebuild so pods pick up gateway-capable `unillm`
  (LLM gateway routing, unillm#112) — a dependency-only change does not change
  the brain SHA on its own, so the SHA-keyed image rollout needs this nudge.

- 2026-08-11: bump brain SHA to ship reverted unillm (OpenRouter direct again;
  Orchestra LLM gateway retired) to pods.

- 2026-08-11: ship gateway-capable `unillm` again, now brokering Anthropic as
  well as OpenRouter (unillm `68317af`). The build resolves each dependency's
  staging SHA at build time, so the image picks the new code up on its own —
  what it cannot do is start without a commit here, since nothing else in
  `unify` changed. Pods keep their provider keys until this image is confirmed
  live; removing a key from a pod still running the previous image leaves it
  with neither a route nor a credential.
