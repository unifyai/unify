<!--
Thanks for contributing to Droid! Please fill out the sections below.
For trivial changes (typo fixes, comment-only edits) you can shorten this template — just keep the Summary.

PRs land on `staging`, not `main`. See CONTRIBUTING.md.
-->

## Summary

<!-- 1–3 sentences: what problem does this PR solve, and why is this the right approach? -->



## Type of change

<!-- Check all that apply. -->

- [ ] Bug fix (non-breaking change that fixes incorrect behavior)
- [ ] Feature (non-breaking change that adds functionality)
- [ ] Refactor (no behavior change)
- [ ] Breaking change (API or data-model change — Droid has zero-backward-compat policy, but please call it out)
- [ ] Test-only (no source changes)
- [ ] Docs / chore / CI

## Areas touched

<!-- Check all that apply. Helps reviewers route. -->

- [ ] Actor / CodeAct
- [ ] ConversationManager / slow brain
- [ ] A specific state manager (Contact / Knowledge / Task / Transcript / Guidance / Function / File / Image / Web / Secret / Blacklist / Data / Memory)
- [ ] Async tool loop (`droid/common/_async_tool/`)
- [ ] Event bus / observability
- [ ] Gateway / external comms
- [ ] Tests / test infra (`tests/`, `conftest.py`, `parallel_run.sh`)
- [ ] CI / build / packaging

## Test plan

<!--
How did you verify this? Paste the command(s) you ran and the result.

The default expectation is to run the relevant directory:
  tests/parallel_run.sh tests/<module>/

For infrastructure changes that are hard to reach via tests, a quick
verification script under tests/_verify_*.py is encouraged
(see .cursor/rules/surgical-verification-before-tests.mdc).
-->

```
tests/parallel_run.sh tests/...
```

- [ ] All relevant tests pass locally
- [ ] If this is a bug fix, I added a regression test (or explained why one isn't feasible)

## Behavior / migration notes

<!--
- Did this change a public manager API, primitive, or event payload?
- Are there schema/context changes in Orchestra that need a migration?
- Any new env vars or config flags? (Update `.env.advanced.example`.)
- If yes to any of the above, describe upgrade steps.
-->

None.

## Checklist

- [ ] PR is targeted at `staging` (not `main`)
- [ ] Followed conventional commit style (`feat(scope):`, `fix(scope):`, `refactor(scope):`, `chore(scope):`, etc.)
- [ ] No `try/except` added defensively — only around specific, recoverable errors
- [ ] No "new" / "updated" / "TODO from chat" temporal comments (see `.cursor/rules/no-temporal-comments.mdc`)
- [ ] No test-specific shortcuts in production code (see `.cursor/rules/no-test-info-in-production-code.mdc`)
- [ ] Updated `AGENTS.md` / `ARCHITECTURE.md` if I changed architectural conventions
