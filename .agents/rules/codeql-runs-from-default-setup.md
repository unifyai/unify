---
description: CodeQL on unify runs from GitHub's default setup, not a workflow file — do not add or re-enable a codeql.yml
---

# CodeQL Comes From Default Setup, Not A Workflow

`unify` has **no `.github/workflows/codeql.yml`**, and must not gain one. Static
analysis runs from GitHub's **default setup** — the `dynamic/github-code-scanning/codeql`
workflow, which has no file in this repo. It analyses `actions`, `javascript`,
`javascript-typescript`, `python` and `typescript` on pull requests and weekly,
with the `security-extended` query suite.

## Why there is no file

There used to be one. On **2026-06-23 12:29Z** nassimberrada enabled default setup
and disabled the advanced workflow one second later, via `gh`:

```
repo.codeql_enabled         2026-06-23 12:29:52Z
workflows.disable_workflow  2026-06-23 12:29:53Z   workflow_id=296489893
```

That order is required, not incidental: GitHub does not run both configurations at
once, so enabling default setup means the advanced workflow has to go. The file was
left behind, disabled, for six weeks and read as "someone switched static analysis
off" — it had not been; default setup was analysing the whole time. It is deleted so
the next reader is not misled the same way.

## What this means in practice

- **Do not** re-enable or re-add a `codeql.yml` here. Two configurations conflict,
  and the workflow-shaped one is the one that loses.
- Change the analysis by changing default setup, not a file:
  `gh api -X PATCH repos/unifyai/unify/code-scanning/default-setup -f query_suite=extended`.
  It returns a `run_id`; the new suite applies once that run completes.
- The `CodeQL` check on a pull request is **advisory here** — the `Staging->Main`
  ruleset requires `black`, `staging-source`, `Flow smoke` and `Contract tests`, not
  CodeQL. A red CodeQL does not block a release, so do not read `BLOCKED` on a PR as
  a CodeQL problem without checking the ruleset.
- A workflow-only diff can make the check report `1 configuration not found` rather
  than a finding, because no results are produced for the languages it expects.

## Sibling repos differ — check before assuming

`orchestra` keeps a real committed `codeql.yml` (advanced setup, `security-extended`,
python) and no default setup. `unisdk`, `unillm` and `docs` are on default setup with
no file. Reading alerts for the private repos needs a token with `security_events`;
without it the API answers `403 Code Security must be enabled`, which is a scope
message and **not** evidence that scanning is off.
