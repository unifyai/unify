# Logs

Everything under `logs/` is gitignored except this file.

| Directory | Written by | Contents |
|---|---|---|
| `logs/pytest/<run>/` | `tests/parallel_run.sh` | One log per tmux session, plus `stores/` holding each session's SQLite file |
| `logs/unify/` | the runtime, when `UNIFY_LOG_DIR` points here | `unify.log` (everything) and `unify_info_only.log` |
| `logs/unillm/` | `unillm` | Raw LLM request/response traces |

The chat CLI writes its runtime logs under `<UNIFY_HOME>/logs` instead.

`rg`, `Grep` and `Glob` honour `.gitignore`, so search here with `rg -uu`
and read files with the `Read` tool or a plain `cat`.
