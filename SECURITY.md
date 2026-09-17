# Security Policy

This document describes Unify's trust model, names the load-bearing
boundaries, and defines what's in and out of scope for vulnerability
reports.

---

## 1. Reporting a Vulnerability

Report privately via **[security@unify.ai](mailto:security@unify.ai)** or
through [GitHub Security
Advisories](https://github.com/unifyai/unify/security/advisories/new).
**Do not open public issues for security vulnerabilities.** Unify does not
operate a bug bounty program.

A useful report includes:

- A concise description and severity assessment.
- The affected component, identified by file path and line range
  (e.g. `unify/function_manager/venv_runner.py:120-145`).
- Environment details (Unify commit SHA, OS, Python version).
- A reproduction against the `staging` branch.
- A statement of which trust boundary in §2 is crossed.

We acknowledge reports within 48 hours and aim to ship a fix within 7 days
for critical issues. Please read §2 and §3 before submitting — findings
that don't cross a documented boundary are still welcome via regular
issues or pull requests, but not through the private channel.

---

## 2. Trust Model

Unify is a single-user personal-assistant runtime that runs entirely on the
operator's machine. It has no server component, no accounts and no network
listener; the only outbound connection is to the LLM provider.

### 2.1 Definitions

- **Operator.** The person who runs the `unify` command. The operator's
  user account is the trust envelope.
- **Assistant.** The LLM-driven runtime the operator is talking to,
  composed of the `ConversationManager`, the `Actor`, and the two skill
  libraries.
- **Inbound surface.** Anything that brings attacker-influenced content
  into the assistant's context — fetched web pages, search results, files
  the operator attaches, and the text of the chat itself.
- **Action surface.** Anything the assistant does that touches the world —
  Python plans executed by the `Actor`, filesystem reads and writes under
  the workspace and beyond, network calls.
- **Trust envelope.** The set of resources the operator's user account can
  reach. Unify assumes this is what it is allowed to reach.

### 2.2 The load-bearing fact: the Actor writes and executes Python

The `Actor` generates a Python program per turn and executes it. Execution
runs in a dedicated subprocess (`unify.function_manager.execution_env`)
with an isolated venv, but **the subprocess shares the operator's user
account, the operator's filesystem, and the operator's network**. The
execution boundary is process-level, not OS-level.

What this confines: accidental misuse of Python's standard library against
the wrong path. What this does **not** confine: anything the operator's
own shell could do.

If an attacker can steer the model's context — via prompt injection in a
fetched web page or an attached file — the system has **no in-process
boundary** that stops the resulting Python from running. Operator review
of inbound surfaces and stored functions is the boundary.

### 2.3 Credential surfaces

- **`.env`** in the checkout — the LLM provider key. Owned by the
  operator's user account; readable by anything the operator runs.
- **Actor subprocess environment** — the Python plan inherits the
  operator's `os.environ` by default. Provider keys are *not* stripped
  from the subprocess environment.
- **`<UNIFY_HOME>/store.sqlite`** — everything the assistant remembers
  (the chat history, stored functions, guidance). It is an
  ordinary file under the operator's home directory, protected only by
  file permissions.

### 2.4 In-process heuristics (useful, not boundaries)

The following components shape what the LLM does. They are not boundaries:

- Tool docstrings, prompt builders, and primitive-level argument
  validation steer the LLM toward safer choices.
- `FunctionManager` classifies every stored function's side-effect class
  and runs tier-0 contract checks around calls, but the *contents* of a
  stored function still execute as arbitrary Python under the operator's
  user.

None of these survive an LLM that wants to do something they don't allow.
Operator review of stored functions, guidance, and inbound surfaces is the
real boundary.

### 2.5 Inbound surfaces

Every byte that reaches the model is attacker-influenceable the moment it
came from outside the operator's head:

- **Attached files** — a path the operator names is read by whatever the
  actor's plan does with it, on the operator's host.

The supported posture for adversarial inbound surfaces is to run Unify
inside a whole-process sandbox (container or VM). That is on the operator;
Unify does not ship one.

---

## 3. Scope

### 3.1 In Scope

- **Trust-boundary bypasses** that let content from an inbound surface
  cause Unify to run code, exfiltrate credentials, or persist data without
  the operator's involvement.
- **Parsing-surface bugs** — path traversal, command injection,
  deserialisation in the store's expression language
  (`unify/db/expressions.py`).
- **Hard-coded credentials or secrets** in the repository.
- **Supply-chain issues** affecting `uv.lock` — lockfile tampering,
  typo-squat.

### 3.2 Out of Scope

- **Prompt injection alone**, without a demonstrated boundary bypass.
  Prompts are influenceable by definition; mitigations are heuristics
  (§2.4).
- **Anything in the sibling repo** (`unillm`) — report against that repo
  directly.
- **Operator-chosen exposures** — a world-readable `.env` or store file, or
  a stored function installed without reading it.
- **Provider-side findings** — bugs in LLM provider APIs should be
  reported to the provider.
- **Pre-existing files in the operator's home directory** that Unify does
  not create or write.

---

## 4. Hardening

Recommendations for operators feeding Unify untrusted inbound surfaces:

- **Use scoped provider keys** where the provider supports them (per-
  project keys, IP allowlists, spend caps).
- **Run Unify in a container or VM** if you intend to expose it to
  adversarial content. The default install is the supported posture only
  when the operator trusts every input.
- **Tighten permissions** on `.env` and `UNIFY_HOME` (`chmod 600` /
  `chmod 700`) and consider full-disk encryption on the host.
- **Read any stored function** before letting the assistant rely on it.
  Stored functions execute arbitrary Python under the operator's user.
- **Run with `unify --debug`** during the first few sessions to see what
  the `Actor` is actually doing.

---

## 5. Disclosure

- We coordinate disclosure with the reporter. Patched releases ship to
  `staging`, then `main`.
- We credit reporters on the release commit unless asked otherwise.
- For sufficiently high-severity issues we will request a CVE.
