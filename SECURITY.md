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
  (e.g. `unify/secret_manager/__init__.py:120-145`).
- Environment details (Unify commit SHA, OS, Python version, whether
  hosted or local install).
- A reproduction against the `staging` branch.
- A statement of which trust boundary in §2 is crossed.

We acknowledge reports within 48 hours and aim to ship a fix within 7 days
for critical issues. Please read §2 and §3 before submitting — findings
that don't cross a documented boundary are still welcome via regular
issues or pull requests, but not through the private channel.

---

## 2. Trust Model

Unify is a single-tenant personal-assistant runtime. The trust boundaries
differ between the open-source local install and the hosted product at
[console.unify.ai](https://console.unify.ai). **This policy describes the
local install.** The hosted product is operated separately and its
security is the responsibility of the operating company; reports against
it go through the same channels but reference the hosted endpoint.

### 2.1 Definitions

- **Operator.** The person who installed Unify and runs the `unify`
  command. The operator's user account is the trust envelope.
- **Assistant.** The LLM-driven runtime that the operator is talking to,
  composed of the `ConversationManager`, `Actor`, and the typed back
  office of state managers.
- **Inbound surface.** Anything that brings attacker-influenced content
  into the assistant's context — emails, SMS, phone-call transcripts,
  fetched web pages, search results, file uploads from external contacts.
- **Action surface.** Anything the assistant does that touches the world
  — Python plans executed by the `Actor`, outbound comms via the gateway,
  filesystem reads and writes, network calls.
- **Trust envelope.** The set of resources the operator's user account
  can reach. The local install assumes this is what Unify is allowed to
  reach.

### 2.2 The load-bearing fact: the Actor writes and executes Python

The `Actor` generates a Python program per turn and executes it. Execution
runs in a dedicated subprocess (`unify.function_manager.execution_env`)
with an isolated venv, but **the subprocess shares the operator's user
account, the operator's filesystem, and the operator's network**. The
execution boundary is process-level, not OS-level.

What this confines: accidental misuse of Python's standard library against
the wrong path. What this does **not** confine: anything the operator's
own shell could do.

If you run Unify against an LLM whose context can be steered by an
attacker — via prompt injection in an inbound email, a fetched web page,
a calendar invite, a phone-call transcript, etc. — the system has **no
in-process boundary** that stops the resulting Python from running.
Operator review of inbound surfaces and installed functions is the
boundary.

### 2.3 Credential surfaces

- **`~/.unity/unity/.env`** — LLM provider keys, `ORCHESTRA_URL`,
  `UNIFY_KEY`, and any optional integration keys (Twilio, Cartesia,
  ElevenLabs, Tavily, etc.). Owned by the operator's user account;
  readable by anything the operator runs.
- **`SecretManager`** — exposes a deliberately-narrow public API.
  `primitives.secrets.ask(...)` returns metadata only (names, types,
  placeholders), never the secret value; `primitives.secrets.update(...)`
  is the only mutation. The encryption key is operator-supplied and not
  bound by Unify to any specific KMS. This is the **highest-blast-radius
  surface in the codebase** — see [`.github/CODEOWNERS`](.github/CODEOWNERS).
- **Actor subprocess environment** — the Python plan inherits the
  operator's `os.environ` by default. Provider keys are *not* stripped
  from the subprocess environment in the supported local-install posture.

### 2.4 In-process heuristics (useful, not boundaries)

The following components shape what the LLM does. They are not boundaries:

- Tool docstrings, prompt builders, and primitive-level argument
  validation steer the LLM toward safer choices.
- The `SecretManager.ask` placeholder-only contract limits what bad
  prompts can trivially achieve through that one tool.
- `FunctionManager` review gates exist for user-installed functions, but
  the *contents* of an installed function still execute as arbitrary
  Python under the operator's user.

None of these survive an LLM that wants to do something they don't allow.
Operator review of installed functions, guidance, and inbound surfaces is
the real boundary.

### 2.5 Inbound surfaces

When Unify is configured to receive messages from external channels (SMS,
email, phone, voice, web search results, fetched files), every byte that
reaches the model is attacker-influenceable. Treat every channel as
untrusted.

Particularly load-bearing:

- **Email and SMS** — easiest to inject from outside.
- **Fetched web pages and search results** — `WebSearcher` does not
  sanitise.
- **Files uploaded by external contacts** — `FileManager.parse` runs
  document parsers (PDF, Office, etc.) on the operator's host.
- **Voice / phone transcripts** — STT output is opaque text that flows
  into the model the same way chat does.

The supported posture for adversarial inbound surfaces is to run Unify
inside a whole-process sandbox (container, VM, or per-session sandbox).
That is on the operator; Unify does not ship one.

### 2.6 Hosted Orchestra backend

The local install persists to the hosted Orchestra backend over HTTPS
(`ORCHESTRA_URL`), authenticated with the `UNIFY_KEY` written to
`~/.unity/unity/.env`. Everything the assistant remembers — contacts,
knowledge, transcripts, tasks — is stored in that per-tenant backend, so
the `UNIFY_KEY` is the credential that guards it: anyone holding the key
can read and write the assistant's memory through the API.

Protect `~/.unity/unity/.env` like any credentials file. Key rotation is
done from [console.unify.ai](https://console.unify.ai).

---

## 3. Scope

### 3.1 In Scope

- **Trust-boundary bypasses** that let an unauthenticated network actor
  cause Unify to run code, exfiltrate credentials, or persist data without
  operator approval.
- **`SecretManager` bugs** that expose secret material outside the
  documented placeholder/metadata API.
- **Parsing-surface bugs** — path traversal, command injection,
  deserialisation in `FileManager`, gateway channels, or comms ingress.
- **AuthN/AuthZ bugs** in any code under `unify/gateway/`.
- **Hard-coded credentials or secrets** in the repository.
- **Supply-chain issues** affecting `uv.lock` or
  `agent-service/package-lock.json` — lockfile tampering, typo-squat.

### 3.2 Out of Scope

- **Prompt injection alone**, without a demonstrated boundary bypass.
  Prompts are influenceable by definition; mitigations are heuristics
  (§2.4).
- **Anything in [console.unify.ai](https://console.unify.ai)** or the
  hosted Unify product — report against the hosted endpoint with the same
  channels.
- **Anything in the sibling repos** (`unisdk`, `unillm`, `orchestra`)
  — report against those repos directly.
- **Operator-chosen exposures** — running Unify with the Orchestra port
  bound to non-loopback, or with `.env` written world-readable, or
  installing a third-party function without reading it.
- **Provider-side findings** — bugs in LLM provider APIs, Twilio,
  Deepgram, etc. should be reported to the provider.
- **Pre-existing files in the operator's home directory** that Unify does
  not create or write.

---

## 4. Deployment Hardening

Recommendations for operators running Unify against untrusted inbound
surfaces:

- **Use scoped provider keys** where the provider supports them (per-
  project keys, IP allowlists, spend caps).
- **Run Unify in a container or VM** if you intend to expose it to
  adversarial inbound surfaces. The default local install is the
  supported posture only when the operator trusts every input.
- **Tighten `.env` permissions** (`chmod 600`) and consider full-disk
  encryption on the host.
- **Read any `FunctionManager`-stored function** before installing it.
  Installed functions execute arbitrary Python under the operator's user.
- **Watch the `unify logs` stream** during the first few sessions to see
  what the `Actor` is actually doing.

---

## 5. Disclosure

- We coordinate disclosure with the reporter. Patched releases ship to
  `staging`, then `main`, then are noted in
  [`CHANGELOG.md`](CHANGELOG.md).
- We credit reporters in the changelog and on the release commit unless
  asked otherwise.
- For sufficiently high-severity issues we will request a CVE.
