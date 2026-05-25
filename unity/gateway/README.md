# unity.gateway

External communication gateway for Unity. This package is the transport
layer that connects the assistant runtime to the outside world: phone
networks, email providers, chat platforms, and inbound webhook
surfaces. It owns the abstractions (broker, storage, secrets, envelope
schemas) through which Unity speaks to those transports.

## Why this exists

Today the channel routers that bridge Unity to Twilio, Microsoft Graph,
Discord, etc. live in a private `communication` repository deployed as
two Cloud Run services (`unity-comms-app`, `unity-adapters`). That
arrangement has two costs:

1. **Open-source discoverability.** Unity is the public-facing vessel
   for the AI assistant. A reader of the open codebase cannot run an
   end-to-end deployment from what is in `unity/` alone, because the
   channel transports live elsewhere and behind a private boundary.
2. **Duplication between local and deployed paths.** Unity ships a
   `LocalCommsIngress` in-process aiohttp server with its own
   `local_providers/twilio.py`, `local_providers/email.py`, etc., that
   re-implements parts of the comms-side routes. The two paths drift.

The fix is to concentrate the channel transports into Unity behind
small, well-defined abstractions, and have the production deployment
consume Unity as a library rather than maintaining a parallel codebase.

## Naming

The package is called `gateway` to match the two nearest open-source
reference designs:

- [`openclaw/openclaw`](https://github.com/openclaw/openclaw) — MIT,
  organises its external surface under `gateway/`.
- [`NousResearch/hermes-agent`](https://github.com/NousResearch/hermes-agent)
  — MIT, same convention.

The name also avoids collision with `unity.comms`, which already
exists and holds the *behavioural* outbound layer (`CommsPrimitives`).
The separation matters:

- `unity.comms` = **what the assistant does** when it decides to reach
  out. Contact resolution, capability gating, transcript publication.
- `unity.gateway` = **how the assistant gets to the outside world**.
  Webhook envelope schemas, pluggable broker, pluggable storage,
  pluggable secrets.

## What is in this package today

Phase A (this landing) ships only the seam — the abstractions and their
default in-process implementations. The channel routers themselves
arrive in Phase B.

```
unity/gateway/
├── __init__.py            # clean public exports
├── README.md              # you are here
├── PHASES.md              # full multi-phase rollout plan
├── event_broker.py        # EventBroker + PubSubConnection Protocols
├── envelopes.py           # canonical inbound webhook envelope schemas
├── storage/
│   ├── base.py            # Storage Protocol
│   ├── local.py           # LocalDiskStorage (default; self-hosted)
│   └── gcs.py             # GcsStorage stub (Phase B)
└── secrets/
    ├── base.py            # SecretManager Protocol
    ├── env.py             # EnvSecretManager (default; self-hosted)
    └── gcp.py             # GcpSecretManager stub (Phase B)
```

`tests/gateway/` mirrors the structure with focused unit tests for
each module.

### EventBroker (`event_broker.py`)

The `EventBroker` `Protocol` is a strict subset of `redis.asyncio.Redis`
pub/sub. The existing
`unity.conversation_manager.in_memory_event_broker.InMemoryEventBroker`
already satisfies it without modification; the test
`test_in_memory_event_broker_satisfies_event_broker_protocol` pins that
invariant.

Concrete implementations now and planned:

| Implementation | Status | Used by |
| --- | --- | --- |
| `InMemoryEventBroker` | Shipped | Tests, single-process self-hosted Unity, offline runs |
| `PubSubEventBroker` | **Phase A.bis** | Hosted Cloud Run deployment (currently inline in `comms_manager.py`) |
| `RedisStreamsEventBroker` | Possible future | Multi-process self-hosted on a single VPS |

### Envelopes (`envelopes.py`)

Pydantic models for the `{thread, publish_timestamp, event}` wire
format that every inbound channel publishes. Phase A models a
representative subset (`msg`, `email`, `unify_message`,
`unity_system_event`) plus a `GenericEnvelope` fallback that keeps
unmigrated channels flowing. `KNOWN_THREADS` enumerates the full
17-thread catalogue recovered from the current code paths.

### Storage (`storage/`)

`LocalDiskStorage` is the self-hosted default — backs attachments under
`UNITY_GATEWAY_STORAGE_DIR` (defaults to `./.unity-gateway-storage`).
`GcsStorage` is a stub that raises `NotImplementedError` pending Phase B,
when the first hosted call site needs it wired in.

### Secrets (`secrets/`)

`EnvSecretManager` is the self-hosted default — reads from process
environment variables, with optional prefix filtering. `GcpSecretManager`
is a stub pending Phase B.

## What is **not** in this package

Anything that requires touching files outside `unity/gateway/` and
`tests/gateway/`. In particular:

- The Pub/Sub-specific code currently inline in
  `unity/conversation_manager/comms_manager.py` is **not yet
  extracted**. That extraction is Phase A.bis and deserves a dedicated
  PR with focused test coverage, because `CommsManager`'s threading
  model (Pub/Sub callbacks marshalled into the asyncio loop via
  `run_coroutine_threadsafe`) is the single highest-risk piece of the
  migration.
- No channel routers have moved. The full `communication/{phone,
  gmail, outlook, whatsapp, discord, teams, email, social, sharepoint,
  unillm}/` tree still lives in the private repository and still
  serves production traffic exactly as before.
- No Dockerfile, Cloud Build config, or Cloud Run service has been
  touched. Production deployment is unchanged.

## What comes next

See `PHASES.md` for the full rollout plan from here to a single-vessel
Unity that consumes the same code in production and in self-hosted
mode.

## Relationship to `unity.conversation_manager`

`unity.conversation_manager` keeps its current public surface. In
particular:

- `unity.conversation_manager.event_broker.get_event_broker()` still
  returns an `InMemoryEventBroker` — unchanged behaviour. Once the
  Phase A.bis `PubSubEventBroker` lands, that factory grows a switch
  on `UNITY_EVENT_BROKER` (default `inmemory`, production sets
  `pubsub`).
- `unity.conversation_manager.local_ingress.LocalCommsIngress` still
  serves the in-process aiohttp routes. It will be retired once the
  full gateway HTTP surface (Phase B) lands and is reachable in
  single-process mode through a unified entrypoint.

## Relationship to `communication/`

The private `communication/` repo's `phone/`, `gmail/`, `outlook/`,
`whatsapp/`, `discord/`, `teams/`, `email/`, `social/`, `sharepoint/`,
and `unillm/` packages migrate to `unity/gateway/channels/<name>/`
during Phase B. The `adapters/` Cloud Functions / webhook receivers
migrate to `unity/gateway/adapters/`. The shared `common/` library
migrates to `unity/gateway/common/`.

The pieces that stay in the private repo indefinitely:

- `communication/infra/` — GCE pool VMs, Cloud DNS, ACME wildcard cert
  renewal, tunnel server. The hosted-SaaS desktop infrastructure.
- `communication/assistant_session_controller/` — Kubernetes
  per-binding session orchestration.
- `communication/cloudbuild/`, `communication/k8s/`,
  `communication/sbc-proxy/`, `communication/scripts/` — the
  deployment + runbook surface that targets Unity's hosted topology.
- `communication/Dockerfile-*` — these get rewritten in Phase C to
  `pip install unity` and shell out to `unity gateway ...` rather
  than `uvicorn communication.main:app`.

Self-hosted users never touch the private repo. They install Unity,
provide their own Twilio / Microsoft Graph / Discord credentials, and
run `unity gateway serve` to get the full external-comms surface
locally.
