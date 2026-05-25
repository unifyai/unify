# unity.gateway.channels

Channel-specific FastAPI routers that bridge external messaging
providers (Twilio, Microsoft Graph, Discord, Gmail, ...) to the
Unity runtime. Each subpackage mirrors one of the channel modules
in the private `communication` repository.

This README is the **migration pattern** Phase B follows for every
channel. Once you have ported one channel using this template, the
remaining nine follow mechanically.

## Layout per channel

```
unity/gateway/channels/<name>/
├── __init__.py      # re-exports `router`
└── views.py         # the FastAPI APIRouter and handlers

tests/gateway/channels/<name>/
├── __init__.py
└── test_views.py    # contract + behavioural tests
```

## Translation rules

When porting a channel from `communication/<name>/views.py` to
`unity/gateway/channels/<name>/views.py`, apply these substitutions
mechanically. None of them should change wire behaviour; they are
about decoupling the channel from `communication`'s internal
plumbing.

| What in `communication` | Replaced by in `unity.gateway` |
| --- | --- |
| `from common.settings import SETTINGS` | `from unity.gateway.secrets import EnvSecretManager` (for credentials) or `from unity.settings import SETTINGS` (for non-credential config) |
| `os.getenv("TWILIO_ACCOUNT_SID")` style direct env reads | `secrets.get("TWILIO_ACCOUNT_SID")` via an `EnvSecretManager` instance |
| `from communication.helpers import get_twilio_client` | Inline helper inside the channel module (promote to `unity/gateway/common/` once two or more channels need it) |
| `from google.cloud import pubsub_v1` + `publisher.publish(...)` | `from unity.conversation_manager.domains.comms_utils import get_outbound_transport` then `transport.publish(topic, msg_bytes, thread=thread)` — or use `_publish_to_assistant_topic` directly if publishing to the per-assistant topic |
| Direct envelope dict construction (`{"thread": ..., "event": {...}}`) | Pydantic `XEnvelope(...)` from `unity.gateway.envelopes` when a concrete schema exists; raw dict + `parse_envelope` otherwise. The schema is added to `envelopes.py` as part of the same PR. |
| `from google.cloud import storage` for attachments | `Storage.write_bytes(key, data)` / `Storage.read_bytes(key)` via `LocalDiskStorage` or `GcsStorage` |
| `from communication.helpers import _lookup_assistant` | Inline (single-call use) or promote to `unity/gateway/common/orchestra.py` (multi-call use). The Orchestra HTTP client is the same; only the import path changes. |

## What does *not* change

- **The on-wire envelope shape**. Every channel's published envelopes
  must keep the existing `{thread, publish_timestamp, event}` shape
  bit-for-bit so downstream consumers don't see a difference.
- **Topic and subscription names**. The
  `unity-{agent_id}{env_suffix}` convention is fixed.
- **HTTP route paths**. The route prefixes mounted in
  `communication/main.py` (e.g. `/phone`, `/social`, `/gmail`)
  remain the same; the gateway app aggregator (Phase B endgame)
  mounts the new routers at the same paths.
- **Auth contract**. `Authorization: Bearer <ORCHESTRA_ADMIN_KEY>`
  still gates admin endpoints; auth middleware moves with the
  router.

## Tests

Every channel migration ships with a test file that exercises:

1. **Router conformance**: the module exposes a `router: APIRouter`
   with the expected route paths (pinned via FastAPI's `routes`
   inspection).
2. **Endpoint behaviour**: each endpoint invoked via FastAPI's
   `TestClient`, with vendor SDKs (Twilio, Graph, Discord) mocked
   so tests don't need real credentials.
3. **Secret resolution**: tests exercise the missing-credential
   error path so misconfiguration fails loudly.
4. **Schema validation**: Pydantic request models reject malformed
   input.

The `social` channel (the Phase B proof-of-concept) is the
reference implementation -- the other channel ports should mirror
its test structure with channel-specific extensions.

## Cutover

Each channel migrated this way is **dormant** until Phase C cuts
the production deployment over to import from `unity.gateway` (via
the rewritten Dockerfiles). Until then, the old code in
`communication/<channel>/` still serves traffic and the new code
exists only as testable Python in Unity.
