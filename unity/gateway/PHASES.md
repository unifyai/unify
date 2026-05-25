# Gateway migration phases

This document is the operational rollout plan for moving Unity's
external-comms transport layer out of the private `communication`
repository and into `unity.gateway`. It is structured so that each
phase is a self-contained PR with its own risk profile and rollback
path.

## Phase A — Foundation (this PR)

**Goal.** Establish the `unity.gateway` package with the protocols,
default in-process implementations, and envelope schemas that
subsequent phases build on. **Purely additive in Unity. No production
impact. Fully reversible.**

**Scope.**

- `unity/gateway/event_broker.py` — `EventBroker` + `PubSubConnection`
  protocols mirroring the existing `InMemoryEventBroker` surface.
- `unity/gateway/envelopes.py` — canonical webhook envelope Pydantic
  schemas. Concrete models for `msg`, `email`, `unify_message`,
  `unity_system_event`; `GenericEnvelope` fallback for the remaining 13
  thread types. Full catalogue documented in `KNOWN_THREADS`.
- `unity/gateway/storage/` — `Storage` protocol, `LocalDiskStorage`
  default, `GcsStorage` stub.
- `unity/gateway/secrets/` — `SecretManager` protocol,
  `EnvSecretManager` default, `GcpSecretManager` stub.
- `tests/gateway/` — focused tests for every module above.
- This document and `README.md`.

**Out of scope (deferred).**

- No changes to `unity/conversation_manager/comms_manager.py`. The
  inline `from google.cloud import pubsub_v1` stays where it is.
- No changes to `unity/conversation_manager/event_broker.py`. The
  existing `get_event_broker()` factory is unchanged.
- No changes to the `communication` repository, any Dockerfile, any
  Cloud Build config, or any deployed service.

**Production impact:** none.

**Rollback:** `git revert` of the single PR. Removes the new
package; nothing else is affected.

---

## Phase A.bis — Pub/Sub extraction

**Goal.** Move the Pub/Sub-specific code currently inline in
`comms_manager.py` into a `PubSubEventBroker` implementation that
satisfies the Phase A `EventBroker` protocol. Wire selection through a
`UNITY_EVENT_BROKER` env var (default `inmemory`; hosted sets
`pubsub`).

**Scope.**

- New `unity/gateway/event_broker_pubsub.py` with `PubSubEventBroker`.
  Wraps `google.cloud.pubsub_v1` behind the `EventBroker` interface
  including the existing threading model (Pub/Sub callbacks on the
  background thread pool marshalled into the asyncio loop via
  `asyncio.run_coroutine_threadsafe`).
- Update `unity/conversation_manager/event_broker.py` to switch on
  `UNITY_EVENT_BROKER`: `inmemory` -> `InMemoryEventBroker`, `pubsub`
  -> `PubSubEventBroker`.
- Update `unity/conversation_manager/comms_manager.py` to remove the
  inline Pub/Sub imports and code paths in favour of broker-only
  calls. The `events_map` and `dispatch_envelope_payload` stay; only
  the transport disappears.
- Update `unity/conversation_manager/settings.py` to expose
  `UNITY_EVENT_BROKER` as a typed setting (default `inmemory`).
- Add `tests/gateway/test_event_broker_pubsub.py` exercising
  `PubSubEventBroker` against a Pub/Sub emulator (or mocked, scoped
  carefully).
- Sanity-run the existing `tests/conversation_manager/` suite to
  confirm the refactor preserves behaviour.

**Out of scope.**

- No `communication` changes. The hosted code path still reads from
  the same Pub/Sub topics and subscriptions it does today; only the
  Unity-side broker abstraction changes.

**Production impact:** zero at deploy time (Unity is not redeployed
in this PR). The risk is that the *next* Unity deployment (a future
Cloud Build of `unity-comms-app` or whatever consumes Unity for the
hosted session worker image) carries the refactored broker code.
Mitigated by:

- Default env value (`inmemory`) leaves the test suite unchanged.
- Hosted sets `UNITY_EVENT_BROKER=pubsub` via the existing deploy
  envs.
- Soak in staging for 24h before promoting to production.

**Rollback:** revert the PR. The hosted deployment falls back to the
inline `pubsub_v1` code path.

---

## Phase B — Mirror channels into Unity

**Goal.** Bring every channel router, the shared library, and the
adapter app into `unity/gateway/`, translated to use the Phase A
abstractions. The old code in `communication/` keeps serving production
unchanged.

**Scope.** Per-channel mechanical port:

| Source | Destination | LOC |
| --- | --- | --- |
| `communication/phone/views.py` | `unity/gateway/channels/phone/views.py` | 366 |
| `communication/gmail/views.py` | `unity/gateway/channels/gmail/views.py` | 306 |
| `communication/outlook/views.py` | `unity/gateway/channels/outlook/views.py` | 445 |
| `communication/whatsapp/views.py` | `unity/gateway/channels/whatsapp/views.py` | 568 |
| `communication/discord/views.py` + `gateway.py` | `unity/gateway/channels/discord/` | 189 + ~600 |
| `communication/teams/views.py` | `unity/gateway/channels/teams/views.py` | 741 |
| `communication/email/views.py` | `unity/gateway/channels/email/views.py` | 102 |
| `communication/social/views.py` | `unity/gateway/channels/social/views.py` | 124 |
| `communication/sharepoint/views.py` | `unity/gateway/channels/sharepoint/views.py` | 511 |
| `communication/unillm/views.py` | `unity/gateway/channels/unillm/views.py` | 115 |
| `communication/adapters/main.py` | `unity/gateway/adapters/main.py` | 4050 |
| `communication/common/` | `unity/gateway/common/` | ~600 |

Translation rules applied to every file:

- `from common.settings import SETTINGS` -> `from unity.gateway.settings import SETTINGS`
- Direct `pubsub_v1.PublisherClient` calls -> `await broker.publish(channel, json.dumps(envelope))`
- Direct `google.cloud.storage` calls -> `await storage.write_bytes(key, data)` / `await storage.read_bytes(key)`
- Direct `google.cloud.secret_manager` calls -> `secrets.get(name)`
- Direct envelope dict construction -> Pydantic `XEnvelope(...)` from `unity.gateway.envelopes`
- Add a concrete envelope schema in `unity/gateway/envelopes.py` for
  each `thread` value the channel publishes, replacing
  `GenericEnvelope` fallback for that thread.

New entrypoints in Unity:

- `unity/gateway/app.py` — single `FastAPI` app mounting all
  `channels/*/views.py` and `adapters/main.py` routers, identical in
  shape to today's `communication/main.py` and `adapters/main.py`.
- `unity/gateway/cli.py` — `unity gateway serve` (mounts comms-side
  routes) and `unity gateway adapters` (mounts adapter-side routes).
  Both invoke the same FastAPI app with different router subsets,
  controlled by an env-driven `_GATEWAY_MOUNT` selector.

Per-channel tests are ported from `communication/tests/<channel>/` to
`tests/gateway/channels/<channel>/`.

**Out of scope.**

- No production cutover. The new code runs only under
  `tests/gateway/` and via local `unity gateway serve`. Production
  still serves from `communication/main.py`.
- No deletion of code in `communication/`. That is Phase D.

**Production impact:** zero. The hosted services still run from the
private repo.

**Rollback:** revert per-channel PRs individually if any port turns up
issues. Each channel is its own commit.

---

## Phase C — Production cutover

**Goal.** Switch the two production Cloud Run services
(`unity-comms-app`, `unity-adapters`) to run on `unity.gateway` code
instead of `communication/*/views.py`. Achieved by changing the
Dockerfiles in `communication/` to `pip install unity` and `CMD ["unity",
"gateway", "serve"]` (or `adapters`).

**Scope.**

- Update `communication/Dockerfile-comms` to clone Unity at the
  branch matching the build, `pip install` it, and replace the
  `CMD ["uvicorn", "communication.main:app", ...]` line with
  `CMD ["unity", "gateway", "serve", "--host=0.0.0.0", "--port=8080"]`.
- Update `communication/Dockerfile-adapters` the same way, with
  `CMD ["unity", "gateway", "adapters", "--host=0.0.0.0", "--port=8080"]`.
- Update `communication/cloudbuild/unity-comms-app.yaml` and
  `communication/cloudbuild/adapters.yaml` if any env vars or build
  args differ (`UNITY_EVENT_BROKER=pubsub`,
  `UNITY_GATEWAY_STORAGE_BACKEND=gcs`,
  `UNITY_GATEWAY_SECRETS_BACKEND=gcp`).
- Keep `communication/cloudbuild/*-staging.yaml` driven off the
  Unity feature branch first.

**Rollout sequence.**

1. **Staging adapters cutover.** Deploy new `unity-adapters-staging`
   built from the changed `Dockerfile-adapters`. Soak 24h. Verify
   every inbound webhook (Twilio voice, Twilio SMS, Twilio WhatsApp,
   Gmail watch, Outlook subscription, Teams subscription) still
   produces an identical Pub/Sub message shape compared to the
   pre-cutover baseline. Pre-cutover diff baseline captured by
   sampling 100 inbound events with a logger before any change.
2. **Staging comms cutover.** Deploy new `unity-comms-app-staging`
   built from the changed `Dockerfile-comms`. Soak 24h. Verify every
   admin-authenticated outbound API still works end-to-end by running
   a smoke suite that exercises one of every channel.
3. **Production adapters cutover.** Cloud Run traffic split: 10% to
   the new revision for 4h, 50% for 4h, 100% on success. Rollback by
   shifting traffic back to the previous revision (one
   `gcloud run services update-traffic` call).
4. **Production comms cutover.** Same traffic-split sequence.

**Production impact:** real, mitigated by staging soak + traffic
split. **Requires human eyes on the Cloud Run dashboards** during
each promotion step.

**Rollback at each step:**

- Pre-promotion: revert the PR.
- Mid-promotion: `gcloud run services update-traffic <service>
  --to-revisions=<previous-revision>=100`.
- Post-promotion: redeploy the previous Cloud Build image tag.

---

## Phase D — Slim `communication/`

**Goal.** After Phase C has been stable in production for one week
with no incidents, delete the now-vestigial channel and adapter code
from `communication/`. The repo reduces to its true private surface:
hosted-SaaS infrastructure.

**Scope.**

- Delete from `communication/`:
  - `phone/`, `gmail/`, `outlook/`, `whatsapp/`, `discord/`, `teams/`,
    `email/`, `social/`, `sharepoint/`, `unillm/`
  - `adapters/`
  - `common/`
  - `main.py`, `dependencies.py`
  - The `[tool.setuptools.packages.find]` entries for the above.
- Keep in `communication/`:
  - `infra/` (GCE pool VMs, Cloud DNS, ACME, tunnel server)
  - `assistant_session_controller/` (K8s session orchestrator)
  - `k8s/`, `cloudbuild/`, `scripts/`, `sbc-proxy/`
  - `Dockerfile-comms`, `Dockerfile-adapters` (now Unity-based)
- Update `communication/README.md` to reflect the new scope: this is
  the hosted-SaaS deployment + per-assistant desktop infrastructure
  layer, not the channel transport layer.

**Production impact:** none if Phase C is complete. The deleted files
are no longer imported by the deployment.

**Rollback:** revert the deletion PR; the deleted files are restored
from git history. (No production state to roll back, since by
definition no one is using them at this point.)

---

## Cross-phase invariants

- The on-wire envelope shape across Phase B/C **must not change**. The
  `{thread, publish_timestamp, event: {...}}` Pydantic schemas in
  `unity/gateway/envelopes.py` are derived from the existing wire
  format and must stay backward-compatible. Any new field is
  additive; no field is renamed or repurposed.
- Pub/Sub topic names and subscription IDs **must not change** during
  Phases B/C. Consumers and publishers in the hosted code path must
  continue to read from `assistant_topic(assistant_id)` and write to
  `unity-<agent_id>-sub` exactly as today.
- The admin auth contract (`Authorization: Bearer
  <ORCHESTRA_ADMIN_KEY>`) **must not change** during Phases B/C.
- No phase requires a coordinated Orchestra change. Orchestra
  contracts (the REST API Unity already calls today) are untouched.

## Reference designs

The terminology and module boundaries follow:

- `openclaw/openclaw` — MIT, organises its external comms surface
  under `gateway/`. The "gateway is just the control plane; the
  product is the assistant" framing in their README directly mirrors
  the split we are adopting.
- `NousResearch/hermes-agent` — MIT, runs the same gateway pattern
  with Telegram and other channels in-tree, deployable to a $5 VPS.

The end-state for self-hosted Unity matches both: a single repository
the user clones, with the gateway surface in `gateway/` (here:
`unity/gateway/`), a single `serve` command that brings up the
external-comms HTTP boundary, and an `InMemoryEventBroker` /
`LocalDiskStorage` / `EnvSecretManager` default that works without any
cloud account.
