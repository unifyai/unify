# unity.gateway

External communication gateway for Unity. This package connects the assistant
runtime to phone networks, email providers, chat platforms, internal
Console/Orchestra events, and provider webhooks.

Unity owns channel semantics in one place: webhook parsing, route resolution,
OAuth callbacks, envelope creation, and outbound provider calls live in
`unity.gateway`. Local self-hosted and hosted SaaS deployments run this same
code. Only the backend implementations differ.

## Backend Contracts

Deployment-specific behavior sits behind small protocols:

- `EnvelopeSink` delivers normalized inbound envelopes to Unity. Local mode uses
  an HTTP sink into the local runtime; hosted mode uses Pub/Sub.
- `RuntimeActivator` ensures the target assistant runtime is ready. Local mode
  treats the runtime as already running; hosted mode delegates to
  Communication's AssistantSession infrastructure.
- `Storage` stores attachments. Local mode uses local disk.
- `CredentialStore` reads operator provider credentials. Local mode uses
  environment variables.
- `PublicUrlProvider` builds public callback URLs for providers.
- `Scheduler` owns recurring maintenance in the active backend.

## Package Layout

```text
unity/gateway/
├── app.py                 # FastAPI app mounting channels and adapters
├── __main__.py            # python -m unity.gateway serve/doctor
├── context.py             # GatewayContext dependency injection
├── envelope_sink.py       # local/http/pubsub delivery backends
├── runtime.py             # local/hosted runtime activation backends
├── public_url.py          # callback URL construction
├── scheduler.py           # scheduler abstraction
├── envelopes.py           # canonical inbound envelope schemas
├── adapters/              # inbound provider/internal webhooks
├── channels/              # outbound/admin channel APIs
├── storage/               # storage backends
└── credentials/           # operator credential backends
```

`tests/gateway/` mirrors the structure with focused tests for contracts, route
shape, and selected channel behavior.

## Local Self-Hosted Usage

Run the gateway alongside local Orchestra, Console, and the ConversationManager:

```bash
python -m unity.gateway serve --port 8001 --single-url --public-url https://your-public-callback.example
```

`scripts/local.sh start` starts the gateway automatically and points
`UNITY_COMMS_URL`, `UNITY_ADAPTERS_URL`, and `LOCAL_ADAPTERS_URL` at the same
local process. In local mode, adapter routes publish through `HttpEnvelopeSink`
to the local Unity runtime.

External provider channels require provider credentials and a public HTTPS
callback URL. The gateway reads operator credentials from environment variables
such as `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `SLACK_SIGNING_SECRET`,
`GOOGLE_CLIENT_ID`, `GOOGLE_CLIENT_SECRET`, `MICROSOFT_CLIENT_ID`, and
`MICROSOFT_CLIENT_SECRET`.

Check configuration with:

```bash
python -m unity.gateway doctor --check-credentials
```

## Hosted Usage

The hosted `communication` service composes this gateway app and injects hosted
infrastructure backends: Pub/Sub envelope delivery and Communication's existing
AssistantSession activation infrastructure. Hosted-only VM pools, tunnels,
Cloud Scheduler/Tasks, DNS, and Kubernetes controllers remain in
`communication/infra`.

## Relationship to `unity.conversation_manager`

`unity.conversation_manager` remains the assistant runtime and live
conversation orchestrator. The gateway is the HTTP edge. Local inbound delivery
flows through `HttpEnvelopeSink` into the runtime-side local ingress endpoint;
hosted inbound delivery flows through Pub/Sub topics consumed by Unity runtime
workers.

## Relationship to `communication/`

`communication/` is the hosted infrastructure wrapper around `unity.gateway`.
The pieces that stay there are:

- `communication/infra/`: GCE pool VMs, Cloud DNS, ACME wildcard cert renewal,
  tunnel server, and hosted runtime activation.
- `communication/assistant_session_controller/`: Kubernetes per-binding session
  orchestration.
- `communication/cloudbuild/`, `communication/k8s/`,
  `communication/sbc-proxy/`, and `communication/scripts/`: deployment and
  runbook surfaces for Unity's hosted topology.
- `communication/Dockerfile-*`: hosted images that install Unity and run the
  hosted wrapper around `unity.gateway`.

Self-hosted users never touch the private repo. They install Unity, provide
provider credentials and a public callback URL, and run the Unity gateway
locally.

## Verification

Focused tests live under `tests/gateway/`:

```bash
tests/parallel_run.sh tests/gateway/
```

Provider SDK calls are mocked in unit tests. Real Twilio, Slack, Gmail, Outlook,
Teams, and Discord checks should be explicit smoke tests in an environment with
provider credentials and a reachable callback URL.
