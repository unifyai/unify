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
├── __main__.py            # python -m unity.gateway serve/setup/doctor/urls/smoke
├── context.py             # GatewayContext dependency injection
├── local_setup.py         # local channel setup metadata
├── wizard.py              # interactive env-file setup wizard
├── local-setup.md         # local operator setup guide
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
`GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`,
`MS365_BYOD_CLIENT_ID`, and `MS365_BYOD_CLIENT_SECRET`.

Run the local setup wizard, print callback URLs, and validate configuration with:

```bash
python -m unity.gateway setup --interactive
python -m unity.gateway setup --channels twilio google --write-env --env-file .env
python -m unity.gateway urls --public-url https://your-public-callback.example
python -m unity.gateway doctor --check-credentials --channels all --env-file .env
python -m unity.gateway smoke --base-url http://127.0.0.1:8001 --check-credentials
```

The setup registry covers user-facing channels (`twilio`, `whatsapp`, `social`,
`slack`, `google`, `microsoft`, `discord`, `email`), local capabilities
(`local-stack`, `unillm`, `voice`), and internal Console/runtime adapter
endpoints (`internal`). Internal endpoints are validated by smoke and
compatibility tests rather than configured in provider dashboards.

## Hosted Usage

The hosted deployment in [`unity-deploy`](https://github.com/unifyai/unity-deploy)
composes this gateway app and injects hosted infrastructure backends: Pub/Sub
envelope delivery and the hosted AssistantSession activation infrastructure.
Hosted-only VM pools, tunnels, Cloud Scheduler/Tasks, DNS, and Kubernetes
controllers live in `unity-deploy` as well.

## Relationship to `unity.conversation_manager`

`unity.conversation_manager` remains the assistant runtime and live
conversation orchestrator. The gateway is the HTTP edge. Local inbound delivery
flows through `HttpEnvelopeSink` into the runtime-side local ingress endpoint;
hosted inbound delivery flows through Pub/Sub topics consumed by Unity runtime
workers.

## Relationship to `unity-deploy`

[`unity-deploy`](https://github.com/unifyai/unity-deploy) is the hosted
infrastructure wrapper around `unity.gateway`. The hosted-only pieces that live
there are:

- GCE pool VMs, Cloud DNS, ACME wildcard cert renewal, tunnel server, and
  hosted runtime activation.
- The Kubernetes per-binding AssistantSession orchestration.
- The deploy and runbook surfaces (Cloud Build, k8s manifests, SBC proxy, ops
  scripts) for Unity's hosted topology.
- The hosted Docker images that install Unity and run the wrapper around
  `unity.gateway`.

(These previously lived in a separate `communication` repository, now archived;
the hosted runtime, adapters, and infrastructure moved into `unity-deploy`.)

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
