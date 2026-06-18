# droid.gateway

External communication gateway for Droid. This package connects the assistant
runtime to phone networks, email providers, chat platforms, internal
Console/Orchestra events, and provider webhooks.

Droid owns channel semantics in one place: webhook parsing, route resolution,
OAuth callbacks, envelope creation, and outbound provider calls live in
`droid.gateway`. Local self-hosted and hosted SaaS deployments run this same
code. Only the backend implementations differ.

## Backend Contracts

Deployment-specific behavior sits behind small protocols:

- `EnvelopeSink` delivers normalized inbound envelopes to Droid. Local mode uses
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
droid/gateway/
├── app.py                 # FastAPI app mounting channels and adapters
├── __main__.py            # python -m droid.gateway serve/setup/doctor/urls/smoke
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
python -m droid.gateway serve --port 8001 --single-url --public-url https://your-public-callback.example
```

`scripts/local.sh start` starts the gateway automatically and points
`DROID_COMMS_URL`, `DROID_ADAPTERS_URL`, and `LOCAL_ADAPTERS_URL` at the same
local process. In local mode, adapter routes publish through `HttpEnvelopeSink`
to the local Droid runtime.

External provider channels require provider credentials and a public HTTPS
callback URL. The gateway reads operator credentials from environment variables
such as `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`, `SLACK_SIGNING_SECRET`,
`GOOGLE_OAUTH_CLIENT_ID`, `GOOGLE_OAUTH_CLIENT_SECRET`,
`MS365_BYOD_CLIENT_ID`, and `MS365_BYOD_CLIENT_SECRET`.

Run the local setup wizard, print callback URLs, and validate configuration with:

```bash
python -m droid.gateway setup --interactive
python -m droid.gateway setup --channels twilio google --write-env --env-file .env
python -m droid.gateway urls --public-url https://your-public-callback.example
python -m droid.gateway doctor --check-credentials --channels all --env-file .env
python -m droid.gateway smoke --base-url http://127.0.0.1:8001 --check-credentials
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

## Relationship to `droid.conversation_manager`

`droid.conversation_manager` remains the assistant runtime and live
conversation orchestrator. The gateway is the HTTP edge. Local inbound delivery
flows through `HttpEnvelopeSink` into the runtime-side local ingress endpoint;
hosted inbound delivery flows through Pub/Sub topics consumed by Droid runtime
workers.

## Relationship to `unity-deploy`

[`unity-deploy`](https://github.com/unifyai/unity-deploy) is the hosted
infrastructure wrapper around `droid.gateway`. The hosted-only pieces that live
there are:

- GCE pool VMs, Cloud DNS, ACME wildcard cert renewal, tunnel server, and
  hosted runtime activation.
- The Kubernetes per-binding AssistantSession orchestration.
- The deploy and runbook surfaces (Cloud Build, k8s manifests, SBC proxy, ops
  scripts) for Droid's hosted topology.
- The hosted Docker images that install Droid and run the wrapper around
  `droid.gateway`.

(These previously lived in a separate `communication` repository, now archived;
the hosted runtime, adapters, and infrastructure moved into `unity-deploy`.)

Self-hosted users never touch the private repo. They install Droid, provide
provider credentials and a public callback URL, and run the Droid gateway
locally.

## Verification

Focused tests live under `tests/gateway/`:

```bash
tests/parallel_run.sh tests/gateway/
```

Provider SDK calls are mocked in unit tests. Real Twilio, Slack, Gmail, Outlook,
Teams, and Discord checks should be explicit smoke tests in an environment with
provider credentials and a reachable callback URL.
