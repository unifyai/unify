# unity.gateway.channels

Channel-specific FastAPI routers that bridge external messaging providers to
the Unity runtime. Local self-hosted deployments and hosted SaaS deployments use
the same route handlers; only the backend implementations behind
`GatewayContext` differ.

## Local Channel Setup

Run the local setup helper from the repository root:

```bash
python -m unity.gateway setup --interactive --env-file .env
python -m unity.gateway setup --channels twilio,slack --public-url https://your-public-callback.example
```

To print exact provider callback URLs:

```bash
python -m unity.gateway urls --public-url https://your-public-callback.example
```

To validate local configuration:

```bash
python -m unity.gateway doctor --check-credentials --channels all --env-file .env
python -m unity.gateway smoke --base-url http://127.0.0.1:8001 --public-url https://your-public-callback.example
```

The declarative channel setup metadata lives in `unity.gateway.local_setup`.
It lists the environment variables, callback paths, public HTTPS requirements,
and operator notes used by the CLI and tests. Channel behavior stays in the
existing router modules.

## Provider Recipes

### Twilio SMS and Phone

Required credentials:

- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`

Configure your Twilio phone number with the generated `/twilio/sms` and
`/twilio/call` callback URLs. Phone call flows may also use `/phone/twiml` and
`/phone/conference-status` on the same public HTTPS base URL.

Optional credentials include `LIVEKIT_URL`, `LIVEKIT_API_KEY`,
`LIVEKIT_API_SECRET`, and `LIVEKIT_SIP_URI` for full SIP call dispatch.

### Twilio WhatsApp

Required credentials:

- `TWILIO_WA_ACCOUNT_SID`
- `TWILIO_WA_AUTH_TOKEN`

Unity uses Twilio's WhatsApp transport. Configure the Twilio WhatsApp sender to
reach the generated public callback URLs.

### Social Verification

Required credentials:

- `TWILIO_ACCOUNT_SID`
- `TWILIO_AUTH_TOKEN`
- `ORCHESTRA_ADMIN_KEY`

Social verification is Twilio-backed outbound verification. It does not have a
provider callback URL of its own.

### Slack

Required credentials:

- `SLACK_SIGNING_SECRET`
- `ORCHESTRA_ADMIN_KEY`

Create a Slack app, enable the Events API, and set the request URL to the
generated `/slack/events` URL. Slack Socket Mode is not part of the current
Unity gateway surface.

### Google OAuth and Gmail

Required credentials:

- `GOOGLE_OAUTH_CLIENT_ID`
- `GOOGLE_OAUTH_CLIENT_SECRET`
- `ORCHESTRA_ADMIN_KEY`

Register the generated `/google/auth/callback` URL in Google Cloud. Gmail push
notifications use the generated `/email/gmail` endpoint and still require the
provider-side watch/PubSub setup for a real account.

### Microsoft OAuth, Outlook, Teams, and SharePoint

Required credentials:

- `MS365_BYOD_CLIENT_ID`
- `MS365_BYOD_CLIENT_SECRET`
- `ORCHESTRA_ADMIN_KEY`

Register the generated `/microsoft/auth/callback` URL in the Azure app
registration. Outlook and Teams notification subscriptions must use the
generated public HTTPS URLs.

Optional admin Graph credentials (`MS365_ADMIN_TENANT_ID`,
`MS365_ADMIN_CLIENT_ID`, `MS365_ADMIN_CLIENT_SECRET`) are only needed for
tenant-level provisioning and SharePoint-style app-only operations.

### Discord

Required credentials:

- `ORCHESTRA_ADMIN_KEY`

The local Discord surface covers bot registration, pool sync, status, and
outbound send routes. Discord interaction webhooks are not currently part of
the Unity gateway surface.

### Generic Email

Required credentials:

- `ORCHESTRA_ADMIN_KEY`

Generic email routes cover provider-agnostic send and attachment operations.
Configure Google/Gmail or Microsoft/Outlook for provider-specific inbox
watching.

### Local Capabilities

`local-stack`, `unillm`, and `voice` are capability groups rather than external
message channels. They cover local URL wiring, model provider keys, LiveKit, and
voice/audio provider keys. Internal Console/runtime endpoints are grouped under
`internal` and validated by smoke tests rather than provider dashboards.

## What Is Still External

Self-hosted users still need provider accounts, provider credentials, a public
HTTPS callback URL, and any provider-side app/webhook/OAuth configuration. Unity
does not run a tunnel service or mutate provider consoles.

Hosted SaaS runs the private [`unity-deploy`](https://github.com/unifyai/unity-deploy)
repository for GCP, Kubernetes, DNS, scheduler, tunnel, and runtime-activation
infrastructure. Self-hosted users do not need that repository.
