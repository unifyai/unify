# Local Gateway Setup

`droid.gateway` is the HTTP edge for external channels. It receives provider
webhooks, normalizes them into Droid envelopes, and forwards them into the local
ConversationManager ingress when running self-hosted.

## Start Locally

For day-to-day local testing, prefer the wrapper:

```bash
scripts/local.sh start --full
```

That starts:

- the gateway on `127.0.0.1:8001`
- the ConversationManager local ingress on `127.0.0.1:8787`

You can also start only the gateway:

```bash
python -m droid.gateway serve --host 127.0.0.1 --port 8001 --single-url --public-url https://your-public-callback.example
```

## Public Callback URL

External providers must be able to reach your laptop over public HTTPS. Droid
does not ship a tunnel service, but it works with any tunnel that gives you a
stable HTTPS base URL.

Common options:

- Cloudflare Tunnel: `cloudflared tunnel --url http://127.0.0.1:8001`
- ngrok: `ngrok http 8001`
- Tailscale Funnel: expose `http://127.0.0.1:8001` through your tailnet

Export the resulting URL:

```bash
export DROID_GATEWAY_PUBLIC_URL=https://your-public-callback.example
```

Then print provider URLs:

```bash
python -m droid.gateway urls --public-url "$DROID_GATEWAY_PUBLIC_URL"
```

## Setup, Doctor, and Smoke

Run the guided wizard for a local env file:

```bash
python -m droid.gateway setup --interactive --env-file .env
```

Useful variants:

```bash
python -m droid.gateway setup --channels twilio,gmail --write-env --env-file .env
python -m droid.gateway setup --channels twilio google --quick --interactive --env-file .env
python -m droid.gateway setup --channels all --non-interactive --env-file .env
```

Print setup guidance and credential placeholders:

```bash
python -m droid.gateway setup --channels twilio slack google --public-url "$DROID_GATEWAY_PUBLIC_URL"
```

Append missing credential placeholders to a local env file:

```bash
python -m droid.gateway setup --channels twilio --write-env --env-file .env
```

Validate URL shape and selected channel credentials:

```bash
python -m droid.gateway doctor --channels twilio slack --public-url "$DROID_GATEWAY_PUBLIC_URL" --check-credentials --env-file .env
python -m droid.gateway doctor --channels all --check-credentials --fix --env-file .env
```

Check the local gateway process:

```bash
python -m droid.gateway smoke --base-url http://127.0.0.1:8001 --public-url "$DROID_GATEWAY_PUBLIC_URL"
```

`doctor --fix` only performs safe local file repairs, currently appending missing
credential placeholders. It never buys phone numbers, creates provider apps, or
mutates provider dashboards.

## Channel And Capability Groups

The wizard registry covers:

- Local stack: Docker/Orchestra/Console URL wiring, gateway public URL, local
  ingress URL, admin auth, and local storage.
- Phone, SMS, and calls: Twilio plus optional LiveKit SIP credentials.
- WhatsApp and social verification: Twilio-backed WhatsApp sender and
  verification-code flows.
- Email and collaboration: Google/Gmail, Microsoft/Outlook/Teams/SharePoint,
  and generic email APIs.
- Chat apps: Slack and Discord.
- Model and voice capabilities: UniLLM/OpenAI/Anthropic plus Deepgram,
  ElevenLabs, Cartesia, and OpenAI voice/realtime keys where the local voice
  stack consumes them.
- Internal runtime endpoints: `/unify/message`, `/unify/attachment`,
  `/unify/meet`, `/droid/system-event`, and `/assistant/*`. These are smoke
  tested, not provider setup steps.

## What Droid Provides

Droid provides the gateway app, channel routes, webhook parsing, OAuth callback
routes, outbound channel APIs, local envelope delivery, and setup metadata.

## What You Still Provide

For real channels, the operator still provides:

- provider accounts
- provider credentials
- a public HTTPS callback URL
- OAuth app registrations and webhook subscriptions in provider consoles
- phone numbers, email identities, or bot/app installations

## Relationship to Hosted Communication

The private [`droid-deploy`](https://github.com/unifyai/droid-deploy) repository
is not required for self-hosted users. It wraps `droid.gateway` for hosted SaaS
and owns GCP, Kubernetes, DNS, scheduler, tunnel, and hosted runtime-activation
infrastructure.
