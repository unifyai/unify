# Local Gateway Setup

`unity.gateway` is the HTTP edge for external channels. It receives provider
webhooks, normalizes them into Unity envelopes, and forwards them into the local
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
python -m unity.gateway serve --host 127.0.0.1 --port 8001 --single-url --public-url https://your-public-callback.example
```

## Public Callback URL

External providers must be able to reach your laptop over public HTTPS. Unity
does not ship a tunnel service, but it works with any tunnel that gives you a
stable HTTPS base URL.

Common options:

- Cloudflare Tunnel: `cloudflared tunnel --url http://127.0.0.1:8001`
- ngrok: `ngrok http 8001`
- Tailscale Funnel: expose `http://127.0.0.1:8001` through your tailnet

Export the resulting URL:

```bash
export UNITY_GATEWAY_PUBLIC_URL=https://your-public-callback.example
```

Then print provider URLs:

```bash
python -m unity.gateway urls --public-url "$UNITY_GATEWAY_PUBLIC_URL"
```

## Setup, Doctor, and Smoke

Print setup guidance and credential placeholders:

```bash
python -m unity.gateway setup --channels twilio slack google --public-url "$UNITY_GATEWAY_PUBLIC_URL"
```

Append missing credential placeholders to a local env file:

```bash
python -m unity.gateway setup --channels twilio --write-env --env-file .env
```

Validate URL shape and selected channel credentials:

```bash
python -m unity.gateway doctor --channels twilio slack --public-url "$UNITY_GATEWAY_PUBLIC_URL" --check-credentials
```

Check the local gateway process:

```bash
python -m unity.gateway smoke --base-url http://127.0.0.1:8001 --public-url "$UNITY_GATEWAY_PUBLIC_URL"
```

## What Unity Provides

Unity provides the gateway app, channel routes, webhook parsing, OAuth callback
routes, outbound channel APIs, local envelope delivery, and setup metadata.

## What You Still Provide

For real channels, the operator still provides:

- provider accounts
- provider credentials
- a public HTTPS callback URL
- OAuth app registrations and webhook subscriptions in provider consoles
- phone numbers, email identities, or bot/app installations

## Relationship to Hosted Communication

The private `communication` repository is not required for self-hosted users. It
wraps `unity.gateway` for hosted SaaS and owns GCP, Kubernetes, DNS, scheduler,
tunnel, and hosted runtime-activation infrastructure.
