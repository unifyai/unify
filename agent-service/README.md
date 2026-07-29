# Magnitude BrowserAgent Service

This Node.js service acts as an HTTP wrapper for the Magnitude `BrowserAgent`, allowing a Python client (like the CodeActActor) to perform autonomous web automation tasks.

## Setup

1.  **Install Dependencies**: Ensure you have Node.js installed.

2.  **Build local magnitude-core (Unity fork setup)**:

    This repo uses a local checkout of Unity's modified `magnitude-core` via a file dependency (see `package.json`: `"magnitude-core": "file:../magnitude/packages/magnitude-core"`). The `magnitude/` directory contains our fork of the magnitude repository with Unity-specific enhancements.

    ```bash
    # First, clone the unity repository if you haven't already
    git clone <unity-repo-url>
    cd unity

    # Clone Unity's magnitude fork into the magnitude/ subdirectory (private repo - requires auth)
    # Option 1: Using CLONE_TOKEN environment variable
    git clone https://x-access-token:${CLONE_TOKEN}@github.com/unifyai/magnitude.git magnitude
    # Option 2: Using gh CLI (if authenticated)
    gh repo clone unifyai/magnitude magnitude

    cd magnitude
    git checkout unity-modifications  # Our branch with Unity enhancements

    # Build magnitude-core
    cd packages/magnitude-core
    npm install
    npm run build
    # optional: run tests/examples if needed
    ```

3.  **Install service deps**:

    From the `agent-service` directory, run:
    ```bash
    cd ../../..
    cd agent-service
    npm install
    ```

4.  **Create Environment File**: This service routes all LLM traffic through a UniLLM proxy and authenticates with Unify.
    Create a `.env` file in the root of this directory:
    ```
    # agent-service/.env
    ORCHESTRA_URL="..."
    UNIFY_KEY="..."
    # Hosted deploys usually provide UNITY_COMMS_URL; local gateway runs can
    # provide UNITY_GATEWAY_URL; use UNITY_UNILLM_URL to point at a specific
    # OpenAI-compatible UniLLM base URL directly.
    UNITY_COMMS_URL="..."
    # UNITY_GATEWAY_URL="http://localhost:8080"
    # UNITY_UNILLM_URL="http://localhost:8080/unillm"
    # UNITY_AGENT_SERVICE_LLM_MODEL="claude-4.6-sonnet@anthropic"
    # Optional - enables POST /captcha/solve to delegate reCAPTCHA v2
    # challenges to the AntiCaptcha worker pool.  Sign up at
    # https://anti-captcha.com, deposit ~$5 (covers ~10k v2 solves), and
    # copy the API key from the account dashboard.  When unset, the
    # /captcha/solve handler returns 503 anticaptcha_key_missing.
    ANTICAPTCHA_KEY="..."
    # Optional - managed egress. Lets a session choose which country its
    # traffic leaves from (see "Egress policy" below). Unset, only
    # egress.mode "direct" and "byo" are available; "region" is refused
    # rather than silently egressing from the host.
    UNITY_EGRESS_PROXY_SERVER="http://gate.provider.example:7777"
    # {region} / {REGION} (lower / upper case) and {session} are substituted;
    # residential providers encode geography and sticky-session handles in the
    # username. Match the provider's expected case — one that wants cc-GB and
    # receives cc-gb may ignore the geo-targeting rather than reject it.
    # Oxylabs, for example:
    #   customer-<account>-cc-{REGION}-sessid-{session}-sesstime-30
    UNITY_EGRESS_PROXY_USERNAME="account-country-{REGION}-session-{session}"
    UNITY_EGRESS_PROXY_PASSWORD="..."
    ```

## Egress policy

By default a browser session leaves from wherever the host sits, which for
hosted assistants is `us-central1`. That is a correctness problem before it is
anything else: an assistant researching prices or availability for a user in
Germany sees what a machine in Iowa sees.

`POST /start` accepts an optional `egress` object:

```jsonc
{ "mode": "direct" }                                  // unchanged; the default
{ "mode": "region", "region": "gb" }                  // managed provider
{ "mode": "byo", "proxy": { "server": "...", "username": "...", "password": "..." },
  "region": "gb" }                                    // caller-supplied exit
```

Three things move together, which is why this is a policy rather than a proxy
field: the exit itself, the **timezone / locale / `Accept-Language`** derived
from the region, and **WebRTC containment**. A proxied session still reporting
the host's timezone is a worse signal than an unproxied one, and Chromium
reveals the host address over STUN regardless of the HTTP proxy — so neither is
separately settable.

Resolution **fails closed**: a policy that cannot be honoured returns
`400 invalid_egress_policy` rather than falling back to direct egress, because
a caller that asked for a specific exit and silently got the host's own address
cannot tell that it happened.

`region` accepts an ISO 3166-1 alpha-2 code. Only regions we have contracted
egress for are supported — currently `gb` and `us` — and an unknown one is
refused with the supported set in the error. Adding a region is two lines in
`REGION_PROFILES` plus provider coverage; advertising one we cannot serve would
turn a clear refusal at configuration time into a session that starts and then
behaves wrongly.

## Running the Service

You can run the service by:

```bash
npx ts-node src/index.ts
```

The service will start and listen on `http://localhost:3000`.

## Developing with local magnitude-core changes

If you modify code in `magnitude/packages/magnitude-core`, rebuild it and refresh the local dependency in this service:

```bash
# In magnitude-core
cd magnitude/packages/magnitude-core
npm run build

# Back in agent-service - reinstall to pick up the updated local package
cd ../..
cd agent-service
npm install --force   # ensures the local file: dependency is re-copied
```

### Working with the Unity Fork

The `magnitude/` directory is our fork of the magnitude repository with Unity-specific modifications. Key points:

- **Branch**: Always work on `unity-modifications` branch
- **Upstream sync**: Use `upstream-main` branch to pull in latest magnitude changes
- **Private repo**: This is a private fork; use `CLONE_TOKEN` or `gh` CLI for authenticated access

See `MAGNITUDE_SETUP.md` in the repo root for detailed workflow instructions.

Notes:
- If you see runtime errors such as "Cannot find module './dist/...'", it means `magnitude-core` has not been built. Run `npm run build` in `magnitude-core`.
- For a tighter inner loop, you can also use `yalc` (optional):
  - In `magnitude-core`: `npm run build && npx yalc publish --push`
  - In `agent-service`: `npx yalc add magnitude-core`
  - Re-run the publish step after changes to auto-push updates.

## API Endpoints

-   `POST /nav`: Navigates the browser to a URL.
-   `POST /act`: Executes a high-level task on the current page.
-   `POST /extract`: Extracts structured data from the current page.
-   `GET /screenshot`: Returns a base64-encoded screenshot of the current page.
-   `POST /captcha/solve`: Delegates the on-page reCAPTCHA v2 challenge to the AntiCaptcha worker pool, then injects the returned Google-signed token back into the live page. Requires `ANTICAPTCHA_KEY`. Body: `{ sessionId, variant?: "v2_checkbox" | "v2_invisible" }`.
-   `POST /stop`: Gracefully shuts down the agent and browser.
-   `GET /health`: Checks if the service is ready to accept requests.
