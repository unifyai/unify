# Self-Host Docker Compose

Stranger install path using prebuilt images and Docker Compose.

## Quick start

```bash
curl -fsSL https://raw.githubusercontent.com/unifyai/unity/staging/scripts/install.sh | bash
```

Requires **Docker** only. The installer:

1. Writes `~/.unity/docker-compose.yml` and `.env`
2. Runs the BYOK wizard (LLM + voice keys)
3. Pulls GHCR images and starts the stack
4. Opens Console at http://127.0.0.1:3000

Register on `/login`, then chat with your Coordinator.

**What you get out of the box:** Marty can chat, call, and use a **managed Linux desktop** inside Docker (view it from Console during a call via assistant screen share — served at `http://127.0.0.1:8090`).

**macOS — control your physical Mac:** That requires a one-time host install (Screen Sharing + Unify Desktop Assistant). See [Control your Mac](#control-your-mac-macos) below — do this if you want Marty to operate **your apps and files on this machine**, not only the Docker sandbox.

## Control your Mac (macOS)

Use this when Marty should drive **your physical Mac** (Finder, Chrome, logged-in apps). Skip it if the managed Docker desktop is enough.

### 1. Finish the compose install first

Complete [Quick start](#quick-start) above: `unity stack up`, sign in at Console, hire or open your Coordinator.

Copy **your API key** from Console: assistant row → **⋯** → **Connect your desktop** → **Copy API Key**. This is your Orchestra user key (the same one Marty uses), not `ORCHESTRA_ADMIN_KEY`. Paste it into the Desktop Assistant installer or tray **Settings…** when prompted. It is **not** written to `~/.unity/.env` (that file is only for stack secrets and BYOK provider keys).

### 2. Install Unify Desktop Assistant

Download the latest **`unify-desktop-assistant_*_macos.pkg`** from [GitHub Releases](https://github.com/unifyai/unify-desktop-assistant/releases) and run the installer.

Full tray-app details: [unify-desktop-assistant/macos/README.md](https://github.com/unifyai/unify-desktop-assistant/blob/staging/macos/README.md).

**Developer alternative** (no `.pkg`): clone [unify-desktop-assistant](https://github.com/unifyai/unify-desktop-assistant) and run:

```bash
cd unify-desktop-assistant/macos/tools
./setup.sh --self-host --unify-key YOUR_KEY --link-coordinator
```

`YOUR_KEY` is the same **Copy API Key** value from Console (**Connect your desktop**), not `ORCHESTRA_ADMIN_KEY` from `~/.unity/.env`.

### 3. Enable Screen Sharing

The Desktop Assistant installer (or `setup.sh`) turns on **Screen Sharing** (Remote Management) and starts background services via launchd:

| Service | Port | Role |
|---------|------|------|
| Apple Screen Sharing (VNC) | 5900 | Your Mac's display |
| websockify (noVNC) | 6080 | Local proxy for the agent |
| agent-service | 13000 | Unity control API (Console keeps **3000**) |

On first run, macOS may prompt for **Screen Sharing** / **Accessibility** permissions — approve them.

If services show red in the menu-bar app, open **Unify Desktop Assistant → Settings…**, paste your API key again (compose self-host is detected automatically), or run **Start Services**.

Verify locally (optional): tray app → open desktop viewer, or check the menu-bar status is green.

### 4. Register and link in Console

With `unity stack up` running, open **Unify Desktop Assistant → Settings…** and paste your API key (from **Connect your desktop** in Console) if you did not enter it during install. Compose self-host is auto-detected (`~/.unity/docker-compose.yml`); setup registers `http://host.docker.internal:13000` and links the Coordinator.

Then in Console → assistant **⋯** → **Connect your desktop**:

- Confirm your Mac appears in the list and link it to the Coordinator if needed.
- **Save User Password** (macOS login password) if prompted — used later for unlock/accessibility.

### 5. Restart Unity so CM picks up the link

```bash
unity restart
```

Ask Marty to do something on your Mac (e.g. “take a screenshot of my desktop”). You do **not** need to open `http://127.0.0.1:6080/vnc.html` — that URL is for local debugging; Console shows Marty’s **managed** desktop at `:8090`, not your Mac’s noVNC feed.

A copy of this guide is written to `~/.unity/README.md` when you run the installer.

## Daily commands

| Command | Effect |
|---------|--------|
| `unity` (or `unity up`) | Start or resume the stack |
| `unity down` | Stop Console UI; CM + scheduler keep running |
| `unity down --full` | Stop all services |
| `unity restart` | Recreate containers after editing `.env` |
| `unity status` | Show container status |
| `unity smoke` | Verify the running local stack end-to-end |
| `unity logs [service...]` | Follow logs (optionally for specific services) |
| `unity pull` | Pull the latest images |
| `unity doctor` | Docker + key + service health |
| `unity integrations-sync` | Sync the Composio app catalog (needs `COMPOSIO_API_KEY`) |

Every command is also available under `unity stack <command>` (e.g. `unity stack logs`).

## Configuration

The installer generates local secrets (`POSTGRES_PASSWORD`, `ORCHESTRA_ADMIN_KEY`,
`NEXTAUTH_SECRET`, `JWT_SECRET`) and runs the BYOK wizard for provider keys.

| Key | Required for |
|-----|----------------|
| `OPENAI_API_KEY`, `ANTHROPIC_API_KEY`, or `DEEPSEEK_API_KEY` | Coordinator chat (wizard: pick one) |
| `OPENAI_API_KEY` | Tool-search embeddings (recommended even with other chat providers) |
| `DEEPGRAM_API_KEY` + `CARTESIA_API_KEY` or `ELEVEN_API_KEY` | Browser voice calls (STT + TTS) |
| `UNIFY_MODEL` | Optional override; Unity picks a default when unset |

On first `docker compose up`, the one-shot `orchestra-seed` service inserts billing
plan rows Postgres needs before registration. Always start with `unity stack up`
(full stack) — starting individual services manually can skip that seed step.

Edit `~/.unity/.env` for BYOK keys and secrets. After changes:

```bash
unity restart
```

Workspace files live at `~/Unity/Local` (bind-mounted into CM and desktop containers).

## Developer source install

Clone repos and run from source (for Unity development):

```bash
curl -fsSL .../install.sh | bash -s -- --source-install
```

This clones `unity`, `unify`, `unillm`, `console`, and `orchestra` and runs the
one-time bootstrap. From then on the **single canonical local end-to-end
command is `unity stack up`** (alias: `unity`), which starts Orchestra, the
Unity gateway, Console (in self-host mode), and the Coordinator runtime.
Re-run the bootstrap any time with `unity setup`.

```bash
unity setup        # one-time bootstrap (local Orchestra, Console env, voice)
unity stack up     # the one command to run the whole stack (alias: unity)
```

Console's own `scripts/local.sh` is an internal dev/test harness (seeded dev
data, E2E tests) and is not the way to run the product locally — `unity stack
up` invokes it with `--self-host` for you.

## System requirements

- macOS, Linux, or Windows via WSL2
- Docker Desktop or Docker Engine
- ~12 GB RAM recommended (desktop + CM + ML deps)
- Multi-GB disk for image pulls

## LiveKit / voice (compose)

Voice uses LiveKit in `--dev` mode with ports `7880` (WS), `7881` (TCP fallback), and `7882/udp` mapped to the host. If browser calls fail on macOS Docker Desktop, confirm these ports are reachable and not blocked by a firewall. See `deploy/selfhost/LIVEKIT_COMPOSE.md` for validation steps.

## Architecture

See `deploy/selfhost/docker-compose.yml` for the full service graph: Postgres, Orchestra, Pub/Sub emulator, LiveKit, gateway, Console, CM supervisor, desktop, and Caddy proxy.

Entrypoint scripts (`cm-entrypoint.sh`, `desktop-entrypoint.sh`, `publish-desktop-ready.sh`, `ensure-pubsub-topics.sh`) ship inside the `unity-selfhost` and `unity-desktop-selfhost` images. After changing them, rebuild and publish those images — editing copies under `~/.unity/` does not affect running containers.
