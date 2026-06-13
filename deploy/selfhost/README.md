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

## Daily commands

| Command | Effect |
|---------|--------|
| `unity` (or `unity up`) | Start or resume the stack |
| `unity down` | Stop Console UI; CM + scheduler keep running |
| `unity down --full` | Stop all services |
| `unity restart` | Recreate containers after editing `.env` |
| `unity status` | Show container status |
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
