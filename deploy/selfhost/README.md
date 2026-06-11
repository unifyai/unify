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
| `unity` / `unity stack up` | Start or resume the stack |
| `unity stack down` | Stop Console UI; CM + scheduler keep running |
| `unity stack down --full` | Stop all services |
| `unity restart` | Recreate containers after editing `.env` |
| `unity stack doctor` | Docker + service health |
| `unity stack logs` | Follow compose logs |

## Configuration

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

- Docker Desktop or Docker Engine
- ~12 GB RAM recommended (desktop + CM + ML deps)
- Multi-GB disk for image pulls

## LiveKit / voice (compose)

Voice uses LiveKit in `--dev` mode with ports `7880` (WS), `7881` (TCP fallback), and `7882/udp` mapped to the host. If browser calls fail on macOS Docker Desktop, confirm these ports are reachable and not blocked by a firewall. See `deploy/selfhost/LIVEKIT_COMPOSE.md` for validation steps.

## Architecture

See `deploy/selfhost/docker-compose.yml` for the full service graph: Postgres, Orchestra, Pub/Sub emulator, LiveKit, gateway, Console, CM supervisor, desktop, and Caddy proxy.
