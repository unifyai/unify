# LiveKit in Docker Compose (self-host)

## Port mapping

The compose file publishes:

- `7880/tcp` — WebSocket signaling (`ws://127.0.0.1:7880`)
- `7881/tcp` — TCP fallback
- `7882/udp` — WebRTC media (dev mode)

Console and the browser use `ws://127.0.0.1:7880`. CM and gateway use `ws://livekit:7880` on the internal network.

## Validation checklist

1. Start stack: `unity stack up`
2. Confirm LiveKit is up: `docker compose -f ~/.unity/docker-compose.yml ps livekit`
3. Open Console, start a voice call with Deepgram + Cartesia keys in `.env`
4. If media fails on macOS Docker Desktop:
   - Ensure UDP 7882 is not blocked
   - Try Chrome (WebRTC over TCP fallback on 7881)
   - Check `docker logs` on the `livekit` service for ICE errors

## Known limitation

Docker Desktop on macOS runs containers in a VM; UDP forwarding can be less reliable than on Linux. Treat voice as best-effort on macOS until validated on your machine.
