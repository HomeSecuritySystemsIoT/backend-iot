# IoT Backend Service

Node.js server that accepts connections from ESP32-S3 camera nodes, streams live JPEG frames to browser clients via WebSocket, and detects motion server-side using frame diffing (`sharp`).

Dependencies: `ws`, `sharp`.

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 7890 | TCP | WebSocket (live video) + HTTP (SSE motion events, `/devices`) |
| 7891 | TCP | JPEG frame stream from ESP32 |

> Port 7892/udp is exposed in the Docker config but is not used by the server — leftover from a previous version.

## How it works

### TCP (ESP32 → Server, port 7891)

On connect, the server sends an `I` command. The ESP32 should reply with its unique ID (e.g. MAC address) followed by `\n`. If no reply arrives within 2 s, the server falls back to the remote IP as the device ID.

After identification, the server drives the ESP32 with periodic commands:

| Command | Meaning |
|---------|---------|
| `I` | Identify — server asks ESP32 for its ID at connect time |
| `G` | Grab — capture and send one JPEG frame (sent at ~1 fps while clients are connected) |
| `P` | Ping/keepalive — sent every 5 s when no clients are watching |

Frames arrive as a 4-byte **little-endian** size header followed by that many bytes of JPEG:

```
ESP32 → Server : [4 bytes LE: size][size bytes: JPEG]
```

Each frame is:
1. Broadcast via WebSocket to all browser clients subscribed to that device.
2. Compared against the previous frame for motion detection (server-side, using `sharp`).

Frames are **not** written to disk.

### WebSocket (browser → Server, port 7890)

Connect with `ws://<host>:7890?device=<deviceId>` to receive a live JPEG stream for a device. Each message is a raw JPEG buffer.

While at least one WebSocket or SSE client is connected, the server requests frames from the ESP32 at 1 fps. When all clients disconnect, it switches to a 5 s keepalive ping.

### HTTP / SSE (port 7890)

**`GET /motion?device=<deviceId>`** — Server-Sent Events stream of motion alerts.

Each event payload:
```json
{ "deviceId": "AA:BB:CC:DD:EE:FF", "changedPercent": 0.412, "ts": "2025-01-01T00:00:00.000Z" }
```

Motion is triggered when more than 35 % of pixels change by more than 25 brightness levels between consecutive frames.

**`GET /devices`** — Returns the list of currently connected device IDs.
```json
{ "devices": ["AA:BB:CC:DD:EE:FF", "192.168.1.43"] }
```

## Multiple ESP32s

Each ESP32 is tracked by its device ID (MAC from the identification handshake, or IP as fallback). WebSocket and SSE subscriptions are per-device.

## Run

```bash
npm install
node server.js
# or
npm start
```

## Docker

```bash
docker compose up --build
```

## VPS firewall

```bash
ufw allow 7890/tcp
ufw allow 7891/tcp
```
