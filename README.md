# IoT Backend Service

Standalone Node.js server that accepts connections from ESP32-S3 camera nodes. 

## Ports

| Port | Protocol | Purpose |
|------|----------|---------|
| 7891 | TCP | JPEG frame stream from ESP32 |
| 7892 | UDP | Motion detection events from ESP32 |

## How it works

### TCP (frames only)
The ESP32 connects as a TCP client and only sends data — raw JPEG frames prefixed with a 4-byte little-endian size header. The server reassembles and saves each frame.

```
ESP32 → Server : [4 bytes: size][size bytes: JPEG]
```

### UDP (motion detection + commands)
UDP is bidirectional. The ESP32 sends motion detection values to the server, and the server sends commands back to the ESP32 on the same endpoint.

```
ESP32  → Server : [4 bytes: uint32 motion diff]
Server → ESP32  : "G"   ← request next frame
Server → ESP32  : "S"   ← stop
...
```

The server learns the ESP32's UDP address and port the first time it receives a packet from it. Commands are only sent after that first contact.

Available commands:

| Command | Meaning |
|---------|---------|
| `G` | Grab — capture and send a frame |
| `S` | Stop streaming |
| `N` | Normal mode |
| `M` | Motion detection mode |


## Multiple ESP32s

Each ESP32 is identified by its IP address. Frames are saved under a per-device folder:

```
images_received/
├── 192_168_1_42/
│   ├── 1714123456789_1.jpg
│   └── 1714123456999_2.jpg
└── 192_168_1_43/
    └── 1714123457100_1.jpg
```

## Run

```bash
node server.js
```

## VPS firewall

```bash
ufw allow 7891/tcp
ufw allow 7892/udp
```

## Next steps

The `fs.writeFile` call in `server.js` is the integration point. When the Next.js frontend is ready, replace it with a WebSocket broadcast to subscribed browser clients instead of writing to disk.
