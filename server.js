const net  = require('net');
const fs   = require('fs');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const motion = require('./motion-detection');

const TCP_PORT            = 7891;
const WS_PORT             = 7890;
const LOGS_DIR            = path.join(__dirname, 'logs');
const IDENTIFY_TIMEOUT_MS = 2000;

fs.mkdirSync(LOGS_DIR, { recursive: true });

// ── LOGGER ──────────────────────────────────────────────────────────────────

const tcpLog = fs.createWriteStream(path.join(LOGS_DIR, 'tcp.log'), { flags: 'a' });

function ts() { return new Date().toISOString(); }

function logTcp(msg) {
  process.stdout.write(`[TCP] ${msg}\n`);
  tcpLog.write(`[${ts()}] ${msg}\n`);
}

// ── STATE ────────────────────────────────────────────────────────────────────

const tcpConnections  = new Map();  // deviceId → TCP socket
const browserClients  = new Map();  // deviceId → Set<WebSocket>  (video feed)
const streamIntervals = new Map();  // deviceId → interval handle

// ── HELPERS ──────────────────────────────────────────────────────────────────

function sendCommand(deviceId, cmd) {
  const socket = tcpConnections.get(deviceId);
  if (!socket?.writable) return;
  socket.write(Buffer.from(cmd), (err) => {
    if (err) logTcp(`Failed to send '${cmd}' to ${deviceId}: ${err.message}`);
  });
}

function videoWatcherCount(deviceId) {
  return browserClients.get(deviceId)?.size ?? 0;
}

// Switches between G-stream (any subscriber present) and P-keepalive (none).
// Both video watchers and motion SSE subscribers trigger streaming.
function updateDeviceMode(deviceId) {
  if (!tcpConnections.has(deviceId)) return;

  clearInterval(streamIntervals.get(deviceId));

  if (videoWatcherCount(deviceId) + motion.watcherCount(deviceId) > 0) {
    logTcp(`${deviceId} — client(s) active, streaming at 1 fps`);
    streamIntervals.set(deviceId, setInterval(() => sendCommand(deviceId, 'G'), 1000));
  } else {
    logTcp(`${deviceId} — no clients, sending keepalive 'P'`);
    streamIntervals.set(deviceId, setInterval(() => sendCommand(deviceId, 'P'), 5000));
  }
}

function broadcastFrame(deviceId, jpeg) {
  const clients = browserClients.get(deviceId);
  if (!clients?.size) return;
  for (const ws of clients) {
    if (ws.readyState === 1) ws.send(jpeg);
  }
}

// ── DEVICE IDENTIFICATION ────────────────────────────────────────────────────
//
// Protocol (to be implemented on the ESP32 side):
//   Server sends: 'I'
//   ESP32 replies: "<unique-id>\n"  (e.g. MAC address "AA:BB:CC:DD:EE:FF\n")
//
// Until the ESP32 implements this, the server falls back to the remote IP
// after IDENTIFY_TIMEOUT_MS milliseconds.

function requestDeviceId(socket) {
  socket.write(Buffer.from('I'), (err) => {
    if (err) logTcp(`Failed to send identify request to ${socket.remoteAddress}`);
  });
}

function parseDeviceId(chunk, newlineIndex) {
  const id = chunk.subarray(0, newlineIndex).toString('utf8').trim();
  return id.length > 0 ? id : null;
}

// ── TCP SERVER ───────────────────────────────────────────────────────────────

const tcpServer = net.createServer((socket) => {
  const remoteIp = socket.remoteAddress.replace(/^::ffff:/, '');
  let deviceId   = remoteIp;
  let phase      = 'identify'; // 'identify' | 'stream'

  let stagingBuffer = Buffer.alloc(5 * 1024 * 1024);
  let writeIndex    = 0;
  let expectedSize  = -1;

  function finalizeDevice(resolvedId) {
    if (phase !== 'identify') return;
    clearTimeout(identifyTimeout);
    phase = 'stream';

    if (resolvedId !== remoteIp) logTcp(`Device ${remoteIp} identified as: ${resolvedId}`);
    deviceId = resolvedId;

    tcpConnections.set(deviceId, socket);
    logTcp(`Connected: ${deviceId}`);
    updateDeviceMode(deviceId);
  }

  requestDeviceId(socket);
  let identifyTimeout = setTimeout(() => finalizeDevice(remoteIp), IDENTIFY_TIMEOUT_MS);

  socket.on('data', (chunk) => {
    if (phase === 'identify') {
      const nl = chunk.indexOf(0x0A);
      if (nl !== -1) {
        finalizeDevice(parseDeviceId(chunk, nl) ?? remoteIp);
        const remainder = chunk.subarray(nl + 1);
        if (remainder.length > 0) socket.emit('data', remainder);
      }
      return;
    }

    if (writeIndex + chunk.length > stagingBuffer.length) {
      const grown = Buffer.alloc(stagingBuffer.length * 2);
      stagingBuffer.copy(grown);
      stagingBuffer = grown;
      logTcp(`Buffer expanded for ${deviceId}`);
    }

    chunk.copy(stagingBuffer, writeIndex);
    writeIndex += chunk.length;

    if (expectedSize === -1 && writeIndex >= 4) {
      expectedSize = stagingBuffer.readUInt32LE(0);
    }

    if (expectedSize !== -1 && writeIndex >= expectedSize + 4) {
      const jpeg = Buffer.from(stagingBuffer.subarray(4, 4 + expectedSize));

      broadcastFrame(deviceId, jpeg);
      motion.onFrame(deviceId, jpeg)
        .then(changed => { if (changed) logTcp(`Motion on ${deviceId}: ${(changed * 100).toFixed(1)}%`); })
        .catch(err   => logTcp(`Motion check error for ${deviceId}: ${err.message}`));

      const leftover = writeIndex - (4 + expectedSize);
      if (leftover > 0) {
        stagingBuffer.copy(stagingBuffer, 0, 4 + expectedSize, writeIndex);
        writeIndex = leftover;
      } else {
        writeIndex = 0;
      }
      expectedSize = -1;
    }
  });

  socket.on('close', () => {
    clearTimeout(identifyTimeout);
    if (tcpConnections.get(deviceId) === socket) {
      logTcp(`Disconnected: ${deviceId}`);
      tcpConnections.delete(deviceId);
      motion.onDeviceDisconnect(deviceId);
      clearInterval(streamIntervals.get(deviceId));
      streamIntervals.delete(deviceId);
    }
  });

  socket.on('error', (err) => logTcp(`Error from ${deviceId}: ${err.message}`));
});

// ── HTTP + SSE SERVER (shares port with WebSocket) ───────────────────────────
//
// GET /motion?device=<deviceId>  →  SSE stream of motion events
//   event payload: { deviceId, changedPercent, ts }
//
// WebSocket upgrades on the same port are handled by wss below.

const httpServer = http.createServer((req, res) => {
  const url = new URL(req.url, 'http://localhost');

  // GET /devices — localhost-only, returns connected device IDs
  if (url.pathname === '/devices') {
    const remoteIp = req.socket.remoteAddress?.replace(/^::ffff:/, '') ?? '';
    if (remoteIp !== '127.0.0.1' && remoteIp !== '::1') {
      res.writeHead(403);
      res.end('Forbidden');
      return;
    }
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ devices: [...tcpConnections.keys()] }));
    return;
  }

  if (url.pathname !== '/motion') {
    res.writeHead(404);
    res.end();
    return;
  }

  const deviceId = url.searchParams.get('device');
  if (!deviceId) {
    res.writeHead(400);
    res.end('Missing ?device= param');
    return;
  }

  res.writeHead(200, {
    'Content-Type':                'text/event-stream',
    'Cache-Control':               'no-cache',
    'Connection':                  'keep-alive',
    'Access-Control-Allow-Origin': '*',
  });
  res.write('\n'); // flush headers immediately so the browser opens the stream

  motion.addClient(deviceId, res);
  logTcp(`Motion SSE client connected for ${deviceId} (${motion.watcherCount(deviceId)} subscribed)`);

  updateDeviceMode(deviceId);

  req.on('close', () => {
    motion.removeClient(deviceId, res);
    logTcp(`Motion SSE client disconnected for ${deviceId} (${motion.watcherCount(deviceId)} subscribed)`);
    updateDeviceMode(deviceId);
  });
});

const wss = new WebSocketServer({ server: httpServer });

wss.on('connection', (ws, req) => {
  const url      = new URL(req.url, 'http://localhost');
  const deviceId = url.searchParams.get('device');

  if (!deviceId) {
    ws.close(1008, 'Missing ?device= param');
    return;
  }

  if (!browserClients.has(deviceId)) browserClients.set(deviceId, new Set());
  browserClients.get(deviceId).add(ws);
  logTcp(`WS client connected for ${deviceId} (${browserClients.get(deviceId).size} watching)`);

  updateDeviceMode(deviceId);

  ws.on('close', () => {
    browserClients.get(deviceId)?.delete(ws);
    const remaining = browserClients.get(deviceId)?.size ?? 0;
    logTcp(`WS client disconnected for ${deviceId} (${remaining} watching)`);
    updateDeviceMode(deviceId);
  });
});

// ── START ────────────────────────────────────────────────────────────────────

tcpServer.listen(TCP_PORT, () => logTcp(`Listening on :${TCP_PORT}`));
httpServer.listen(WS_PORT, () => process.stdout.write(`[WS/SSE] Listening on :${WS_PORT}\n`));
