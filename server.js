const net = require('net');
const tls = require('tls');
const fs = require('fs');
const http = require('http');
const path = require('path');
const { WebSocketServer } = require('ws');
const motion = require('./motion-detection');

const TCP_PORT = 7891;
const TCP_TLS_PORT = 7893;
const WS_PORT = 7890;
const LOGS_DIR = path.join(__dirname, 'logs');
const ONE_SECOND_MS = 1000;
const IDENTIFY_TIMEOUT_MS = 2 * ONE_SECOND_MS;
const DEVICE_TIMEOUT_MS = 15 * ONE_SECOND_MS; // destroy socket after 15 s with no data from device

const TLS_KEY_PATH = process.env.TLS_KEY_PATH || path.join(__dirname, 'certs', 'server.key');
const TLS_CERT_PATH = process.env.TLS_CERT_PATH || path.join(__dirname, 'certs', 'server.crt');
const TLS_CA_PATH = process.env.TLS_CA_PATH || path.join(__dirname, 'certs', 'ca.crt');

fs.mkdirSync(LOGS_DIR, { recursive: true });

// ── LOGGER ──────────────────────────────────────────────────────────────────

const tcpLog = fs.createWriteStream(path.join(LOGS_DIR, 'tcp.log'), { flags: 'a' });

function ts() { return new Date().toISOString(); }

function logTcp(msg) {
  process.stdout.write(`[TCP] ${msg}\n`);
  // tcpLog.write(`[${ts()}] ${msg}\n`);
}

// ── STATE ────────────────────────────────────────────────────────────────────

const tcpConnections = new Map();  // deviceId → TCP socket
const browserClients = new Map();  // deviceId → Set<WebSocket>  (video feed)
const streamIntervals = new Map();  // deviceId → interval handle
const deviceWatchdogs = new Map();  // deviceId → watchdog interval handle
const deviceLastSeen = new Map();  // deviceId → Date.now() of last received byte

// ── HELPERS ──────────────────────────────────────────────────────────────────

function sendCommand(deviceId, cmd) {
  const socket = tcpConnections.get(deviceId);
  if (!socket?.writable) return;
  socket.write(Buffer.from(cmd), (err) => {
    if (err) {
      logTcp(`Failed to send '${cmd}' to ${deviceId}: ${err.message} — destroying socket`);
      socket.destroy();
    }
  });
}

function videoWatcherCount(deviceId) {
  return browserClients.get(deviceId)?.size ?? 0;
}

// Switches between G-stream (any subscriber present) and P-keepalive (none).
// Both video watchers and motion SSE subscribers trigger streaming.
function updateDeviceMode(deviceId) {
  const hasTcp = tcpConnections.has(deviceId);
  logTcp(`updateDeviceMode(${deviceId}) — tcp=${hasTcp} videoWatchers=${videoWatcherCount(deviceId)} motionWatchers=${motion.watcherCount(deviceId)}`);
  if (!hasTcp) return;

  clearInterval(streamIntervals.get(deviceId));

  if (videoWatcherCount(deviceId) + motion.watcherCount(deviceId) > 0) {
    logTcp(`${deviceId} — client(s) active, streaming at 1 fps`);

    // 2 fps
    streamIntervals.set(deviceId, setInterval(() => {
      logTcp(`${deviceId} — sending G`);
      sendCommand(deviceId, 'G');
    }, ONE_SECOND_MS / 2));

  } else {
    logTcp(`${deviceId} — no clients, sending keepalive 'P'`);
    streamIntervals.set(deviceId, setInterval(() => sendCommand(deviceId, 'P'), 5 * ONE_SECOND_MS));
  }
}

function broadcastFrame(deviceId, jpeg) {
  const clients = browserClients.get(deviceId);
  logTcp(`broadcastFrame(${deviceId}) — ${jpeg.length} bytes — ${clients?.size ?? 0} ws client(s)`);
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

// ── DEVICE CONNECTION HANDLER (shared by plain TCP and TLS TCP) ──────────────

function handleDeviceConnection(socket) {
  socket.setKeepAlive(true, 5 * ONE_SECOND_MS); // detect dead connections after ~5 s of silence
  const remoteIp = socket.remoteAddress.replace(/^::ffff:/, '');
  let deviceId = remoteIp;
  let phase = 'identify'; // 'identify' | 'stream'

  let stagingBuffer = Buffer.alloc(5 * 1024 * 1024);
  let writeIndex = 0;
  let expectedSize = -1;

  function finalizeDevice(resolvedId) {
    if (phase !== 'identify') return;
    clearTimeout(identifyTimeout);
    phase = 'stream';

    if (resolvedId !== remoteIp) logTcp(`Device ${remoteIp} identified as: ${resolvedId}`);
    deviceId = resolvedId;

    tcpConnections.set(deviceId, socket);
    deviceLastSeen.set(deviceId, Date.now());

    const watchdog = setInterval(() => {
      if (Date.now() - deviceLastSeen.get(deviceId) > DEVICE_TIMEOUT_MS) {
        logTcp(`${deviceId} — no data for ${DEVICE_TIMEOUT_MS / 1000}s, assuming disconnected`);
        socket.destroy();
      }
    }, 5 * ONE_SECOND_MS);
    deviceWatchdogs.set(deviceId, watchdog);

    logTcp(`Connected: ${deviceId}`);
    updateDeviceMode(deviceId);
  }

  requestDeviceId(socket);
  let identifyTimeout = setTimeout(() => finalizeDevice(remoteIp), IDENTIFY_TIMEOUT_MS);

  socket.on('data', (chunk) => {
    deviceLastSeen.set(deviceId, Date.now());

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
        .catch(err => logTcp(`Motion check error for ${deviceId}: ${err.message}`));

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
    clearInterval(deviceWatchdogs.get(deviceId));
    deviceWatchdogs.delete(deviceId);
    deviceLastSeen.delete(deviceId);
    if (tcpConnections.get(deviceId) === socket) {
      logTcp(`Disconnected: ${deviceId}`);
      tcpConnections.delete(deviceId);
      motion.onDeviceDisconnect(deviceId);
      clearInterval(streamIntervals.get(deviceId));
      streamIntervals.delete(deviceId);
    }
  });

  socket.on('error', (err) => logTcp(`Error from ${deviceId}: ${err.message}`));
}

// ── TCP SERVER ───────────────────────────────────────────────────────────────

const tcpServer = net.createServer(handleDeviceConnection);

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
    // if (remoteIp !== '127.0.0.1' && remoteIp !== '::1') {
    //   res.writeHead(403);
    //   res.end('Forbidden');
    //   return;
    // }
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
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache',
    'Connection': 'keep-alive',
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
  const url = new URL(req.url, 'http://localhost');
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

// ── TLS TCP SERVER ───────────────────────────────────────────────────────────
//
// Mirrors the plain TCP server on port 7891 but with TLS encryption.
// Place your certificate at certs/server.crt and key at certs/server.key,
// or override via TLS_KEY_PATH / TLS_CERT_PATH environment variables.
// If the files are missing the TLS server is skipped; the plain server on
// port 7891 remains fully operational.

function loadTlsOptions() {
  try {
    return {
      key: fs.readFileSync(TLS_KEY_PATH),
      cert: fs.readFileSync(TLS_CERT_PATH),
      ca: fs.readFileSync(TLS_CA_PATH),
      requestCert: true,  // ask the ESP32 for its client certificate
      rejectUnauthorized: true,  // reject any device not signed by our CA
    };
  } catch {
    return null;
  }
}

const tlsOptions = loadTlsOptions();

if (tlsOptions) {
  const tlsTcpServer = tls.createServer(tlsOptions, handleDeviceConnection);
  tlsTcpServer.listen(TCP_TLS_PORT, () => logTcp(`TLS listening on :${TCP_TLS_PORT}`));
} else {
  process.stdout.write(`[TLS] Certs not found at ${TLS_KEY_PATH} / ${TLS_CERT_PATH} — TLS server disabled\n`);
}

// ── START ────────────────────────────────────────────────────────────────────

tcpServer.listen(TCP_PORT, () => logTcp(`Listening on :${TCP_PORT}`));
httpServer.listen(WS_PORT, () => process.stdout.write(`[WS/SSE] Listening on :${WS_PORT}\n`));
