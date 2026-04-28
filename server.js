const net = require('net');
const dgram = require('dgram');
const fs = require('fs');
const path = require('path');
const { WebSocketServer } = require('ws');

const TCP_PORT = 7891;
const UDP_PORT = 7892;
const WS_PORT  = 7890;
const IMAGES_DIR = path.join(__dirname, 'images_received');
const LOGS_DIR   = path.join(__dirname, 'logs');

fs.mkdirSync(IMAGES_DIR, { recursive: true });
fs.mkdirSync(LOGS_DIR,   { recursive: true });

// ── LOGGER ──────────────────────────────────────────────────────────────────

const tcpLog = fs.createWriteStream(path.join(LOGS_DIR, 'tcp.log'), { flags: 'a' });
const udpLog = fs.createWriteStream(path.join(LOGS_DIR, 'udp.log'), { flags: 'a' });

function ts() { return new Date().toISOString(); }

function logTcp(msg) {
  process.stdout.write(`[TCP] ${msg}\n`);
  tcpLog.write(`[${ts()}] ${msg}\n`);
}

function logUdp(msg) {
  process.stdout.write(`[UDP] ${msg}\n`);
  udpLog.write(`[${ts()}] ${msg}\n`);
}

// ── STATE ────────────────────────────────────────────────────────────────────

// deviceId → TCP socket
const tcpConnections = new Map();

// deviceId → { address, port }
const udpEndpoints = new Map();

// deviceId → Set<WebSocket>  (browser clients watching this device)
const browserClients = new Map();

// deviceIds that already had their G sequence started this session
const sessionStarted = new Set();

// deviceId → interval handle for the G command loop
const streamIntervals = new Map();

// ── HELPERS ──────────────────────────────────────────────────────────────────

function sendCommand(deviceId, cmd) {
  const socket = tcpConnections.get(deviceId);
  if (!socket?.writable) {
    logTcp(`No TCP connection for ${deviceId}, cannot send '${cmd}'`);
    return;
  }
  socket.write(Buffer.from(cmd), (err) => {
    if (err) logTcp(`Failed to send '${cmd}' to ${deviceId}: ${err.message}`);
  });
}

function tryStartSession(deviceId) {
  if (sessionStarted.has(deviceId))  return;
  if (!tcpConnections.has(deviceId)) return;

  sessionStarted.add(deviceId);
  logTcp(`Session ready for ${deviceId} — streaming at 1 fps, starting sending command 'G'`);

  const interval = setInterval(() => sendCommand(deviceId, 'G'), 1000);
  streamIntervals.set(deviceId, interval);
}

function broadcastFrame(deviceId, jpeg) {
  const clients = browserClients.get(deviceId);
  if (!clients?.size) return;
  for (const ws of clients) {
    if (ws.readyState === 1) ws.send(jpeg);
  }
}

// ── UDP SERVER ───────────────────────────────────────────────────────────────

const udpServer = dgram.createSocket('udp4');

udpServer.on('message', (msg, rinfo) => {
  const deviceId = rinfo.address;

  if (!udpEndpoints.has(deviceId)) {
    logUdp(`New endpoint registered: ${deviceId}:${rinfo.port}`);
  }
  udpEndpoints.set(deviceId, { address: rinfo.address, port: rinfo.port });

  if (msg.length === 4) {
    const motionDiff = msg.readUInt32BE(0);
    logUdp(`Motion from ${deviceId} — diff: ${motionDiff}`);
  }
});

udpServer.on('error', (err) => logUdp(`Server error: ${err.message}`));

// ── TCP SERVER ───────────────────────────────────────────────────────────────

const tcpServer = net.createServer((socket) => {
  const deviceId = socket.remoteAddress.replace(/^::ffff:/, '');
  const deviceKey = deviceId.replace(/\./g, '_');
  const deviceDir = path.join(IMAGES_DIR, deviceKey);

  fs.mkdirSync(deviceDir, { recursive: true });

  let stagingBuffer = Buffer.alloc(5 * 1024 * 1024);
  let writeIndex = 0;
  let expectedSize = -1;
  let frameCount = 0;

  tcpConnections.set(deviceId, socket);
  logTcp(`Connected: ${deviceId}`);

  // TCP came in — check if UDP is already up and session hasn't started yet
  tryStartSession(deviceId);

  socket.on('data', (chunk) => {
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
      frameCount++;

      const filename = `photo_sent.jpg`;
      fs.writeFile(path.join(deviceDir, filename), jpeg, (err) => {
        if (err) logTcp(`Failed to save frame from ${deviceId}: ${err.message}`);
        else     logTcp(`Frame from ${deviceId} → ${filename} (${jpeg.length} bytes)`);
      });

      broadcastFrame(deviceId, jpeg);

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
    logTcp(`Disconnected: ${deviceId}`);
    tcpConnections.delete(deviceId);
    sessionStarted.delete(deviceId);
    clearInterval(streamIntervals.get(deviceId));
    streamIntervals.delete(deviceId);
  });

  socket.on('error', (err) => logTcp(`Error from ${deviceId}: ${err.message}`));
});

// ── WEBSOCKET SERVER (browser clients) ──────────────────────────────────────

const wss = new WebSocketServer({ port: WS_PORT });

wss.on('connection', (ws, req) => {
  const url = new URL(req.url, 'http://localhost');
  const deviceId = url.searchParams.get('device');

  if (!deviceId) {
    ws.close(1008, 'Missing ?device= param');
    return;
  }

  if (!browserClients.has(deviceId)) browserClients.set(deviceId, new Set());
  browserClients.get(deviceId).add(ws);

  ws.on('close', () => browserClients.get(deviceId)?.delete(ws));
});

// ── START ────────────────────────────────────────────────────────────────────

tcpServer.listen(TCP_PORT, () => logTcp(`Listening on :${TCP_PORT}`));
udpServer.bind(UDP_PORT,   () => logUdp(`Listening on :${UDP_PORT}`));
wss.on('listening',        () => process.stdout.write(`[WS]  Listening on :${WS_PORT}\n`));
