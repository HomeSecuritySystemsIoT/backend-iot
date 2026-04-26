const net = require('net');
const dgram = require('dgram');
const fs = require('fs');
const path = require('path');

const TCP_PORT = 7891;
const UDP_PORT = 7892;
const IMAGES_DIR = path.join(__dirname, 'images_received');
const LOGS_DIR = path.join(__dirname, 'logs');

fs.mkdirSync(IMAGES_DIR, { recursive: true });
fs.mkdirSync(LOGS_DIR, { recursive: true });

// ── LOGGER ──────────────────────────────────────────────────────────────────

const tcpLog = fs.createWriteStream(path.join(LOGS_DIR, 'tcp.log'), { flags: 'a' });
const udpLog = fs.createWriteStream(path.join(LOGS_DIR, 'udp.log'), { flags: 'a' });

function timestamp() {
  return new Date().toISOString();
}

function logTcp(msg) {
  const line = `[${timestamp()}] ${msg}\n`;
  process.stdout.write(`[TCP] ${msg}\n`);
  tcpLog.write(line);
}

function logUdp(msg) {
  const line = `[${timestamp()}] ${msg}\n`;
  process.stdout.write(`[UDP] ${msg}\n`);
  udpLog.write(line);
}

// ── UDP SERVER (motion detection + command channel) ─────────────────────────

// deviceIp → { address, port } — populated when ESP32 first sends a UDP packet
const udpEndpoints = new Map();

const udpServer = dgram.createSocket('udp4');

function sendCommand(deviceIp, cmd) {
  const endpoint = udpEndpoints.get(deviceIp);
  if (!endpoint) {
    logUdp(`No endpoint known for ${deviceIp}, cannot send '${cmd}'`);
    return;
  }
  const msg = Buffer.from(cmd);
  udpServer.send(msg, endpoint.port, endpoint.address, (err) => {
    if (err) logUdp(`Failed to send '${cmd}' to ${deviceIp}: ${err.message}`);
    else logUdp(`Sent '${cmd}' → ${deviceIp}:${endpoint.port}`);
  });
}

udpServer.on('message', (msg, rinfo) => {
  if (!udpEndpoints.has(rinfo.address)) {
    logUdp(`New endpoint registered: ${rinfo.address}:${rinfo.port}`);
  }
  udpEndpoints.set(rinfo.address, { address: rinfo.address, port: rinfo.port });

  if (msg.length === 4) {
    const motionDiff = msg.readUInt32BE(0);
    logUdp(`Motion from ${rinfo.address} — diff: ${motionDiff}`);
  }
});

udpServer.on('error', (err) => {
  logUdp(`Server error: ${err.message}`);
});

// ── TCP SERVER (camera frames only) ────────────────────────────────────────

const tcpServer = net.createServer((socket) => {
  const deviceIp = socket.remoteAddress.replace(/^::ffff:/, '');
  const deviceKey = deviceIp.replace(/\./g, '_');
  const deviceDir = path.join(IMAGES_DIR, deviceKey);

  fs.mkdirSync(deviceDir, { recursive: true });

  let stagingBuffer = Buffer.alloc(5 * 1024 * 1024); // 5MB
  let writeIndex = 0;
  let expectedSize = -1;
  let frameCount = 0;

  logTcp(`Connected: ${deviceIp}`);

  // Send 5 'G' commands, one per second
  for (let i = 0; i < 5; i++) {
    setTimeout(() => sendCommand(deviceIp, 'G'), i * 1000);
  }

  socket.on('data', (chunk) => {
    // Grow buffer dynamically if needed
    if (writeIndex + chunk.length > stagingBuffer.length) {
      const grown = Buffer.alloc(stagingBuffer.length * 2);
      stagingBuffer.copy(grown);
      stagingBuffer = grown;
      logTcp(`Buffer expanded for ${deviceIp}`);
    }

    chunk.copy(stagingBuffer, writeIndex);
    writeIndex += chunk.length;

    // Parse 4-byte big-endian size header
    if (expectedSize === -1 && writeIndex >= 4) {
      expectedSize = stagingBuffer.readUInt32BE(0);
    }

    // Full frame received
    if (expectedSize !== -1 && writeIndex >= expectedSize + 4) {
      const jpeg = Buffer.from(stagingBuffer.subarray(4, 4 + expectedSize));
      frameCount++;

      const filename = `${Date.now()}_${frameCount}.jpg`;
      const filepath = path.join(deviceDir, filename);

      fs.writeFile(filepath, jpeg, (err) => {
        if (err) logTcp(`Failed to save frame from ${deviceIp}: ${err.message}`);
        else logTcp(`Frame from ${deviceIp} → ${filename} (${jpeg.length} bytes)`);
      });

      // Shift leftover data to front of buffer
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

  socket.on('close', () => logTcp(`Disconnected: ${deviceIp}`));
  socket.on('error', (err) => logTcp(`Error from ${deviceIp}: ${err.message}`));
});

// ── START ───────────────────────────────────────────────────────────────────

tcpServer.listen(TCP_PORT, () => logTcp(`Listening on :${TCP_PORT}`));
udpServer.bind(UDP_PORT, () => logUdp(`Listening on :${UDP_PORT}`));
