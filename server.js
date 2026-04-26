const net = require('net');
const dgram = require('dgram');
const fs = require('fs');
const path = require('path');

const TCP_PORT = 7891;
const UDP_PORT = 7892;
const IMAGES_DIR = path.join(__dirname, 'images_received');

fs.mkdirSync(IMAGES_DIR, { recursive: true });

// deviceIp → { address, port } — populated when ESP32 first sends a UDP packet
const udpEndpoints = new Map();

const udpServer = dgram.createSocket('udp4');

function sendCommand(deviceIp, cmd) {
  const endpoint = udpEndpoints.get(deviceIp);
  if (!endpoint) {
    console.warn(`[UDP] No endpoint known for ${deviceIp}, cannot send '${cmd}'`);
    return;
  }
  const msg = Buffer.from(cmd);
  udpServer.send(msg, endpoint.port, endpoint.address, (err) => {
    if (err) console.error(`[UDP] Failed to send '${cmd}' to ${deviceIp}:`, err.message);
    else console.log(`[UDP] Sent '${cmd}' → ${deviceIp}:${endpoint.port}`);
  });
}

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

  console.log(`[TCP] ESP32 connected: ${deviceIp}`);

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
        if (err) console.error(`[TCP] Failed to save frame from ${deviceIp}:`, err.message);
        else console.log(`[TCP] ${deviceIp} → ${filename} (${jpeg.length} bytes)`);
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

  socket.on('close', () => console.log(`[TCP] ESP32 disconnected: ${deviceIp}`));
  socket.on('error', (err) => console.error(`[TCP] Error from ${deviceIp}:`, err.message));
});

// ── UDP SERVER (motion detection + command channel) ─────────────────────────

udpServer.on('message', (msg, rinfo) => {
  // Register or refresh the ESP32's UDP endpoint
  if (!udpEndpoints.has(rinfo.address)) {
    console.log(`[UDP] Registered endpoint: ${rinfo.address}:${rinfo.port}`);
  }
  udpEndpoints.set(rinfo.address, { address: rinfo.address, port: rinfo.port });

  if (msg.length === 4) {
    const motionDiff = msg.readUInt32BE(0);
    console.log(`[UDP] Motion from ${rinfo.address} — diff: ${motionDiff}`);
  }
});

udpServer.on('error', (err) => {
  console.error('[UDP] Server error:', err.message);
});

// ── START ───────────────────────────────────────────────────────────────────

tcpServer.listen(TCP_PORT, () => console.log(`[TCP] Listening on :${TCP_PORT}`));
udpServer.bind(UDP_PORT, () => console.log(`[UDP] Listening on :${UDP_PORT}`));
