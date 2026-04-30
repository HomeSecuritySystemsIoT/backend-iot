const sharp = require('sharp');

// Fraction of pixels that must change to fire a motion event (0 → 1).
const MOTION_THRESHOLD = 0.35;

// Per-pixel grayscale brightness delta (0-255) to count a pixel as "changed".
const PIXEL_DIFF = 25;

// ── STATE ────────────────────────────────────────────────────────────────────

// deviceId → Set<ServerResponse>  (active SSE connections)
const sseClients = new Map();

// deviceId → Buffer  (previous JPEG frame for diffing)
const prevFrames = new Map();

// ── SSE CLIENT MANAGEMENT ────────────────────────────────────────────────────

// Returns how many SSE clients are subscribed to a device.
function watcherCount(deviceId) {
  return sseClients.get(deviceId)?.size ?? 0;
}

// Registers an SSE response object. Caller must have already sent the headers.
function addClient(deviceId, res) {
  if (!sseClients.has(deviceId)) sseClients.set(deviceId, new Set());
  sseClients.get(deviceId).add(res);
}

// Unregisters an SSE response object (call on request 'close').
function removeClient(deviceId, res) {
  sseClients.get(deviceId)?.delete(res);
}

// ── MOTION DETECTION ─────────────────────────────────────────────────────────

// Decodes two JPEG buffers to grayscale and returns the fraction of pixels
// that changed by more than PIXEL_DIFF (0 → 1).
async function computeChangedPercent(prevBuf, currBuf) {
  const [prev, curr] = await Promise.all([
    sharp(prevBuf).grayscale().raw().toBuffer({ resolveWithObject: true }),
    sharp(currBuf).grayscale().raw().toBuffer({ resolveWithObject: true }),
  ]);

  if (prev.info.width !== curr.info.width || prev.info.height !== curr.info.height) return 0;

  const total = prev.info.width * prev.info.height;
  let changed = 0;
  for (let i = 0; i < total; i++) {
    if (Math.abs(prev.data[i] - curr.data[i]) > PIXEL_DIFF) changed++;
  }
  return changed / total;
}

function broadcastMotionEvent(deviceId, changedPercent) {
  const clients = sseClients.get(deviceId);
  if (!clients?.size) return;

  const payload = JSON.stringify({
    deviceId,
    changedPercent: Math.round(changedPercent * 1000) / 1000,
    ts:             new Date().toISOString(),
  });

  for (const res of clients) {
    res.write(`data: ${payload}\n\n`);
  }
}

// Call this on every incoming JPEG frame.
// Returns the changedPercent if motion was detected, or null otherwise.
// Throws if sharp fails to decode a frame — caller should catch and log.
async function onFrame(deviceId, jpeg) {
  const prev = prevFrames.get(deviceId);
  prevFrames.set(deviceId, jpeg);
  if (!prev) return null;

  const changed = await computeChangedPercent(prev, jpeg);
  if (changed < MOTION_THRESHOLD) return null;

  broadcastMotionEvent(deviceId, changed);
  return changed;
}

// Call when a device disconnects to free the stored previous frame.
function onDeviceDisconnect(deviceId) {
  prevFrames.delete(deviceId);
}

module.exports = { watcherCount, addClient, removeClient, onFrame, onDeviceDisconnect };
