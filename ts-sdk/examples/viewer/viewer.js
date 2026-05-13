// quicrtc · browser viewer — kind-agnostic.
//
// Connects to any quicrtc server via a pasted share URL, inspects the
// announced remote tracks, and renders one panel per track based on
// its Kind:
//
//   • Video      → canvas K/P debug overlay (last frame size + age)
//   • Tokens     → live-appending text panel
//   • ToolCalls  → JSON event list, newest at the top
//   • Telemetry  → key/value gauges parsed from JSON-shaped datagrams
//
// In addition, telemetry-via-datagram is consumed from the
// WebTransport datagrams pipe regardless of whether a Telemetry track
// was announced — many servers (including agent_pubsub) ship telemetry
// as raw datagrams with a 4-byte trackId envelope, not as a track.
//
// If a "user_actions" track is in the remote announce, this viewer
// publishes a click event back when the developer clicks the screen
// canvas — demonstrating end-to-end bidirectional traffic.
//
// Backpressure: when the SDK fires onBackpressure on the screen track
// at level≥2, this viewer drops further screen AUs until level returns
// to 0. The dropped count is rendered in the screen panel header so
// the behavior is visible.

import {
  ConnectionState,
  QuicRTCClient,
  TrackKind,
  decodeDatagram,
} from '../../dist/index.js';

// ===== DOM refs =====
const els = {
  statePill: document.getElementById('state-pill'),
  sid: document.getElementById('sid'),
  disconnect: document.getElementById('disconnect'),
  connectCard: document.getElementById('connect-card'),
  connectForm: document.getElementById('connect-form'),
  shareInput: document.getElementById('share-url'),
  connectError: document.getElementById('connect-error'),
  overview: document.getElementById('overview'),
  trackChips: document.getElementById('track-chips'),
  sdpLine: document.getElementById('sdp'),
  bpBar: document.getElementById('bp-bar'),
  bpAction: document.getElementById('bp-action'),
  panels: document.getElementById('panels'),
  dcCard: document.getElementById('dc-card'),
  dcTranscript: document.getElementById('dc-transcript'),
  dcForm: document.getElementById('dc-form'),
  dcInput: document.getElementById('dc-input'),
};

// Pre-fill from ?share= so deep links work.
const initialShare = new URL(location.href).searchParams.get('share');
if (initialShare) els.shareInput.value = initialShare;

// ===== State =====
let client = null;
let panelByTrack = new Map();   // trackName -> { el, body, count, kind, state }
let screenState = null;         // active screen panel state (for backpressure drops)
let userActionsSender = null;   // for publish-back on canvas click
let bpLevel = 0;
const counters = new Map();     // trackName -> count (AUs)
let datagramCount = 0;

// ===== Connect flow =====
els.connectForm.addEventListener('submit', async (e) => {
  e.preventDefault();
  els.connectError.hidden = true;
  await connect(els.shareInput.value.trim());
});

els.disconnect.addEventListener('click', async () => {
  if (client) { try { await client.close(); } catch {} }
});

async function connect(shareUrl) {
  let url, slug, certHash;
  try {
    ({ url, slug, certHash } = parseShareUrl(shareUrl));
  } catch (err) {
    showConnectError(err.message);
    return;
  }

  client = new QuicRTCClient({ maxQueueSize: 200, reconnect: false });
  client.onStateChange((s) => renderState(s));
  client.onBackpressure((track, level) => onBackpressure(track, level));

  try {
    await client.connect(url, { slug, certHash });
  } catch (err) {
    showConnectError('connect failed: ' + (err?.message || err));
    client = null;
    return;
  }

  els.sid.textContent = client.sessionId() ? `sid=${client.sessionId().slice(0, 12)}` : '';
  els.connectCard.hidden = true;
  els.overview.hidden = false;
  els.panels.hidden = false;
  els.disconnect.hidden = false;

  const sdp = client.getSDP();
  if (sdp) els.sdpLine.textContent =
    `sdp: codec=${sdp.codec} ${sdp.width}×${sdp.height}@${sdp.fps}`;

  // Spawn panels as tracks announce. The first call may return [];
  // we re-check every 200ms for 5s in case announces arrive late.
  refreshTracks();
  const interval = setInterval(refreshTracks, 200);
  setTimeout(() => clearInterval(interval), 5000);

  // Start the datagram drain loop (kind-agnostic — relies on the
  // 4-byte envelope from wire/decodeDatagram).
  drainDatagrams();

  // DataChannel (if present)
  const dc = client.getDataChannel();
  if (dc) {
    els.dcCard.hidden = false;
    dc.onMessage((data) => appendDC('in', new TextDecoder().decode(data)));
    els.dcForm.addEventListener('submit', async (e) => {
      e.preventDefault();
      const text = els.dcInput.value;
      if (!text) return;
      try {
        await dc.sendText(text);
        appendDC('out', text);
        els.dcInput.value = '';
      } catch (err) {
        appendDC('in', '[send error: ' + err.message + ']');
      }
    });
  }

  // Periodic stats so a developer can confirm bytes are flowing.
  const statsTimer = setInterval(() => {
    if (!client) return;
    const s = client.getDetailedStats();
    // Stick stats into the SDP line subtly.
    if (sdp) {
      els.sdpLine.textContent =
        `sdp: codec=${sdp.codec} ${sdp.width}×${sdp.height}@${sdp.fps}` +
        `  •  in=${humanBytes(s.bytesIn)}  out=${humanBytes(s.bytesOut)}` +
        `  AU=${s.auReceived}  dg=${datagramCount}`;
    }
  }, 1000);

  // Stop stats + cleanup on disconnect.
  client.onStateChange((s) => {
    if (s === ConnectionState.Disconnected) {
      clearInterval(statsTimer);
    }
  });
}

function parseShareUrl(raw) {
  if (!raw) throw new Error('paste a share URL');
  let u;
  try { u = new URL(raw); } catch { throw new Error('not a valid URL'); }
  if (u.protocol !== 'https:') throw new Error('URL must be https://');
  // Fragment: slug=...&hash=...
  const frag = new URLSearchParams(u.hash.replace(/^#/, ''));
  const slug = frag.get('slug') || '';
  const certHash = frag.get('hash') || '';
  if (!slug) throw new Error('URL fragment missing ?slug=');
  if (!certHash) throw new Error('URL fragment missing ?hash= (cert pin)');
  // Strip fragment before passing to the SDK.
  const url = u.origin + u.pathname + u.search;
  return { url, slug, certHash };
}

function showConnectError(msg) {
  els.connectError.textContent = msg;
  els.connectError.hidden = false;
}

// ===== Tracks & panels =====
function refreshTracks() {
  if (!client) return;
  const tracks = client.getRemoteTracks();
  for (const t of tracks) {
    if (panelByTrack.has(t.name)) continue;
    addPanel(t);
  }
  // Update chips
  renderChips(tracks);
}

function renderChips(tracks) {
  // Build chips for every panel (so chips show even after the track
  // announces stop arriving).
  const lines = [];
  for (const [name, p] of panelByTrack) {
    lines.push(`<span class="chip"><strong>${escapeHtml(name)}</strong>${p.kind || '?'} · ${counters.get(name) || 0}</span>`);
  }
  lines.push(`<span class="chip"><strong>datagrams</strong>${datagramCount}</span>`);
  els.trackChips.innerHTML = lines.join('');
}

function addPanel(track) {
  const panel = document.createElement('section');
  panel.className = 'panel';
  panel.dataset.track = track.name;

  const head = document.createElement('header');
  head.innerHTML =
    `<span class="name">${escapeHtml(track.name)}</span>` +
    `<span class="kind">${track.kind}${track.codec ? ' · ' + escapeHtml(track.codec) : ''}</span>` +
    `<span class="count">0</span>`;
  panel.appendChild(head);

  const body = document.createElement('div');
  body.className = 'body';
  panel.appendChild(body);

  const state = { el: panel, body, head, kind: track.kind, drops: 0 };
  els.panels.appendChild(panel);
  panelByTrack.set(track.name, state);
  counters.set(track.name, 0);

  switch (track.kind) {
    case TrackKind.Video: mountVideo(track.name, state); break;
    case TrackKind.Tokens: mountTokens(track.name, state); break;
    case TrackKind.ToolCalls: mountToolCalls(track.name, state); break;
    case TrackKind.Telemetry: mountTelemetry(track.name, state); break;
    default: mountTokens(track.name, state); // sensible default for unknown kinds
  }

  // If the server announced a `user_actions` track inbound to it,
  // we publish back; but the server doesn't announce its inbound
  // expectations. We unconditionally create a user_actions sender
  // when a video screen panel mounts (the typical CUA pattern).
  if (track.kind === TrackKind.Video && !userActionsSender) {
    client.addTrack({ name: 'user_actions', kind: TrackKind.ToolCalls })
      .then((s) => { userActionsSender = s; })
      .catch(() => { /* server may not accept publish-back; that's OK */ });
  }
}

function bumpCount(trackName) {
  const c = (counters.get(trackName) || 0) + 1;
  counters.set(trackName, c);
  const p = panelByTrack.get(trackName);
  if (p) p.head.querySelector('.count').textContent = c;
}

// ===== Panel: Video =====
function mountVideo(name, state) {
  const canvas = document.createElement('canvas');
  canvas.className = 'screen-canvas';
  canvas.width = 600; canvas.height = 320;
  const ctx = canvas.getContext('2d');

  const meta = document.createElement('div');
  meta.className = 'screen-meta';
  meta.innerHTML =
    `<span class="last">no frames yet</span>` +
    `<span class="age"></span>` +
    `<span class="kp"></span>` +
    `<span class="drop" hidden></span>` +
    `<label class="slow"><input type="checkbox" id="slow-consumer-${name}"> simulate slow render (50ms)</label>`;
  state.body.appendChild(canvas);
  state.body.appendChild(meta);

  // Slow-consumer toggle. When ticked, the onTrack handler awaits
  // a 50 ms sleep before processing each frame. Combined with a
  // publisher that pushes faster than 20 Hz this saturates the
  // SDK's per-track receive queue, which is the condition under
  // which the SDK fires onBackpressure inward (the BP indicator
  // up top lights up).
  const slowCb = meta.querySelector(`#slow-consumer-${name}`);

  canvas.addEventListener('click', async (ev) => {
    if (!userActionsSender) return;
    const rect = canvas.getBoundingClientRect();
    const x = Math.round(ev.clientX - rect.left);
    const y = Math.round(ev.clientY - rect.top);
    const payload = new TextEncoder().encode(JSON.stringify({
      kind: 'click', x, y, t: Date.now(),
    }));
    try {
      await userActionsSender.send({
        bytes: payload, keyframe: true,
        ptsMicro: BigInt(Date.now()) * 1000n,
        seq: counters.get('user_actions') || 0,
        discontinuity: false,
      });
      counters.set('user_actions', (counters.get('user_actions') || 0) + 1);
    } catch { /* connection went away */ }
  });

  state.screen = { canvas, ctx, meta, lastSeq: -1, lastAt: 0, slowCb };
  screenState = state;

  // Push-mode subscription — we treat each AU as one frame. The AU
  // bytes are real image/png per agent_pubsub/server. We decode
  // with createImageBitmap so the canvas paints actual pixels (not
  // a debug overlay) and the user sees the screen update in real
  // time, just as a CUA agent's host would observe its browser.
  client.onTrack(name, async (au) => {
    if (slowCb.checked) {
      // Block the receive loop so the SDK's per-track queue fills up.
      // Honest backpressure trigger — no fake activity, the queue
      // saturates because we're not draining fast enough.
      await new Promise((r) => setTimeout(r, 50));
    }
    if (bpLevel >= 2) {
      state.drops++;
      meta.querySelector('.drop').hidden = false;
      meta.querySelector('.drop').textContent = `dropped: ${state.drops}`;
      return;
    }
    // Detect gap and request keyframe
    if (state.screen.lastSeq >= 0 && au.seq > state.screen.lastSeq + 1 && !au.keyframe) {
      client.requestKeyframe(name).catch(() => {});
    }
    state.screen.lastSeq = au.seq;
    state.screen.lastAt = performance.now();
    await drawFrame(state.screen, au);
    bumpCount(name);
  });

  // Tick to update "age" display.
  setInterval(() => {
    if (state.screen.lastAt === 0) return;
    const age = Math.round(performance.now() - state.screen.lastAt);
    meta.querySelector('.age').textContent = `age: ${age}ms`;
  }, 200);
}

// drawFrame renders the AU's PNG bytes onto the canvas. The agent_
// pubsub / cua servers ship real PNG (Codec="image/png" in SDP and
// in the bytes), so we hand them to the platform's decoder and
// drawImage them. A small K/P overlay text + size is overlaid in a
// corner so the developer can still read framing metadata at a
// glance.
async function drawFrame(s, au) {
  const { ctx, canvas } = s;
  const W = canvas.width, H = canvas.height;

  // Decode the PNG. createImageBitmap is widely available and
  // does the decode off the main thread on modern browsers; if it
  // fails (e.g. server sent something that isn't a PNG), fall
  // back to the K/P-only debug overlay so we don't break the page.
  let bitmap = null;
  try {
    const blob = new Blob([au.bytes], { type: 'image/png' });
    bitmap = await createImageBitmap(blob);
  } catch {
    bitmap = null;
  }

  if (bitmap) {
    // Paint the real pixels, scaled to fit the canvas while keeping
    // the source aspect ratio (otherwise a 480×320 source looks
    // stretched on a 600×320 canvas).
    const srcAR = bitmap.width / bitmap.height;
    const dstAR = W / H;
    let dx = 0, dy = 0, dw = W, dh = H;
    if (srcAR > dstAR) {
      dh = Math.round(W / srcAR);
      dy = Math.round((H - dh) / 2);
    } else {
      dw = Math.round(H * srcAR);
      dx = Math.round((W - dw) / 2);
    }
    ctx.fillStyle = '#0a0d12';
    ctx.fillRect(0, 0, W, H);
    ctx.drawImage(bitmap, dx, dy, dw, dh);
    bitmap.close();
  } else {
    // No-decoder fallback: same painted overlay as before.
    ctx.fillStyle = au.keyframe ? '#1f5e3a' : '#1f3a5e';
    ctx.fillRect(0, 0, W, H);
    ctx.fillStyle = '#d6f0df';
    ctx.font = '600 36px ui-monospace, monospace';
    ctx.fillText(au.keyframe ? 'K' : 'P', 18, 50);
  }

  // Small K/P + size overlay so framing metadata is visible without
  // hiding the pixels. Positioned bottom-left over a translucent
  // strip so it's readable on any frame.
  ctx.fillStyle = 'rgba(0, 0, 0, 0.55)';
  ctx.fillRect(0, H - 28, W, 28);
  ctx.fillStyle = au.keyframe ? '#7ee7a1' : '#d9dee5';
  ctx.font = '600 13px ui-monospace, monospace';
  ctx.fillText(`${au.keyframe ? 'K' : 'P'}  seq=${au.seq}  ${humanBytes(au.bytes.byteLength)}  · click to publish user_actions`,
    8, H - 9);

  // Last-frame readout in the meta row.
  s.meta.querySelector('.last').textContent =
    `last: ${au.keyframe ? 'K' : 'P'} seq=${au.seq} ${humanBytes(au.bytes.byteLength)}`;
  s.meta.querySelector('.kp').textContent = au.keyframe ? 'keyframe' : '';
}

// ===== Panel: Tokens =====
function mountTokens(name, state) {
  const pre = document.createElement('pre');
  pre.className = 'token-stream';
  state.body.appendChild(pre);
  client.onTrack(name, (au) => {
    pre.textContent += new TextDecoder().decode(au.bytes);
    // Keep the last ~4000 chars to avoid runaway memory.
    if (pre.textContent.length > 4000) {
      pre.textContent = pre.textContent.slice(-4000);
    }
    state.body.scrollTop = state.body.scrollHeight;
    bumpCount(name);
  });
}

// ===== Panel: ToolCalls =====
function mountToolCalls(name, state) {
  const ul = document.createElement('ul');
  ul.className = 'event-list';
  state.body.appendChild(ul);
  client.onTrack(name, (au) => {
    const li = document.createElement('li');
    const text = new TextDecoder().decode(au.bytes);
    li.innerHTML = `<span class="seq">#${au.seq}</span>${escapeHtml(text)}`;
    ul.insertBefore(li, ul.firstChild);
    if (ul.childElementCount > 50) ul.removeChild(ul.lastChild);
    bumpCount(name);
  });
}

// ===== Panel: Telemetry (track-shaped, if the server uses a Telemetry track) =====
function mountTelemetry(name, state) {
  const wrap = document.createElement('div');
  state.body.appendChild(wrap);
  state.telemetryWrap = wrap;
  client.onTrack(name, (au) => {
    renderTelemetryObject(state, new TextDecoder().decode(au.bytes));
    bumpCount(name);
  });
}

function renderTelemetryObject(state, raw) {
  const wrap = state.telemetryWrap;
  let obj;
  try { obj = JSON.parse(raw); } catch { return; }
  // One row per key — replace in place.
  const entries = Object.entries(obj);
  wrap.innerHTML = entries.map(([k, v]) =>
    `<div class="gauge"><span class="k">${escapeHtml(k)}</span><span class="v">${escapeHtml(String(v))}</span></div>`
  ).join('');
}

// ===== Datagram drain =====
// Datagrams arrive on the WebTransport datagrams pipe with a 4-byte
// envelope (decodeDatagram). We render them into a virtual
// "datagrams" panel since they're not always paired with an
// announced track on the server.
async function drainDatagrams() {
  const panel = ensureDatagramPanel();
  while (client && client.getConnectionState() !== ConnectionState.Disconnected) {
    let raw;
    try {
      raw = await client.receiveDatagram();
    } catch (err) {
      const msg = (err?.message || '').toLowerCase();
      if (msg.includes('unsupported') || msg.includes('closed')) return;
      continue;
    }
    if (!raw) return;
    datagramCount++;
    let env;
    try { env = decodeDatagram(raw); } catch {
      // Not enveloped — render raw size only.
      panel.body.textContent =
        `last datagram: ${humanBytes(raw.byteLength)} (no envelope)`;
      continue;
    }
    const text = new TextDecoder().decode(env.payload);
    panel.body.innerHTML =
      `<div class="gauge"><span class="k">trackId</span><span class="v">${env.trackId}</span></div>` +
      `<div class="gauge"><span class="k">seq</span><span class="v">${env.seq}</span></div>` +
      gaugesFromMaybeJSON(text);
  }
}

function ensureDatagramPanel() {
  let p = panelByTrack.get('(datagrams)');
  if (p) return p;
  const panel = document.createElement('section');
  panel.className = 'panel';
  const head = document.createElement('header');
  head.innerHTML =
    `<span class="name">(datagrams)</span>` +
    `<span class="kind">unreliable · QUIC datagrams</span>` +
    `<span class="count" id="dg-count">0</span>`;
  panel.appendChild(head);
  const body = document.createElement('div'); body.className = 'body';
  body.textContent = 'awaiting first datagram…';
  panel.appendChild(body);
  els.panels.appendChild(panel);
  p = { el: panel, body, head, kind: 'datagrams' };
  panelByTrack.set('(datagrams)', p);
  // Bump count separately since the helper uses counters Map
  setInterval(() => {
    head.querySelector('.count').textContent = datagramCount;
  }, 250);
  return p;
}

function gaugesFromMaybeJSON(text) {
  try {
    const obj = JSON.parse(text);
    return Object.entries(obj).map(([k, v]) =>
      `<div class="gauge"><span class="k">${escapeHtml(k)}</span><span class="v">${escapeHtml(String(v))}</span></div>`
    ).join('');
  } catch {
    return `<div class="gauge"><span class="k">payload</span><span class="v">${escapeHtml(text.slice(0, 200))}</span></div>`;
  }
}

// ===== Backpressure =====
function onBackpressure(trackName, level) {
  bpLevel = Math.max(0, Math.min(5, Math.round(level / 20)));
  els.bpBar.dataset.level = String(bpLevel);
  if (trackName === 'screen' && bpLevel >= 2) {
    els.bpAction.textContent = `dropping ${trackName} (level ${level})`;
  } else if (bpLevel === 0) {
    els.bpAction.textContent = '';
  } else {
    els.bpAction.textContent = `${trackName || 'session'} level ${level}`;
  }
}

// ===== State pill =====
function renderState(s) {
  els.statePill.dataset.state = stateName(s);
  els.statePill.textContent = stateName(s);
  if (s === ConnectionState.Connecting) {
    // Reset per-track lastSeq on reconnect so gap detection doesn't
    // immediately fire a keyframe request.
    if (screenState && screenState.screen) screenState.screen.lastSeq = -1;
  }
  if (s === ConnectionState.Disconnected) {
    els.disconnect.hidden = true;
  }
}

function stateName(s) {
  switch (s) {
    case ConnectionState.Connected: return 'connected';
    case ConnectionState.Connecting: return 'connecting';
    case ConnectionState.Error: return 'error';
    default: return 'disconnected';
  }
}

// ===== DataChannel transcript =====
function appendDC(dir, text) {
  const line = document.createElement('div');
  line.className = dir;
  line.textContent = (dir === 'out' ? '> ' : '< ') + text;
  els.dcTranscript.appendChild(line);
  els.dcTranscript.scrollTop = els.dcTranscript.scrollHeight;
}

// ===== Utils =====
function humanBytes(n) {
  if (n < 1024) return n + ' B';
  if (n < 1024 * 1024) return (n / 1024).toFixed(1) + ' KB';
  if (n < 1024 * 1024 * 1024) return (n / 1024 / 1024).toFixed(1) + ' MB';
  return (n / 1024 / 1024 / 1024).toFixed(1) + ' GB';
}

function escapeHtml(s) {
  return String(s).replace(/[&<>"]/g, (c) => ({
    '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;',
  }[c]));
}
