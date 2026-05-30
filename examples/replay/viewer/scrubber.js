// Browser scrubber for a quicrtc session recording. Loads a bundle.json
// produced by `replay -file <rec> -json -payloads` (timeline metadata +
// base64 frame payloads) and lets you drag a playhead across the capture
// clock, rendering the cross-track state — exactly Timeline.SeekSnapshot,
// in the browser. Pure vanilla JS + canvas, no dependencies.

const KIND_COLOR = { video: '#2f6fed', tokens: '#15a34a', toolcalls: '#e08a1e', telemetry: '#64748b' };
const SUBKIND = { screen: 'video', reasoning: 'tokens', toolcalls: 'toolcalls', telemetry: 'telemetry' };
// Each lane's quicrtc delivery class — the whole point the timeline makes
// visible: four kinds of traffic, four delivery guarantees, one capture clock.
const DELIVERY = { screen: 'stream/GOP', reasoning: 'low-latency', toolcalls: 'bidi/call', telemetry: 'datagram' };
const ORDER = ['screen', 'reasoning', 'toolcalls', 'telemetry'];

let DATA = null, maxMs = 1, gluedEnd = 1, play = 300, timer = null;

// Clock model.
//   'quic'  — one QUIC connection, one capture clock. Every lane's rel_ms is
//             on the same clock, so the reconstruction is exact and the
//             session ends at maxMs.
//   'glued' — four independent connections (WebRTC + SSE + gRPC + OTLP), each
//             with its own delivery latency (the GLUED table below). Delivery
//             is never *earlier* than capture, so every lane is pushed
//             strictly later; the slowest lane drags the session's end past
//             maxMs, and because the per-lane rates differ the cross-lane skew
//             starts small and GROWS over the session. quicrtc adds none of
//             this (skew stays 0).
let MODE = 'quic';
// GLUED holds illustrative per-connection delivery latencies: delay(lane, t) =
// base + rate·t — a setup/RTT/buffer floor plus a portion that accumulates
// (bufferbloat, batched OTLP export, SSE proxy buffering).
const GLUED = {
  screen:    { base: 80,  rate: 0.02 },
  toolcalls: { base: 110, rate: 0.05 },
  reasoning: { base: 140, rate: 0.09 },
  telemetry: { base: 180, rate: 0.14 },
};
function gluedDelay(f) { const g = GLUED[f.track] || { base: 0, rate: 0 }; return g.base + g.rate * f.rel_ms; }
const eff = f => f.rel_ms + (MODE === 'glued' ? gluedDelay(f) : 0);
const endFor = m => m === 'glued' ? gluedEnd : maxMs;

const cv = document.getElementById('tl'), ctx = cv.getContext('2d');
const scrub = document.getElementById('scrub'), timeEl = document.getElementById('time'), playBtn = document.getElementById('play');

async function load() {
  try {
    const r = await fetch('./bundle.json');
    if (!r.ok) throw new Error('no bundle');
    init(await r.json());
  } catch (e) {
    document.getElementById('drop').style.display = 'block';
  }
}
window.addEventListener('dragover', e => e.preventDefault());
window.addEventListener('drop', async e => {
  e.preventDefault();
  const f = e.dataTransfer.files[0];
  if (f) init(JSON.parse(await f.text()));
});

function init(d) {
  DATA = d;
  maxMs = Math.max(1, ...d.frames.map(f => f.rel_ms)) + 40;
  gluedEnd = Math.max(maxMs, ...d.frames.map(f => f.rel_ms + gluedDelay(f))) + 40;
  axMin = 0; axMax = gluedEnd;          // fixed scale for both modes
  scrub.max = endFor(MODE);
  play = Math.min(play, +scrub.max);
  scrub.value = play;
  document.getElementById('drop').style.display = 'none';
  render();
}

// Re-sync the backing buffer to the canvas's CSS size. Done at the top of
// every render so it's robust to layout not being settled at init (a
// 0-width read there would otherwise leave the canvas blank forever).
function fit() {
  const w = Math.round(cv.clientWidth * devicePixelRatio), h = Math.round(200 * devicePixelRatio);
  if (w > 0 && (cv.width !== w || cv.height !== h)) {
    cv.width = w; cv.height = h;
    ctx.setTransform(devicePixelRatio, 0, 0, devicePixelRatio, 0, 0);
  }
}
window.addEventListener('resize', () => { if (DATA) render(); });

// Axis range in ms — a fixed scale of [0, gluedEnd] in BOTH modes, so the
// glued stack's later session end shows up as dots reaching into space the
// quicrtc timeline leaves empty. Set once in init().
let axMin = 0, axMax = 1;
const X = ms => { const L = 116, R = cv.clientWidth - 20; return L + ((ms - axMin) / (axMax - axMin)) * (R - L); };

// latest frame per track at or before t — the SeekSnapshot reconstruction.
function snapshot(ms) {
  const s = {};
  for (const f of DATA.frames) {
    if (eff(f) <= ms && (!s[f.track] || eff(f) > eff(s[f.track]))) s[f.track] = f;
  }
  return s;
}

function roundRect(x, y, w, h, r) {
  ctx.beginPath();
  ctx.moveTo(x + r, y);
  ctx.arcTo(x + w, y, x + w, y + h, r);
  ctx.arcTo(x + w, y + h, x, y + h, r);
  ctx.arcTo(x, y + h, x, y, r);
  ctx.arcTo(x, y, x + w, y, r);
  ctx.closePath();
}

// niceStep picks a human-friendly axis interval (…,50,100,250,500,1000,…)
// near the requested raw size, so the time axis has ~9 readable labels at
// any session length instead of a hardcoded 50 ms (which smears on a long
// recording).
function niceStep(raw) {
  const pow = Math.pow(10, Math.floor(Math.log10(Math.max(1, raw))));
  const f = raw / pow;
  const n = f <= 1 ? 1 : f <= 2 ? 2 : f <= 2.5 ? 2.5 : f <= 5 ? 5 : 10;
  return Math.max(1, n * pow);
}

function render() {
  fit();
  const W = cv.clientWidth, top = 34, rh = 38;
  ctx.clearRect(0, 0, W, 200);
  ctx.font = '11px sans-serif';
  const step = niceStep(axMax / 9), useSec = axMax >= 2000;
  for (let ms = 0; ms <= axMax + 1; ms += step) {
    const x = X(ms);
    ctx.strokeStyle = '#eef0f3'; ctx.beginPath(); ctx.moveTo(x, top - 6); ctx.lineTo(x, top + 4 * rh); ctx.stroke();
    if (x < W - 34) {
      ctx.fillStyle = '#9aa3ae';
      ctx.fillText(useSec ? +(ms / 1000).toFixed(2) + 's' : ms + 'ms', x + 4, top - 11);
    }
  }
  const snap = snapshot(play);
  ORDER.forEach((trk, i) => {
    const y = top + i * rh, c = KIND_COLOR[SUBKIND[trk]];
    ctx.fillStyle = '#3a4150'; ctx.font = '600 12px sans-serif'; ctx.fillText(trk, 12, y + rh / 2 - 1);
    ctx.fillStyle = '#9aa3ae'; ctx.font = '10px sans-serif'; ctx.fillText(DELIVERY[trk], 12, y + rh / 2 + 12);
    ctx.fillStyle = c; ctx.fillRect(106, y + 6, 3, rh - 14);
  });
  // Compact fixed-width markers (not text-length boxes, which overlap into
  // a smear when events cluster on a long session). De-overlap markers on
  // the same track so a tight cluster reads as adjacent ticks.
  const lastRight = {};
  for (const f of DATA.frames) {
    const i = ORDER.indexOf(f.track);
    if (i < 0) continue;
    const y = top + i * rh + 9, c = KIND_COLOR[f.kind] || '#888', w = 11, h = rh - 18;
    let x = X(eff(f)) - w / 2;
    if (lastRight[f.track] !== undefined && x < lastRight[f.track] + 2) x = lastRight[f.track] + 2;
    lastRight[f.track] = x + w;
    ctx.fillStyle = c; roundRect(x, y, w, h, 3); ctx.fill();
    if (f.keyframe) { ctx.strokeStyle = '#eab308'; ctx.lineWidth = 2; roundRect(x, y, w, h, 3); ctx.stroke(); }
    if (snap[f.track] === f) {
      ctx.strokeStyle = '#e11d48'; ctx.lineWidth = 2.5; roundRect(x - 2.5, y - 2.5, w + 5, h + 5, 5); ctx.stroke();
    }
  }
  // Playhead — one scrub line. In quicrtc mode the lanes' dots line up under
  // it (one clock); in glued mode each lane's dots are shifted by its own
  // clock offset, so the rows visibly drift apart around this same line.
  const px = X(play);
  ctx.strokeStyle = '#e11d48'; ctx.lineWidth = 2; ctx.setLineDash([4, 4]);
  ctx.beginPath(); ctx.moveTo(px, top - 8); ctx.lineTo(px, top + 4 * rh); ctx.stroke(); ctx.setLineDash([]);
  updatePanels(snap);
}

function text(f) {
  if (f.payload_b64) {
    try {
      // atob yields raw bytes (one char per byte); decode as UTF-8 so
      // multi-byte glyphs (curly quotes, accents, emoji) render correctly
      // instead of mojibake like "â€œ".
      const bin = atob(f.payload_b64);
      return new TextDecoder().decode(Uint8Array.from(bin, ch => ch.charCodeAt(0)));
    } catch (e) { }
  }
  return f.preview.replace(/^"|"$/g, '');
}

function updatePanels(snap) {
  timeEl.textContent = 't = ' + play + ' ms';
  updateSkew(snap);
  const sc = snap['screen'];
  if (sc && sc.payload_b64) {
    // screen frames are JPEG ("/9j/…") from the browser capture; the synth
    // sample is PNG ("iVBOR…"). Sniff the magic so both render.
    const mime = sc.payload_b64.startsWith('/9j/') ? 'image/jpeg' : 'image/png';
    document.getElementById('screenImg').src = `data:${mime};base64,` + sc.payload_b64;
  } else document.getElementById('screenImg').src = '';

  const toks = DATA.frames.filter(f => f.track === 'reasoning' && eff(f) <= play);
  document.getElementById('reasoning').innerHTML =
    toks.map((f, i) => `<span class="tok ${i === toks.length - 1 ? 'cur' : ''}">${text(f)}</span>`).join(' ') || '<span class="tok">—</span>';

  const calls = DATA.frames.filter(f => f.track === 'toolcalls' && eff(f) <= play);
  document.getElementById('tools').innerHTML =
    calls.map((f, i) => `<div class="call ${i === calls.length - 1 ? 'cur' : ''}">+${f.rel_ms}ms&nbsp;·&nbsp;${text(f)}</div>`).reverse().join('') || '<div class="call">—</div>';

  const tl = snap['telemetry'];
  let html = '<div class="g"><span>—</span><b></b></div>';
  if (tl) { try { html = Object.entries(JSON.parse(text(tl))).map(([k, v]) => `<div class="g"><span>${k}</span><b>${v}</b></div>`).join(''); } catch (e) { html = text(tl); } }
  document.getElementById('tele').innerHTML = html;
}

// updateSkew reflects the clock model in the readout chip: 0 ms (exact) for
// quicrtc's single capture clock, or the worst-case spread of the per-lane
// offsets for the glued multi-connection stack.
function updateSkew(snap) {
  const sk = document.getElementById('skew');
  if (!sk) return;
  if (MODE === 'quic') { sk.textContent = 'cross-lane skew ≈ 0 ms · exact'; sk.className = 'skew ok'; return; }
  // skew = spread of the delivery delays of the frames each lane is currently
  // showing — so it grows as you scrub later into the session.
  const delays = ORDER.filter(t => snap[t]).map(t => gluedDelay(snap[t]));
  const spread = delays.length ? Math.round(Math.max(...delays) - Math.min(...delays)) : 0;
  sk.textContent = `cross-lane skew ≈ ${spread} ms · approximate`;
  sk.className = 'skew bad';
}

// --- interaction ---
document.querySelectorAll('#seg .opt').forEach(btn => btn.addEventListener('click', () => {
  MODE = btn.dataset.mode;
  document.querySelectorAll('#seg .opt').forEach(b => b.classList.toggle('active', b === btn));
  scrub.max = endFor(MODE);
  if (play > +scrub.max) play = +scrub.max;
  scrub.value = play;
  const note = document.getElementById('modenote');
  if (note) note.textContent = MODE === 'glued'
    ? `simulated — each lane delayed by its connection; session ends ~${Math.round(gluedEnd - maxMs)} ms later`
    : 'every lane shares one capture clock; nothing is delayed';
  if (DATA) render();
}));
function setFromX(clientX) {
  const r = cv.getBoundingClientRect(), L = 116, R = cv.clientWidth - 20;
  let ms = Math.round(axMin + ((clientX - r.left - L) / (R - L)) * (axMax - axMin));
  play = Math.max(0, Math.min(+scrub.max, ms));
  scrub.value = play;
  render();
}
let drag = false;
cv.addEventListener('mousedown', e => { stop(); drag = true; setFromX(e.clientX); });
window.addEventListener('mousemove', e => { if (drag) setFromX(e.clientX); });
window.addEventListener('mouseup', () => drag = false);
scrub.addEventListener('input', () => { stop(); play = +scrub.value; render(); });

function stop() { if (timer) { clearInterval(timer); timer = null; playBtn.textContent = '▶ play'; } }
playBtn.addEventListener('click', () => {
  if (timer) { stop(); return; }
  if (play >= +scrub.max) play = 0;
  playBtn.textContent = '❚❚ pause';
  timer = setInterval(() => {
    play += 8;
    if (play >= +scrub.max) { play = +scrub.max; stop(); }
    scrub.value = play; render();
  }, 16);
});

load();
