// quicrtc · Agent Session viewer — "watch & steer your agent" demo.
//
// A product-shaped veneer over the cua servers that reads like the live
// session view of an AI coding agent (think a Devin-style "Machine" tab:
// a live desktop, the agent's activity feed, and a "take control" button).
//
// It opens TWO sessions of the SAME agent workload side by side:
//   LEFT  — a `cua/server -mode=naive` server (one connection, everything
//           on a single stream: the "standard" transport a glued stack uses).
//   RIGHT — a `cua/server -mode=multistream` server (quicrtc's per-lane
//           isolation + datagrams).
//
// The headline metric is INPUT LATENCY: how long your "take control"
// click takes to round-trip. Each side continuously fires a control ping
// (the `action` lane from examples/cua) and times the echo; clicking the
// desktop fires an extra one and drops a latency badge where you clicked.
// Under a heavy desktop stream the LEFT side's control pings queue behind
// video bytes on the one stream — the gauge climbs red and the desktop
// freezes — while the RIGHT side stays instant on its own lane.
//
// This file imports ONLY the public SDK surface from ../../dist/index.js.
// It does not touch the wire, the SDK internals, or any Go package.

import {
  QuicRTCClient,
  ConnectionState,
  TrackKind,
  type AccessUnit,
  type RemoteTrack,
} from '../../dist/index.js';

// ===== Wire shapes — must match examples/cua/server/main.go exactly. =====
interface CuaRequest { turn: number; kind: string; msg_id: number; }
interface CuaResponse { turn: number; kind: string; msg_id: number; body?: unknown; }

// The `action` lane is our "input / take-control" round-trip; the other
// three are fired only to keep the activity feed realistic (an agent is
// always reading a11y/dom/perf while it works).
const CONTROL_LANE = 'action';
const BACKGROUND_LANES = ['a11y', 'dom', 'perf'] as const;

// ===== DOM helpers =====
const $ = <T extends Element = HTMLElement>(sel: string): T =>
  document.querySelector(sel) as unknown as T;

const els = {
  form: $('#connect-form') as HTMLFormElement,
  urlA: $('#url-a') as HTMLInputElement,
  urlB: $('#url-b') as HTMLInputElement,
  rate: $('#rate') as HTMLInputElement,
  connectCard: $('#connect-card') as HTMLElement,
  connectError: $('#connect-error') as HTMLElement,
  stage: $('#stage') as HTMLElement,
  scoreboard: $('#scoreboard') as HTMLElement,
  ratio: $('#ratio') as HTMLElement,
  ratioSub: $('#ratio-sub') as HTMLElement,
  stop: $('#stop') as HTMLButtonElement,
  sessions: $('#sessions') as HTMLElement,
  sessionTpl: $('#session-template') as HTMLTemplateElement,
};

// ===== Running quantile over a sliding window (true p50/p99 of window). =====
class Quantiles {
  private buf: number[] = [];
  private cap: number;
  total = 0;
  constructor(cap = 120) { this.cap = cap; }
  push(ms: number): void {
    this.total++;
    this.buf.push(ms);
    if (this.buf.length > this.cap) this.buf.shift();
  }
  p(q: number): number | null {
    if (this.buf.length === 0) return null;
    const s = [...this.buf].sort((a, b) => a - b);
    const idx = Math.min(s.length - 1, Math.max(0, Math.ceil(q * s.length) - 1));
    return s[idx];
  }
}

// ===== One agent session (one server / one transport mode) =====
class AgentSession {
  readonly label: string;
  readonly sub: string;
  readonly shareUrl: string;
  readonly variant: 'standard' | 'quicrtc';
  client: QuicRTCClient | null = null;
  mode: 'naive' | 'multistream' | 'unknown' = 'unknown';

  // DOM
  root: HTMLElement;
  pill: HTMLElement;
  canvas: HTMLCanvasElement;
  ctx: CanvasRenderingContext2D;
  overlay: HTMLElement;
  gaugeNum: HTMLElement;
  gaugeUnit: HTMLElement;
  gaugeVerdict: HTMLElement;
  gauge: HTMLElement;
  feed: HTMLUListElement;
  fpsEl: HTMLElement;

  // Control-lane (input) measurement
  q = new Quantiles();
  lastLatency: number | null = null;
  lastResolvedAt = 0;
  // msg_id -> pending request info (only CONTROL_LANE feeds the gauge)
  pending = new Map<number, { lane: string; sentAt: number; click?: { x: number; y: number } }>();

  // screen bookkeeping
  screenFrames = 0;
  lastScreenAt = 0;
  screenWindow: number[] = [];
  everRendered = false;

  stopped = false;

  constructor(
    variant: 'standard' | 'quicrtc',
    label: string,
    sub: string,
    shareUrl: string,
    tpl: HTMLTemplateElement,
  ) {
    this.variant = variant;
    this.label = label;
    this.sub = sub;
    this.shareUrl = shareUrl;

    const frag = tpl.content.cloneNode(true) as DocumentFragment;
    this.root = frag.querySelector('.session') as HTMLElement;
    this.root.dataset.variant = variant;
    (this.root.querySelector('.session-title') as HTMLElement).textContent = label;
    (this.root.querySelector('.session-sub') as HTMLElement).textContent = sub;
    this.pill = this.root.querySelector('.pill') as HTMLElement;
    this.canvas = this.root.querySelector('.desktop-canvas') as HTMLCanvasElement;
    this.ctx = this.canvas.getContext('2d')!;
    this.overlay = this.root.querySelector('.desktop-overlay') as HTMLElement;
    this.gauge = this.root.querySelector('.gauge') as HTMLElement;
    this.gaugeNum = this.root.querySelector('.gauge-num') as HTMLElement;
    this.gaugeUnit = this.root.querySelector('.gauge-unit') as HTMLElement;
    this.gaugeVerdict = this.root.querySelector('.gauge-verdict') as HTMLElement;
    this.feed = this.root.querySelector('.feed') as HTMLUListElement;
    this.fpsEl = this.root.querySelector('.desktop-fps') as HTMLElement;

    this.paintIdle('starting…');

    // Click-to-take-control: fire an extra control ping and drop a latency
    // badge where the user clicked.
    this.canvas.addEventListener('click', (ev) => {
      const rect = this.canvas.getBoundingClientRect();
      const x = ev.clientX - rect.left;
      const y = ev.clientY - rect.top;
      void this.fireControl({ x, y });
    });
  }

  private setState(name: string): void {
    this.pill.dataset.state = name;
    this.pill.textContent = name;
  }

  async connect(): Promise<void> {
    const { url, slug, certHash } = parseShareUrl(this.shareUrl);
    this.setState('connecting');
    const client = new QuicRTCClient({ maxQueueSize: 400, reconnect: false });
    this.client = client;
    client.onStateChange((s) => {
      if (s === ConnectionState.Connected) this.setState('connected');
      else if (s === ConnectionState.Connecting) this.setState('connecting');
      else if (s === ConnectionState.Error) this.setState('error');
      else if (s === ConnectionState.Disconnected) this.setState('disconnected');
    });
    await client.connect(url, { slug, certHash });
    this.setState('connected');
    await this.subscribeTracks();
    this.drainDatagrams();
  }

  private async subscribeTracks(): Promise<void> {
    const seen = new Set<string>();
    const subscribe = (t: RemoteTrack) => {
      if (seen.has(t.name)) return;
      seen.add(t.name);
      if (t.name === 'data') this.mode = 'naive';
      if (['acks', 'a11y', 'dom', 'perf'].includes(t.name)) {
        if (this.mode !== 'naive') this.mode = 'multistream';
      }
      if (t.kind === TrackKind.Video) {
        this.client!.onTrack(t.name, (au) => void this.onScreen(au));
      } else {
        this.client!.onTrack(t.name, (au) => this.onResponseAU(au));
      }
    };
    const deadline = performance.now() + 2500;
    while (performance.now() < deadline && !this.stopped) {
      for (const t of this.client!.getRemoteTracks()) subscribe(t);
      await sleep(150);
    }
    if (this.mode === 'unknown') this.mode = 'multistream';
  }

  // ----- desktop video -----
  private async onScreen(au: AccessUnit): Promise<void> {
    this.screenFrames++;
    const now = performance.now();
    this.lastScreenAt = now;
    this.screenWindow.push(now);
    while (this.screenWindow.length && now - this.screenWindow[0] > 1000) this.screenWindow.shift();
    try {
      const png = au.bytes.slice();
      const bmp = await createImageBitmap(new Blob([png], { type: 'image/png' }));
      const { canvas, ctx } = this;
      ctx.fillStyle = '#0a0d12';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      const ar = bmp.width / bmp.height;
      let dw = canvas.width, dh = Math.round(canvas.width / ar), dx = 0, dy = (canvas.height - dh) / 2;
      if (dh > canvas.height) { dh = canvas.height; dw = Math.round(canvas.height * ar); dx = (canvas.width - dw) / 2; dy = 0; }
      ctx.drawImage(bmp, dx, dy, dw, dh);
      bmp.close();
      this.everRendered = true;
    } catch {
      const { canvas, ctx } = this;
      ctx.fillStyle = au.keyframe ? '#16324a' : '#122436';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
      this.everRendered = true;
    }
  }

  // ----- a response arriving on a track -----
  private onResponseAU(au: AccessUnit): void {
    let resp: CuaResponse;
    try { resp = JSON.parse(new TextDecoder().decode(au.bytes)) as CuaResponse; }
    catch { return; } // interleaved screen PNG on naive `data` track — skip
    this.resolve(resp);
  }

  // ----- a response arriving on a datagram (multistream metadata path) -----
  private async drainDatagrams(): Promise<void> {
    const c = this.client!;
    while (!this.stopped && c.getConnectionState() !== ConnectionState.Disconnected) {
      let raw: Uint8Array | undefined;
      try { raw = await c.receiveDatagram(); }
      catch (err) {
        const msg = (err as Error)?.message?.toLowerCase() ?? '';
        if (msg.includes('unsupported') || msg.includes('closed')) return;
        continue;
      }
      if (!raw) return;
      let resp: CuaResponse;
      try { resp = JSON.parse(new TextDecoder().decode(raw)) as CuaResponse; }
      catch { continue; }
      this.resolve(resp);
    }
  }

  private resolve(resp: CuaResponse): void {
    const p = this.pending.get(resp.msg_id);
    if (!p) return;
    this.pending.delete(resp.msg_id);
    const lat = performance.now() - p.sentAt;
    if (p.lane === CONTROL_LANE) {
      this.q.push(lat);
      this.lastLatency = lat;
      this.lastResolvedAt = performance.now();
      if (p.click) this.dropBadge(p.click.x, p.click.y, lat);
    } else {
      this.appendFeed(p.lane, lat);
    }
  }

  // Fire a control ping (the "take control" round-trip). Optionally tie it
  // to a click position so the resolved latency lands as a badge there.
  async fireControl(click?: { x: number; y: number }): Promise<void> {
    const c = this.client;
    if (!c || this.stopped) return;
    const id = nextMsgId();
    this.pending.set(id, { lane: CONTROL_LANE, sentAt: performance.now(), click });
    const req: CuaRequest = { turn: turnCounter, kind: CONTROL_LANE, msg_id: id };
    try { await c.sendDatagram(new TextEncoder().encode(JSON.stringify(req))); }
    catch { this.pending.delete(id); }
  }

  // Fire the background reads (a11y/dom/perf) so the activity feed looks alive.
  async fireBackground(): Promise<void> {
    const c = this.client;
    if (!c || this.stopped) return;
    for (const lane of BACKGROUND_LANES) {
      const id = nextMsgId();
      this.pending.set(id, { lane, sentAt: performance.now() });
      const req: CuaRequest = { turn: turnCounter, kind: lane, msg_id: id };
      try { await c.sendDatagram(new TextEncoder().encode(JSON.stringify(req))); }
      catch { this.pending.delete(id); return; }
    }
    // Bound the pending map under a sustained freeze.
    if (this.pending.size > 2000) {
      const cutoff = performance.now() - 10_000;
      for (const [id, v] of this.pending) if (v.sentAt < cutoff) this.pending.delete(id);
    }
  }

  private appendFeed(lane: string, lat: number): void {
    const li = document.createElement('li');
    const cls = lat > 200 ? 'bad' : lat > 60 ? 'slow' : '';
    li.innerHTML =
      `<span class="feed-lane">${lane}</span>` +
      `<span class="feed-lat ${cls}">${lat.toFixed(0)} ms</span>`;
    this.feed.appendChild(li);
    while (this.feed.childElementCount > 60) this.feed.removeChild(this.feed.firstChild!);
    this.feed.scrollTop = this.feed.scrollHeight;
  }

  private dropBadge(x: number, y: number, lat: number): void {
    const b = document.createElement('div');
    b.className = 'click-badge';
    const cls = lat > 200 ? 'bad' : lat > 60 ? 'slow' : 'good';
    b.classList.add(cls);
    b.textContent = `${lat.toFixed(0)} ms`;
    b.style.left = `${x}px`;
    b.style.top = `${y}px`;
    this.overlay.appendChild(b);
    setTimeout(() => b.remove(), 1400);
  }

  // The number the gauge shows: the worse of (last resolved control latency)
  // and (age of the oldest still-pending control ping). While the lane is
  // healthy this is just the round-trip time; while it's stalled the pending
  // age climbs so the gauge visibly counts upward and turns red.
  currentLatency(now: number): number | null {
    let oldestPending = 0;
    for (const v of this.pending.values()) {
      if (v.lane !== CONTROL_LANE) continue;
      const age = now - v.sentAt;
      if (age > oldestPending) oldestPending = age;
    }
    const resolved = this.lastLatency ?? 0;
    const val = Math.max(resolved, oldestPending);
    if (this.q.total === 0 && oldestPending === 0) return null;
    return val;
  }

  render(now: number): void {
    const lat = this.currentLatency(now);
    let level: 'good' | 'slow' | 'bad' = 'good';
    if (lat == null) {
      this.gaugeNum.textContent = '—';
      this.gaugeVerdict.textContent = 'waiting…';
    } else {
      this.gaugeNum.textContent = lat < 1000 ? lat.toFixed(0) : (lat / 1000).toFixed(1);
      this.gaugeUnit.textContent = lat < 1000 ? 'ms' : 's';
      if (lat > 200) level = 'bad';
      else if (lat > 60) level = 'slow';
      this.gaugeVerdict.textContent =
        level === 'good' ? 'Instant \u2713' : level === 'slow' ? 'Lagging' : 'Frozen \u2717';
    }
    this.gauge.dataset.level = level;

    // Desktop freeze overlay: no new frame for a while under load.
    const screenAge = this.lastScreenAt ? now - this.lastScreenAt : Infinity;
    const frozen = this.everRendered && screenAge > 600;
    this.root.classList.toggle('frozen', frozen);
    this.overlay.dataset.frozen = String(frozen);
    if (this.lastScreenAt) {
      this.fpsEl.textContent = `${this.screenWindow.length} fps`;
    } else {
      this.fpsEl.textContent = 'no frames';
    }
  }

  private paintIdle(msg: string): void {
    const { canvas, ctx } = this;
    ctx.fillStyle = '#0a0d12';
    ctx.fillRect(0, 0, canvas.width, canvas.height);
    ctx.fillStyle = '#3a4a5e';
    ctx.font = '14px system-ui, sans-serif';
    ctx.textAlign = 'center';
    ctx.fillText(msg, canvas.width / 2, canvas.height / 2);
  }

  close(): void {
    this.stopped = true;
    this.client?.close().catch(() => {});
  }
}

// ===== Driver =====
let sessions: AgentSession[] = [];
let controlTimer: number | null = null;
let bgTimer: number | null = null;
let uiTimer: number | null = null;
let msgIdCounter = 1;
let turnCounter = 0;
function nextMsgId(): number { return msgIdCounter++; }

els.form.addEventListener('submit', async (e) => {
  e.preventDefault();
  els.connectError.hidden = true;
  await start();
});
els.stop.addEventListener('click', () => stop());

async function start(): Promise<void> {
  const aUrl = els.urlA.value.trim();
  const bUrl = els.urlB.value.trim();
  if (!aUrl || !bUrl) { showError('paste BOTH share URLs (standard + quicrtc)'); return; }

  try {
    localStorage.setItem('agent.a', aUrl);
    localStorage.setItem('agent.b', bUrl);
  } catch { /* private mode */ }

  stop();
  els.sessions.innerHTML = '';
  sessions = [
    new AgentSession('standard', 'Standard transport', 'one connection · everything shares a stream', aUrl, els.sessionTpl),
    new AgentSession('quicrtc', 'quicrtc', 'one connection · a dedicated lane per stream', bUrl, els.sessionTpl),
  ];
  for (const s of sessions) els.sessions.appendChild(s.root);
  els.stage.hidden = false;
  els.connectCard.hidden = true;

  const results = await Promise.allSettled(sessions.map((s) => s.connect()));
  const failed = results
    .map((r, i) => (r.status === 'rejected' ? `${sessions[i].label}: ${(r.reason as Error)?.message || r.reason}` : null))
    .filter(Boolean);
  if (results.every((r) => r.status === 'rejected')) {
    showError('both sessions failed — ' + failed.join('  |  '));
    els.stage.hidden = true;
    els.connectCard.hidden = false;
    return;
  }
  els.stop.hidden = false;

  const rate = clampRate(parseInt(els.rate.value, 10) || 10);
  const controlPeriod = Math.max(20, Math.round(1000 / rate));
  // Control pings (the input round-trip that feeds the gauge).
  controlTimer = window.setInterval(() => {
    turnCounter++;
    for (const s of sessions) void s.fireControl();
  }, controlPeriod);
  // Background reads a bit slower — just to keep the activity feed alive.
  bgTimer = window.setInterval(() => {
    for (const s of sessions) void s.fireBackground();
  }, 120);
  // UI repaint.
  uiTimer = window.setInterval(() => {
    const now = performance.now();
    for (const s of sessions) s.render(now);
    renderScoreboard(now);
  }, 100);
}

function renderScoreboard(now: number): void {
  if (sessions.length < 2) { els.scoreboard.hidden = true; return; }
  els.scoreboard.hidden = false;
  const std = sessions[0].currentLatency(now);
  const qr = sessions[1].currentLatency(now);
  if (std == null || qr == null || qr <= 0) {
    els.ratio.textContent = '—';
    els.ratioSub.textContent = 'measuring…';
    return;
  }
  const factor = std / qr;
  if (factor >= 1.5) {
    els.ratio.textContent = `${factor.toFixed(factor >= 10 ? 0 : 1)}\u00d7`;
    els.ratioSub.textContent = 'quicrtc is more responsive right now';
    els.scoreboard.dataset.state = 'win';
  } else {
    els.ratio.textContent = 'even';
    els.ratioSub.textContent = 'no contention yet — drive the desktop harder';
    els.scoreboard.dataset.state = 'even';
  }
}

function stop(): void {
  if (controlTimer != null) { clearInterval(controlTimer); controlTimer = null; }
  if (bgTimer != null) { clearInterval(bgTimer); bgTimer = null; }
  if (uiTimer != null) { clearInterval(uiTimer); uiTimer = null; }
  for (const s of sessions) s.close();
  els.stop.hidden = true;
}

// ===== helpers =====
function parseShareUrl(raw: string): { url: string; slug: string; certHash: string } {
  if (!raw) throw new Error('empty URL');
  let u: URL;
  try { u = new URL(raw); } catch { throw new Error('not a valid URL'); }
  if (u.protocol !== 'https:') throw new Error('URL must be https://');
  const frag = new URLSearchParams(u.hash.replace(/^#/, ''));
  const slug = frag.get('slug') ?? '';
  const certHash = frag.get('hash') ?? '';
  if (!slug) throw new Error('fragment missing #slug=');
  if (!certHash) throw new Error('fragment missing &hash= (cert pin)');
  return { url: u.origin + u.pathname + u.search, slug, certHash };
}

function showError(msg: string): void {
  els.connectError.textContent = msg;
  els.connectError.hidden = false;
}

function clampRate(n: number): number { return Math.max(1, Math.min(60, n)); }
function sleep(ms: number): Promise<void> { return new Promise((r) => setTimeout(r, ms)); }

// Pre-fill from last run.
try {
  els.urlA.value = localStorage.getItem('agent.a') ?? '';
  els.urlB.value = localStorage.getItem('agent.b') ?? '';
} catch { /* ignore */ }
