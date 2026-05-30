// quicrtc · naive vs multistream — side-by-side contention demo.
//
// Opens TWO quicrtc sessions (pane A → a `-mode=naive` server, pane B
// → a `-mode=multistream` server) and drives the *identical* CUA-shaped
// workload against both: each turn the browser fires four request
// datagrams ({action, a11y, dom, perf}, matching examples/cua's wire
// types) and the server echoes a response per request — on a track AU
// (naive: all on the single `data` stream; multistream: one stream per
// Kind) or as a raw datagram (multistream + `-datagram-metadata`, which
// `-stress` enables).
//
// We time every echo (browser send → browser receive) per lane, keep a
// running p50/p99, render the live screen frames, a scrolling feed, and
// — the headline — a per-lane "last update age" / FREEZE indicator. When
// a 30 MB/s screen burst saturates the link, pane A's small-lane echoes
// queue behind screen bytes on the one stream and the freeze badge lights
// up red; pane B's lanes ride dedicated streams / datagrams and keep
// flowing.
//
// HONEST NOTE: on localhost the link is too fast to see the divergence
// (see README). Real contention needs `tc netem` or a WAN VM.
//
// This file imports ONLY the public SDK surface from ../../dist/index.js.
// It does not touch the wire, the SDK, or any Go package.

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

// The four request lanes the cua server understands, plus the screen
// track that carries the contending background video.
const LANES = ['action', 'a11y', 'dom', 'perf'] as const;
type LaneName = typeof LANES[number];

// Multistream maps each response Kind to a named track (server's
// trackForKind); naive sends everything on the single `data` track.
const TRACK_FOR_KIND: Record<LaneName, string> = {
  action: 'acks',
  a11y: 'a11y',
  dom: 'dom',
  perf: 'perf',
};

// ===== DOM refs =====
const $ = <T extends Element = HTMLElement>(sel: string): T =>
  document.querySelector(sel) as unknown as T;

const els = {
  form: $('#connect-form') as HTMLFormElement,
  urlA: $('#url-a') as HTMLInputElement,
  urlB: $('#url-b') as HTMLInputElement,
  turnRate: $('#turn-rate') as HTMLInputElement,
  connectCard: $('#connect-card') as HTMLElement,
  connectError: $('#connect-error') as HTMLElement,
  panes: $('#panes') as HTMLElement,
  stop: $('#stop') as HTMLButtonElement,
  paneTpl: $('#pane-template') as HTMLTemplateElement,
  laneTpl: $('#lane-template') as HTMLTemplateElement,
};

// ===== Running quantile estimate over a sliding window =====
// A bounded ring buffer + sort-on-read is plenty for a few hundred
// samples and keeps the readout honest (true p50/p99 of the window,
// no approximation drift).
class Quantiles {
  private buf: number[] = [];
  private cap: number;
  total = 0;
  constructor(cap = 200) { this.cap = cap; }
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

// ===== One pane (one server / one mode) =====
class Pane {
  readonly label: string;
  readonly shareUrl: string;
  client: QuicRTCClient | null = null;
  mode: 'naive' | 'multistream' | 'unknown' = 'unknown';

  // DOM
  root: HTMLElement;
  pill: HTMLElement;
  modeEl: HTMLElement;
  canvas: HTMLCanvasElement;
  ctx: CanvasRenderingContext2D;
  screenLast: HTMLElement;
  screenRate: HTMLElement;
  feed: HTMLUListElement;
  laneEls = new Map<LaneName, {
    root: HTMLElement; freeze: HTMLElement; age: HTMLElement;
    p50: HTMLElement; p99: HTMLElement; n: HTMLElement;
  }>();

  // Per-lane measurement
  q = new Map<LaneName, Quantiles>();
  lastUpdate = new Map<LaneName, number>(); // performance.now() of last echo
  // msg_id -> { lane, sentAt } so a response can be matched back.
  pending = new Map<number, { lane: LaneName; sentAt: number }>();

  // screen frame bookkeeping
  screenFrames = 0;
  lastScreenAt = 0;
  screenWindow: number[] = []; // recent frame timestamps for fps

  stopped = false;

  constructor(label: string, shareUrl: string, paneTpl: HTMLTemplateElement, laneTpl: HTMLTemplateElement) {
    this.label = label;
    this.shareUrl = shareUrl;

    const frag = paneTpl.content.cloneNode(true) as DocumentFragment;
    this.root = frag.querySelector('.pane') as HTMLElement;
    this.pill = this.root.querySelector('.pill') as HTMLElement;
    this.modeEl = this.root.querySelector('.pane-mode') as HTMLElement;
    (this.root.querySelector('.pane-title') as HTMLElement).textContent = label;
    this.canvas = this.root.querySelector('.screen-canvas') as HTMLCanvasElement;
    this.ctx = this.canvas.getContext('2d')!;
    this.screenLast = this.root.querySelector('.screen-last') as HTMLElement;
    this.screenRate = this.root.querySelector('.screen-rate') as HTMLElement;
    this.feed = this.root.querySelector('.feed') as HTMLUListElement;

    const lanesHost = this.root.querySelector('.lanes') as HTMLElement;
    for (const lane of LANES) {
      const lf = laneTpl.content.cloneNode(true) as DocumentFragment;
      const laneRoot = lf.querySelector('.lane') as HTMLElement;
      laneRoot.dataset.lane = lane;
      (laneRoot.querySelector('.lane-name') as HTMLElement).textContent = lane;
      lanesHost.appendChild(laneRoot);
      this.laneEls.set(lane, {
        root: laneRoot,
        freeze: laneRoot.querySelector('.lane-freeze') as HTMLElement,
        age: laneRoot.querySelector('.age-ms') as HTMLElement,
        p50: laneRoot.querySelector('.p50') as HTMLElement,
        p99: laneRoot.querySelector('.p99') as HTMLElement,
        n: laneRoot.querySelector('.n') as HTMLElement,
      });
      this.q.set(lane, new Quantiles());
    }
  }

  setState(name: string): void {
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

    // Detect mode + subscribe to whatever response tracks exist. Naive
    // announces a single `data` track; multistream announces the five
    // named tracks. Poll briefly because announces can trail the SDP.
    await this.subscribeTracks();

    // Drain raw datagrams (multistream + datagram-metadata path). These
    // are plain JSON responses (no envelope) — server uses h.SendDatagram
    // with the marshalled response directly.
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
      this.modeEl.textContent = `mode=${this.mode} · ${[...seen].join(',')}`;

      if (t.kind === TrackKind.Video) {
        this.client!.onTrack(t.name, (au) => this.onScreen(au));
      } else {
        // Tokens-shaped response tracks. Each AU is a JSON CuaResponse
        // we match back by msg_id (or, on the naive `data` track, also
        // carries interleaved screen PNG bytes that won't parse — skip).
        this.client!.onTrack(t.name, (au) => this.onResponseAU(t.name, au));
      }
    };

    const deadline = performance.now() + 2500;
    while (performance.now() < deadline && !this.stopped) {
      for (const t of this.client!.getRemoteTracks()) subscribe(t);
      await sleep(150);
    }
    if (this.mode === 'unknown') {
      // No named response track yet — assume multistream so we still
      // listen on datagrams. Worst case the feed just shows datagram echoes.
      this.mode = 'multistream';
      this.modeEl.textContent = `mode=${this.mode} · (tracks: ${[...seen].join(',') || 'none'})`;
    }
  }

  // ----- screen video -----
  private async onScreen(au: AccessUnit): Promise<void> {
    this.screenFrames++;
    const now = performance.now();
    this.lastScreenAt = now;
    this.screenWindow.push(now);
    while (this.screenWindow.length && now - this.screenWindow[0] > 1000) this.screenWindow.shift();

    this.screenLast.textContent =
      `frame seq=${au.seq} ${humanBytes(au.bytes.byteLength)}`;
    // Draw the real PNG bytes. Trailing pad bytes (the server pads to a
    // target size) are ignored by the decoder.
    try {
      // Copy into a plain ArrayBuffer-backed view so the Blob ctor's
      // BlobPart typing is satisfied across TS lib versions.
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
    } catch {
      // Non-decodable padded bytes: just flash a marker so liveness shows.
      const { canvas, ctx } = this;
      ctx.fillStyle = au.keyframe ? '#1f5e3a' : '#1f3a5e';
      ctx.fillRect(0, 0, canvas.width, canvas.height);
    }
  }

  // ----- response on a track -----
  private onResponseAU(_track: string, au: AccessUnit): void {
    let resp: CuaResponse;
    try { resp = JSON.parse(new TextDecoder().decode(au.bytes)) as CuaResponse; }
    catch { return; } // interleaved screen PNG on naive `data` track — skip
    this.resolve(resp);
  }

  // ----- response on a raw datagram -----
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
      // cua server sends metadata responses as plain JSON datagrams.
      let resp: CuaResponse;
      try { resp = JSON.parse(new TextDecoder().decode(raw)) as CuaResponse; }
      catch { continue; }
      this.resolve(resp);
    }
  }

  // Match a response back to its request, record the echo latency, and
  // mark the lane fresh.
  private resolve(resp: CuaResponse): void {
    const p = this.pending.get(resp.msg_id);
    if (!p) return;
    this.pending.delete(resp.msg_id);
    const lat = performance.now() - p.sentAt;
    this.q.get(p.lane)!.push(lat);
    this.lastUpdate.set(p.lane, performance.now());
    this.appendFeed(p.lane, lat);
  }

  // Fire one turn: four request datagrams, one per lane.
  async fireTurn(turn: number, nextMsgId: () => number): Promise<void> {
    const c = this.client;
    if (!c || this.stopped) return;
    for (const lane of LANES) {
      const id = nextMsgId();
      const req: CuaRequest = { turn, kind: lane, msg_id: id };
      this.pending.set(id, { lane, sentAt: performance.now() });
      try {
        await c.sendDatagram(new TextEncoder().encode(JSON.stringify(req)));
      } catch {
        this.pending.delete(id);
        return; // connection went away
      }
    }
    // Drop very old un-answered requests so the pending map can't grow
    // unbounded under sustained loss (frozen lane).
    if (this.pending.size > 2000) {
      const cutoff = performance.now() - 10_000;
      for (const [id, v] of this.pending) if (v.sentAt < cutoff) this.pending.delete(id);
    }
  }

  private appendFeed(lane: LaneName, lat: number): void {
    const li = document.createElement('li');
    const cls = lat > 200 ? 'bad' : lat > 60 ? 'slow' : '';
    li.innerHTML =
      `<span class="lane-tag">${lane}</span>` +
      `<span class="lat ${cls}">${lat.toFixed(1)} ms</span>`;
    this.feed.appendChild(li);
    while (this.feed.childElementCount > 120) this.feed.removeChild(this.feed.firstChild!);
    this.feed.scrollTop = this.feed.scrollHeight;
  }

  // Repaint lane readouts: p50/p99 + freeze age. Called on a UI ticker.
  render(now: number): void {
    for (const lane of LANES) {
      const e = this.laneEls.get(lane)!;
      const q = this.q.get(lane)!;
      const p50 = q.p(0.5), p99 = q.p(0.99);
      e.p50.textContent = p50 == null ? '—' : `${p50.toFixed(0)}ms`;
      e.p99.textContent = p99 == null ? '—' : `${p99.toFixed(0)}ms`;
      e.n.textContent = String(q.total);

      const last = this.lastUpdate.get(lane);
      if (last == null) { e.age.textContent = '—'; continue; }
      const age = now - last;
      e.age.textContent = age < 1000 ? `${age.toFixed(0)} ms` : `${(age / 1000).toFixed(1)} s`;
      // Freeze thresholds: a turn cadence is ~50ms at 20/s, so anything
      // past a few hundred ms with no echo is a visible stall.
      let level = 0;
      if (age > 1500) level = 2; else if (age > 400) level = 1;
      e.freeze.dataset.frozen = String(level);
      e.freeze.textContent = level === 2 ? 'FROZEN' : level === 1 ? 'stalling' : 'live';
      e.root.classList.toggle('frozen', level === 2);
    }
    // screen fps readout
    if (this.lastScreenAt) {
      const fps = this.screenWindow.length;
      this.screenRate.textContent = `${fps} fps · ${this.screenFrames} frames`;
    }
  }

  close(): void {
    this.stopped = true;
    this.client?.close().catch(() => {});
  }
}

// ===== Driver =====
let panes: Pane[] = [];
let turnTimer: number | null = null;
let uiTimer: number | null = null;
let msgIdCounter = 1;
let turnCounter = 0;

els.form.addEventListener('submit', async (e) => {
  e.preventDefault();
  els.connectError.hidden = true;
  await start();
});

els.stop.addEventListener('click', () => stop());

async function start(): Promise<void> {
  const aUrl = els.urlA.value.trim();
  const bUrl = els.urlB.value.trim();
  if (!aUrl && !bUrl) { showError('paste at least one share URL'); return; }

  // Persist for reload convenience.
  try {
    localStorage.setItem('compare.a', aUrl);
    localStorage.setItem('compare.b', bUrl);
  } catch { /* private mode */ }

  stop(); // clear any previous run
  els.panes.innerHTML = '';
  panes = [];

  if (aUrl) panes.push(new Pane('Pane A — naive', aUrl, els.paneTpl, els.laneTpl));
  if (bUrl) panes.push(new Pane('Pane B — multistream', bUrl, els.paneTpl, els.laneTpl));
  for (const p of panes) els.panes.appendChild(p.root);
  els.panes.hidden = false;

  const results = await Promise.allSettled(panes.map((p) => p.connect()));
  const failed = results
    .map((r, i) => (r.status === 'rejected' ? `${panes[i].label}: ${(r.reason as Error)?.message || r.reason}` : null))
    .filter(Boolean);
  if (failed.length) showError('connect issue — ' + failed.join('  |  '));
  if (results.every((r) => r.status === 'rejected')) return;

  els.stop.hidden = false;

  // Identical turn cadence against BOTH panes.
  const rate = clampRate(parseInt(els.turnRate.value, 10) || 20);
  const periodMs = Math.max(5, Math.round(1000 / rate));
  turnTimer = window.setInterval(() => {
    const turn = turnCounter++;
    for (const p of panes) void p.fireTurn(turn, () => msgIdCounter++);
  }, periodMs);

  // UI repaint at 5 Hz — readouts + freeze badges + ages.
  uiTimer = window.setInterval(() => {
    const now = performance.now();
    for (const p of panes) p.render(now);
  }, 200);
}

function stop(): void {
  if (turnTimer != null) { clearInterval(turnTimer); turnTimer = null; }
  if (uiTimer != null) { clearInterval(uiTimer); uiTimer = null; }
  for (const p of panes) p.close();
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

function clampRate(n: number): number { return Math.max(1, Math.min(200, n)); }
function sleep(ms: number): Promise<void> { return new Promise((r) => setTimeout(r, ms)); }
function humanBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

// Pre-fill from last run.
try {
  els.urlA.value = localStorage.getItem('compare.a') ?? '';
  els.urlB.value = localStorage.getItem('compare.b') ?? '';
} catch { /* ignore */ }

// Reference TRACK_FOR_KIND so the constant documents the mapping even
// though dispatch is server-side; keeps the wire contract visible here.
void TRACK_FOR_KIND;
