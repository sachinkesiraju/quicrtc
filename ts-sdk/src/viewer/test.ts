/**
 * Smoke test for the viewer module. Runs in Node and exercises:
 *   - AgentSession EventTarget dispatch shape
 *   - Typed event listener registration via `on(s, 'state-change', ...)`
 *   - AbortSignal-aware recv rejection
 *
 * Wire is not exercised here; full wire round-trip is covered by
 * the existing wire.test.ts and the Go integration tests. This file
 * only exists to keep the viewer ergonomics layer honest.
 */

import { AgentSession, on } from './index.js';
import { QuicRTCClient } from '../client.js';

let failures = 0;
function assert(cond: boolean, msg: string): void {
  if (!cond) { console.error('FAIL:', msg); failures++; }
}

async function main(): Promise<void> {
  // EventTarget dispatch and typed listener.
  const s = new AgentSession({ url: 'https://example.invalid/wt', slug: 'x' });
  let stateSeen = false;
  const off = on(s, 'state-change', (ev) => {
    if (ev.detail.state === 'closed') stateSeen = true;
  });
  s.close();
  assert(stateSeen, 'on(state-change) should fire on close');
  off();

  // AbortSignal: already-aborted controller rejects recv.
  const ac = new AbortController();
  ac.abort();
  const s2 = new AgentSession({ url: 'https://example.invalid/wt', slug: 'x' });
  let rejected = false;
  try {
    await s2.recv('any', ac.signal);
  } catch (err) {
    rejected = err instanceof DOMException && err.name === 'AbortError';
  }
  assert(rejected, 'recv with aborted signal must reject AbortError');
  s2.close();

  // The viewer wires these client hooks; guard they exist on the public surface.
  const c = new QuicRTCClient();
  for (const m of ['onAnnounce', 'onUnannounce', 'onKindStats', 'onTrack', 'onStateChange', 'receiveDatagram']) {
    assert(typeof (c as unknown as Record<string, unknown>)[m] === 'function', `client.${m} should exist`);
  }

  // Lazy 'au'/'datagram' push listeners attach without throwing pre-connect.
  const s3 = new AgentSession({ url: 'https://example.invalid/wt', slug: 'x' });
  const offAu = on(s3, 'au', () => { /* push consumer */ });
  const offDg = on(s3, 'datagram', () => { /* push consumer */ });
  offAu();
  offDg();
  s3.close();
  assert(true, 'au/datagram listeners attach pre-connect without throwing');

  // recv() aborted mid-flight rejects AbortError (the client removes the
  // waiter, so no later AU is silently swallowed by an orphaned recv).
  const s4 = new AgentSession({ url: 'https://example.invalid/wt', slug: 'x' });
  const ac2 = new AbortController();
  const pending = s4.recv('vid', ac2.signal);
  ac2.abort();
  let midAbort = false;
  try { await pending; } catch (err) { midAbort = err instanceof DOMException && err.name === 'AbortError'; }
  assert(midAbort, 'recv aborted mid-flight rejects AbortError');
  s4.close();

  if (failures > 0) {
    console.error(`viewer test failed: ${failures} assertion(s)`);
    process.exit(1);
  }
  console.log('viewer test passed');
}

main().catch((err) => { console.error(err); process.exit(1); });
