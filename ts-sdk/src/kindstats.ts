// kindstats.ts — subscriber-side per-Kind receive observability for
// the "kind-stats" feature. Mirrors client/kindstats.go on the Go
// side: accumulate publish→recv latency samples plus sequence-gap
// drop counts per track Kind, then snapshot them ~1 Hz into
// wire.KindStats reports the publisher consumes.

import { KindStats } from './types.js';

/** Cap the per-Kind latency window so a fast track can't grow the
 * sample array without bound if the emit timer stalls. */
const KIND_STATS_MAX_SAMPLES = 4096;

interface KindAccumulator {
  latMs: number[];
  lastSeq: number;
  haveSeq: boolean;
  dropped: number;
}

/**
 * KindStatsCollector accumulates received-AU observations per Kind and
 * turns them into KindStats snapshots. Single-threaded (JS event
 * loop), so no locking is needed.
 */
export class KindStatsCollector {
  private acc = new Map<string, KindAccumulator>();

  /**
   * observe records one received AU under its track Kind. recvWallMs
   * is the local receive wall-clock (ms); pubWallMicro is the
   * publisher's stamp in Unix micros (undefined when absent — then no
   * latency sample is recorded). seq drives drop detection via gaps in
   * the per-Kind monotonic sequence.
   */
  observe(kind: string, seq: number, pubWallMicro: bigint | undefined, recvWallMs: number): void {
    let a = this.acc.get(kind);
    if (!a) {
      a = { latMs: [], lastSeq: 0, haveSeq: false, dropped: 0 };
      this.acc.set(kind, a);
    }
    if (a.haveSeq && seq > a.lastSeq + 1) {
      a.dropped += seq - a.lastSeq - 1;
    }
    if (!a.haveSeq || seq > a.lastSeq) {
      a.lastSeq = seq;
      a.haveSeq = true;
    }
    if (pubWallMicro !== undefined && a.latMs.length < KIND_STATS_MAX_SAMPLES) {
      const lat = recvWallMs - Number(pubWallMicro) / 1000;
      if (lat >= 0) a.latMs.push(lat);
    }
  }

  /**
   * snapshot returns one KindStats per Kind that saw activity and
   * resets the per-tick latency window. last_seq and dropped are
   * cumulative and preserved across ticks.
   */
  snapshot(): KindStats[] {
    const out: KindStats[] = [];
    for (const [kind, a] of this.acc) {
      const ks: KindStats = {
        kind,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        last_seq: a.lastSeq,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        recv_p50_ms: 0,
        // eslint-disable-next-line @typescript-eslint/naming-convention
        recv_p99_ms: 0,
        dropped: a.dropped,
      };
      if (a.latMs.length > 0) {
        a.latMs.sort((x, y) => x - y);
        ks.recv_p50_ms = Math.round(percentile(a.latMs, 0.5));
        ks.recv_p99_ms = Math.round(percentile(a.latMs, 0.99));
        a.latMs.length = 0;
      }
      out.push(ks);
    }
    return out;
  }
}

/** percentile returns the p-quantile (0..1) of an ascending array
 * using nearest-rank. sorted must be non-empty. */
export function percentile(sorted: number[], p: number): number {
  if (sorted.length === 1) return sorted[0];
  let idx = Math.round(p * (sorted.length - 1));
  if (idx < 0) idx = 0;
  if (idx >= sorted.length) idx = sorted.length - 1;
  return sorted[idx];
}
