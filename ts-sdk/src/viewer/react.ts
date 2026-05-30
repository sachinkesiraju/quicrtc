/**
 * React hook for quicrtc sessions.
 *
 * Lives in its own subpath so the core SDK doesn't import React.
 * React is a peerDependency; apps that don't use React never pay
 * for it. Apps that do consume:
 *
 *   import { useAgentSession } from 'quicrtc/viewer/react';
 *
 * Implementation note: this file does NOT statically import React.
 * Instead it expects the bundler's tree-shaker to leave the React
 * resolution to the consumer. The hook is typed against the React
 * API surface we use so the consumer's React version determines
 * runtime behavior.
 */

import { AgentSession, ConnectOptions, SessionState } from './index.js';
import type { AccessUnit, RemoteTrack, KindStats } from '../types.js';

// React is a peer dependency, lazily injected so this file compiles
// in the SDK build without @types/react in our devDependencies.
// The shape below is the small subset of React we touch (useState,
// useEffect, useRef, useCallback).
interface ReactLike {
  useState<T>(initial: T | (() => T)): [T, (next: T | ((prev: T) => T)) => void];
  useEffect(fn: () => void | (() => void), deps?: unknown[]): void;
  useRef<T>(initial: T): { current: T };
  useCallback<T>(fn: T, deps: unknown[]): T;
}

let React: ReactLike | null = null;

/** Inject the host's React module. Call once at app startup. */
export function setReact(reactModule: ReactLike): void {
  React = reactModule;
}

export interface AgentSessionHandle {
  state: SessionState;
  tracks: Map<string, RemoteTrack>;
  sessionId: string | undefined;
  lastKindStats: Map<string, KindStats>;
  recv: (trackName: string, signal?: AbortSignal) => Promise<AccessUnit>;
  sendDatagram: (payload: Uint8Array, signal?: AbortSignal) => Promise<void>;
  reconnect: () => Promise<void>;
}

/**
 * useAgentSession opens a quicrtc connection and surfaces its live
 * state as React state. Setting opts to null/undefined closes any
 * existing session.
 *
 * Note: setReact() must be called once at app startup, e.g.
 *   import * as React from 'react';
 *   import { setReact, useAgentSession } from 'quicrtc/viewer/react';
 *   setReact(React);
 */
export function useAgentSession(opts: ConnectOptions | null): AgentSessionHandle {
  if (!React) {
    throw new Error('quicrtc/viewer/react: call setReact(reactModule) once before using the hook');
  }

  const [state, setState] = React.useState<SessionState>('idle');
  const [tracks, setTracks] = React.useState<Map<string, RemoteTrack>>(new Map());
  const [sessionId, setSessionId] = React.useState<string | undefined>(undefined);
  const [kindStats, setKindStats] = React.useState<Map<string, KindStats>>(new Map());
  const sessionRef = React.useRef<AgentSession | null>(null);

  React.useEffect(() => {
    if (!opts) {
      sessionRef.current?.close();
      sessionRef.current = null;
      setState('closed');
      return;
    }
    const session = new AgentSession(opts);
    sessionRef.current = session;

    const onState = (ev: Event) => {
      const e = ev as CustomEvent<{ state: SessionState }>;
      setState(e.detail.state);
      setSessionId(session.sessionId);
    };
    const onTrack = (ev: Event) => {
      const e = ev as CustomEvent<{ track: RemoteTrack }>;
      setTracks((t) => {
        const next = new Map(t);
        next.set(e.detail.track.name, e.detail.track);
        return next;
      });
    };
    const onTrackRemoved = (ev: Event) => {
      const e = ev as CustomEvent<{ name: string }>;
      setTracks((t) => {
        const next = new Map(t);
        next.delete(e.detail.name);
        return next;
      });
    };
    const onKindStats = (ev: Event) => {
      const e = ev as CustomEvent<KindStats>;
      setKindStats((m) => {
        const next = new Map(m);
        next.set(e.detail.kind, e.detail);
        return next;
      });
    };

    session.addEventListener('state-change', onState);
    session.addEventListener('track-added', onTrack);
    session.addEventListener('track-removed', onTrackRemoved);
    session.addEventListener('kind-stats', onKindStats);

    session.connect().catch(() => { /* state listener already saw error */ });

    return () => {
      session.removeEventListener('state-change', onState);
      session.removeEventListener('track-added', onTrack);
      session.removeEventListener('track-removed', onTrackRemoved);
      session.removeEventListener('kind-stats', onKindStats);
      session.close();
    };
  }, [opts]);

  const recv = React.useCallback(
    (trackName: string, signal?: AbortSignal) => {
      const s = sessionRef.current;
      if (!s) return Promise.reject(new Error('session closed'));
      return s.recv(trackName, signal);
    },
    [],
  );
  const sendDatagram = React.useCallback(
    (payload: Uint8Array, signal?: AbortSignal) => {
      const s = sessionRef.current;
      if (!s) return Promise.reject(new Error('session closed'));
      return s.sendDatagram(payload, signal);
    },
    [],
  );
  const reconnect = React.useCallback(async () => {
    const s = sessionRef.current;
    if (!s) return;
    await s.connect();
  }, []);

  return {
    state,
    tracks,
    sessionId,
    lastKindStats: kindStats,
    recv,
    sendDatagram,
    reconnect,
  };
}
