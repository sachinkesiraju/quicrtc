/**
 * QUICRTC TypeScript Client.
 * AI real-time streaming over QUIC + WebTransport.
 */

export {
  QuicRTCClient,
  DataChannel,
  TrackReceiver,
  TrackSender,
  ClientError,
  ConnectionState,
} from './client.js';
export type { QuicRTCClientOptions } from './client.js';

export {
  TrackKind,
  FrameType,
  FrameFlags,
  DefaultClientFeatures,
  DatagramHeaderLen,
  TypeDatagramAU,
  MaxDatagramPayload,
} from './types.js';
export type {
  AccessUnit,
  LocalTrack,
  RemoteTrack,
  SDP,
  Hello,
  Announce,
  Unannounce,
  Resume,
  Backpressure,
  ClientOptions,
  TransportStats,
} from './types.js';

export {
  readControlFrame,
  writeControlFrame,
  readFeedFrame,
  writeFeedFrame,
  writeStreamHeader,
  peekStreamHeader,
  marshalHello,
  unmarshalHello,
  marshalSDP,
  unmarshalSDP,
  marshalAnnounce,
  unmarshalAnnounce,
  marshalUnannounce,
  unmarshalUnannounce,
  marshalResume,
  unmarshalResume,
  marshalBackpressure,
  unmarshalBackpressure,
  encodeDatagram,
  decodeDatagram,
  BufferedReader,
  FrameTooLargeError,
  DatagramTooLargeError,
} from './wire.js';
export type { DecodedDatagram } from './wire.js';
