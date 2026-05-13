package wire

import "encoding/json"

// Hello is the first frame on the control stream. Sent by the
// subscriber; validated by the server in constant time.
//
// Version is the wire-format version string. Servers reject
// mismatched majors. Today the only valid value is "1".
//
// SessionID, when non-empty, asks the server to resume an existing
// session: restore its track set, replay any AUs the subscriber may
// have missed during the disconnect, and continue from there. Empty
// SessionID requests a fresh session; the server allocates a new ID
// and returns it in the SDP response.
//
// Features is the list of optional protocol features the client
// declares support for (e.g. "backpressure", "resume",
// "stream-header-v2"). The server intersects this with its own
// supported set and echoes the intersection in SDP.Features.
// Unknown features are ignored; back-compat with older clients
// that don't send the field.
//
// LastSeenSeq, when present, scopes the resume to a per-track
// continuation point: for each (track name → seq) entry the server
// replays buffered AUs with Seq > seq before the resumed receiver
// starts pumping live AUs. This recovers AUs that were in flight on
// the QUIC wire (or in network queues) at disconnect time but never
// reached the client. If a track is absent from LastSeenSeq the
// server applies its normal park/resume semantics for that track
// (continue from whatever the receiver channel currently holds).
//
// LastSeenSeq is only consulted when SessionID is non-empty and
// matches a live parked session. Older clients that omit the field
// continue to work — they just don't get the replay benefit.
type Hello struct {
	Role        string            `json:"role"`                // "recv" (subscriber) | "send" (publisher, future)
	Slug        string            `json:"slug"`                // shared secret from the share link
	Version     string            `json:"ver"`                 // wire-format version
	SessionID   string            `json:"session,omitempty"`   // resume an existing session
	Features    []string          `json:"features,omitempty"`  // declared client capabilities
	LastSeenSeq map[string]uint32 `json:"last_seen,omitempty"` // per-track resume cursor
}

// SDP is the codec/dimensions advertisement sent by the server in
// response to a valid HELLO. Field names mirror the JS WebCodecs
// VideoDecoderConfig where possible.
//
// SessionID echoes the client's HELLO.SessionID if a resume was
// honored, or carries a freshly-allocated ID when SessionID was
// empty in the HELLO (or didn't match a live session). Clients
// should remember this value to enable resume on reconnect.
//
// Features is the intersection of the client's declared features
// (HELLO.Features) and the server's supported set. Both peers
// should treat features outside this list as unsupported even if
// they had advertised them in HELLO.
type SDP struct {
	Codec     string   `json:"codec"` // e.g. "avc1.42E01F"
	Width     int      `json:"width"`
	Height    int      `json:"height"`
	FPS       int      `json:"fps"`
	ScreenW   float64  `json:"screenW,omitempty"` // optional sender display dims
	ScreenH   float64  `json:"screenH,omitempty"`
	SessionID string   `json:"session,omitempty"`  // assigned/echoed session ID
	Features  []string `json:"features,omitempty"` // negotiated feature subset
}

// CurrentVersion is the wire-format version this build emits.
const CurrentVersion = "1"

// Announce is the payload of a TypeAnnounce control frame. Either side
// of an established session sends one to declare a track they intend
// to publish. The server uses this to learn about subscriber-side
// PublishBack tracks; subscribers use it to learn about new
// server-side tracks added mid-session.
//
// Name uniquely identifies the track within the session. Kind selects
// the receiver's framing expectations. Codec is informational.
type Announce struct {
	Name  string `json:"name"`
	Kind  string `json:"kind"` // matches track.Kind values
	Codec string `json:"codec,omitempty"`
}

// Unannounce is the payload of a TypeUnannounce control frame. The
// peer that previously announced a track sends Unannounce(name) to
// indicate it will not publish more AUs on that track. Receiver
// closes its per-track receive queue after draining buffered AUs.
type Unannounce struct {
	Name string `json:"name"`
}

// Resume is the payload of a TypeResume control frame. On reconnect,
// the client sends Resume{Track, FromSeq} to request that the server
// replay any AUs on the named track with sequence numbers >= FromSeq.
// If the server's replay buffer no longer holds that range, it
// responds with a TypeError frame and the client treats the track as
// freshly subscribed.
type Resume struct {
	TrackName string `json:"track"`
	FromSeq   uint32 `json:"from_seq"`
}

// Backpressure is the payload of a TypeBackpressure control frame.
// The subscriber sends one whenever a track's receive queue fills
// past a configurable threshold (default: 75% full) OR detects loss
// that warrants a fresh keyframe. The publisher receives it and can
// adapt the source rate, emit a keyframe, drop a modality, etc.
//
// Level is a normalized 0-100 indicator: 0 = drained, 100 = full.
// TrackName is the affected track (empty = session-level pressure).
//
// NeedsKeyframe (added in v1.1) signals that the subscriber detected
// a stream-recoverable loss event (P-frame gap, decoder failure) and
// the publisher should emit a fresh keyframe NOW rather than waiting
// for the next natural one. Older v1.0 receivers ignore the field
// (it's an optional JSON field), so this is a backward-compatible
// extension. Used by computer-use / video-streaming agents to cap
// visible-stutter recovery time at ~50ms instead of the GOP cadence.
type Backpressure struct {
	TrackName     string `json:"track,omitempty"`
	Level         uint8  `json:"level"` // 0..100
	NeedsKeyframe bool   `json:"needs_keyframe,omitempty"`
}

func (h Hello) Marshal() ([]byte, error)        { return json.Marshal(h) }
func (s SDP) Marshal() ([]byte, error)          { return json.Marshal(s) }
func (a Announce) Marshal() ([]byte, error)     { return json.Marshal(a) }
func (u Unannounce) Marshal() ([]byte, error)   { return json.Marshal(u) }
func (r Resume) Marshal() ([]byte, error)       { return json.Marshal(r) }
func (b Backpressure) Marshal() ([]byte, error) { return json.Marshal(b) }

func UnmarshalHello(b []byte) (Hello, error) {
	var h Hello
	err := json.Unmarshal(b, &h)
	return h, err
}

func UnmarshalSDP(b []byte) (SDP, error) {
	var s SDP
	err := json.Unmarshal(b, &s)
	return s, err
}

func UnmarshalAnnounce(b []byte) (Announce, error) {
	var a Announce
	err := json.Unmarshal(b, &a)
	return a, err
}

func UnmarshalUnannounce(b []byte) (Unannounce, error) {
	var u Unannounce
	err := json.Unmarshal(b, &u)
	return u, err
}

func UnmarshalResume(b []byte) (Resume, error) {
	var r Resume
	err := json.Unmarshal(b, &r)
	return r, err
}

func UnmarshalBackpressure(b []byte) (Backpressure, error) {
	var bp Backpressure
	err := json.Unmarshal(b, &bp)
	return bp, err
}
