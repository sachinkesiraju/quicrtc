package transport

// Kind identifies which transport implementation a Transport is.
// Since the MoQT-to-native-relay migration there is exactly one
// transport implementation (KindNative); the enum is kept for
// forward compatibility.
type Kind string

const (
	// KindNative is the quicrtc-native transport over QUIC +
	// WebTransport. Used for direct publisher-to-subscriber sessions
	// and for relay topologies (where the relay is just an
	// upstream-aggregating server on the same wire format).
	KindNative Kind = "native"
)
