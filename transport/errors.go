package transport

import "errors"

var (
	// ErrUnsupported is returned when a transport doesn't support an
	// operation. Currently returned when Publish is called twice on
	// the same transport.
	ErrUnsupported = errors.New("quicrtc/transport: operation not supported by this transport")

	// ErrAuth is returned when the peer's auth (slug or token) is
	// rejected during Connect.
	ErrAuth = errors.New("quicrtc/transport: auth failed")

	// ErrIdleTimeout is returned when the session watchdog fires.
	ErrIdleTimeout = errors.New("quicrtc/transport: idle timeout")

	// ErrAlreadyConnected is returned by Connect on a Transport
	// that's already in a connected state.
	ErrAlreadyConnected = errors.New("quicrtc/transport: already connected")

	// ErrNotConnected is returned by Publish / Recv / DataChannel
	// before Connect has succeeded.
	ErrNotConnected = errors.New("quicrtc/transport: not connected")

	// ErrWrongRole is returned when an operation is attempted in the
	// wrong role (e.g., Recv on a publisher-side native transport).
	ErrWrongRole = errors.New("quicrtc/transport: operation not valid for this role")
)
