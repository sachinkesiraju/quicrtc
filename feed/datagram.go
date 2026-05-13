package feed

import (
	"context"
	"errors"
	"sync"

	"github.com/sachinkesiraju/quicrtc/pubsub"
	"github.com/sachinkesiraju/quicrtc/wire"
)

// DatagramSender is the transport hook the datagram pump writes to.
// The high-level server adapts webtransport.Session.SendDatagram into
// this interface so the pump remains testable with an in-memory fake.
type DatagramSender interface {
	// SendDatagram sends one datagram. May return an error for
	// "datagram too large" (quic-go DatagramTooLargeError) or for
	// transport failure. Implementations are expected to be
	// non-blocking; quic-go's datagram queue is bounded internally.
	SendDatagram(payload []byte) error
}

// ErrDatagramSenderNil indicates the pump was started without a
// DatagramSender — Config.DatagramSender must be set when
// Delivery=DeliveryDatagramOrStream.
var ErrDatagramSenderNil = errors.New("feed: DatagramSender required for DeliveryDatagramOrStream")

// datagramBufPool reuses encode scratch space.
var datagramBufPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, wire.MaxDatagramPayload+16)
		return &b
	},
}

// runDatagramOrStream is the datagram-first pump. Each AU is sent as
// one datagram with the 4-byte envelope. Oversized AUs are dropped
// (datagrams are not fragmentable). Backpressure is the QUIC datagram
// queue depth — if SendDatagram returns an error we drop the AU and
// keep going. The hybrid "fall back to a low-priority stream when
// the datagram queue saturates" path is not yet implemented; users
// who need >500 Mbps of datagram throughput should monitor
// OnAUDropped("send_failed") and adjust source rate.
func (p *Pump) runDatagramOrStream(ctx context.Context, recv *pubsub.Receiver) error {
	if p.cfg.DatagramSender == nil {
		return ErrDatagramSenderNil
	}
	trackID := p.cfg.TrackID
	var seq uint16

	frames := recv.Frames()
	for {
		var au pubsub.AccessUnit
		select {
		case <-ctx.Done():
			return ctx.Err()
		case a, ok := <-frames:
			if !ok {
				return nil
			}
			au = a
		}

		// Encode envelope + payload into a pooled buffer.
		bufp := datagramBufPool.Get().(*[]byte)
		buf := (*bufp)[:0]

		encoded, err := wire.EncodeDatagram(buf, trackID, seq, au.Bytes)
		if err != nil {
			// Oversize — drop, surface via metric.
			if p.cfg.OnAUDropped != nil {
				p.cfg.OnAUDropped("datagram_too_large")
			}
			*bufp = buf[:0]
			datagramBufPool.Put(bufp)
			continue
		}

		if err := p.cfg.DatagramSender.SendDatagram(encoded); err != nil {
			// quic-go DatagramTooLargeError or transport closed. Drop.
			if p.cfg.OnAUDropped != nil {
				p.cfg.OnAUDropped("send_failed")
			}
			*bufp = buf[:0]
			datagramBufPool.Put(bufp)
			continue
		}

		if p.cfg.OnWriteBytes != nil {
			p.cfg.OnWriteBytes(len(encoded))
		}

		seq++
		*bufp = buf[:0]
		datagramBufPool.Put(bufp)
	}
}
