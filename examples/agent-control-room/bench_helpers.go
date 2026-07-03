package main

import (
	"encoding/json"

	"github.com/sachinkesiraju/quicrtc/examples/internal/status"
	"github.com/sachinkesiraju/quicrtc/pubsub"
)

func statusNew() *status.Status { return status.New("bench") }

func jsonMarshal(v any) ([]byte, error) { return json.Marshal(v) }

func pubsubAccessUnit(body []byte, atMicro int64) pubsub.AccessUnit {
	return pubsub.AccessUnit{
		Bytes: body, Keyframe: true,
		PTSMicro: uint64(atMicro), Seq: uint32(atMicro),
	}
}
