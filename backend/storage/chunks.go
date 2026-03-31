package storage

import (
	"fmt"
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/klauspost/compress/zstd"
)

type ReplayFramePayload struct {
	TimestampMs int64  `protobuf:"varint,1,opt,name=timestamp_ms,json=timestampMs,proto3"`
	Lap         int32  `protobuf:"varint,2,opt,name=lap,proto3"`
	FrameJson   []byte `protobuf:"bytes,3,opt,name=frame_json,json=frameJson,proto3"`
}

func (m *ReplayFramePayload) Reset()         { *m = ReplayFramePayload{} }
func (m *ReplayFramePayload) String() string { return proto.CompactTextString(m) }
func (*ReplayFramePayload) ProtoMessage()    {}

type ReplayChunkPayload struct {
	SchemaVersion uint32                `protobuf:"varint,1,opt,name=schema_version,json=schemaVersion,proto3"`
	Frames        []*ReplayFramePayload `protobuf:"bytes,2,rep,name=frames,proto3"`
}

func (m *ReplayChunkPayload) Reset()         { *m = ReplayChunkPayload{} }
func (m *ReplayChunkPayload) String() string { return proto.CompactTextString(m) }
func (*ReplayChunkPayload) ProtoMessage()    {}

type TelemetryChunkPayload struct {
	SchemaVersion uint32 `protobuf:"varint,1,opt,name=schema_version,json=schemaVersion,proto3"`
	DriverAbbr    string `protobuf:"bytes,2,opt,name=driver_abbr,json=driverAbbr,proto3"`
	Lap           int32  `protobuf:"varint,3,opt,name=lap,proto3"`
	PayloadJson   []byte `protobuf:"bytes,4,opt,name=payload_json,json=payloadJson,proto3"`
}

func (m *TelemetryChunkPayload) Reset()         { *m = TelemetryChunkPayload{} }
func (m *TelemetryChunkPayload) String() string { return proto.CompactTextString(m) }
func (*TelemetryChunkPayload) ProtoMessage()    {}

var (
	encOnce sync.Once
	encInst *zstd.Encoder
	encErr  error

	decOnce sync.Once
	decInst *zstd.Decoder
	decErr  error
)

func zstdEncoder() (*zstd.Encoder, error) {
	encOnce.Do(func() {
		encInst, encErr = zstd.NewWriter(nil)
	})
	return encInst, encErr
}

func zstdDecoder() (*zstd.Decoder, error) {
	decOnce.Do(func() {
		decInst, decErr = zstd.NewReader(nil)
	})
	return decInst, decErr
}

func EncodeReplayChunk(codec string, schemaVersion uint32, frames []*ReplayFramePayload) ([]byte, error) {
	chunk := &ReplayChunkPayload{
		SchemaVersion: schemaVersion,
		Frames:        frames,
	}
	raw, err := proto.Marshal(chunk)
	if err != nil {
		return nil, err
	}
	if codec == CodecProtobuf {
		return raw, nil
	}
	if codec != CodecProtobufZstd {
		return nil, fmt.Errorf("unsupported replay codec %q", codec)
	}
	enc, err := zstdEncoder()
	if err != nil {
		return nil, err
	}
	return enc.EncodeAll(raw, make([]byte, 0, len(raw))), nil
}

func DecodeReplayChunk(payload []byte, codec string) (*ReplayChunkPayload, error) {
	var raw []byte
	if codec == CodecProtobuf {
		raw = payload
	} else if codec == CodecProtobufZstd {
		dec, err := zstdDecoder()
		if err != nil {
			return nil, err
		}
		var err2 error
		raw, err2 = dec.DecodeAll(payload, nil)
		if err2 != nil {
			return nil, err2
		}
	} else {
		return nil, fmt.Errorf("unsupported replay codec %q", codec)
	}
	var chunk ReplayChunkPayload
	if err := proto.Unmarshal(raw, &chunk); err != nil {
		return nil, err
	}
	if chunk.Frames == nil {
		chunk.Frames = []*ReplayFramePayload{}
	}
	return &chunk, nil
}

func EncodeTelemetryChunk(codec string, schemaVersion uint32, driver string, lap int, payloadJSON []byte) ([]byte, error) {
	chunk := &TelemetryChunkPayload{
		SchemaVersion: schemaVersion,
		DriverAbbr:    driver,
		Lap:           int32(lap),
		PayloadJson:   payloadJSON,
	}
	raw, err := proto.Marshal(chunk)
	if err != nil {
		return nil, err
	}
	if codec == CodecProtobuf {
		return raw, nil
	}
	if codec != CodecProtobufZstd {
		return nil, fmt.Errorf("unsupported telemetry codec %q", codec)
	}
	enc, err := zstdEncoder()
	if err != nil {
		return nil, err
	}
	return enc.EncodeAll(raw, make([]byte, 0, len(raw))), nil
}

func DecodeTelemetryChunk(payload []byte, codec string) (*TelemetryChunkPayload, error) {
	var raw []byte
	if codec == CodecProtobuf {
		raw = payload
	} else if codec == CodecProtobufZstd {
		dec, err := zstdDecoder()
		if err != nil {
			return nil, err
		}
		var err2 error
		raw, err2 = dec.DecodeAll(payload, nil)
		if err2 != nil {
			return nil, err2
		}
	} else {
		return nil, fmt.Errorf("unsupported telemetry codec %q", codec)
	}
	var out TelemetryChunkPayload
	if err := proto.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if out.PayloadJson == nil {
		out.PayloadJson = []byte{}
	}
	return &out, nil
}
