package storage

import "database/sql"

const (
	ReplaySchemaVersion = 1
	CodecProtobufZstd   = "protobuf+zstd"
	CodecProtobuf       = "protobuf"
)

type Store struct {
	db *sql.DB
}

type MigrationFile struct {
	Version  int
	Path     string
	Checksum string
	SQL      string
}

type ReplayFrameIndexRow struct {
	FrameSeq     int
	TimestampMS  int64
	Lap          int
	ChunkSeq     int
	FrameInChunk int
}

type ReplayChunkRow struct {
	ChunkSeq   int
	StartTSMS  int64
	EndTSMS    int64
	FrameCount int
	Codec      string
	Payload    []byte
}

type ReplayMeta struct {
	SessionID   int64
	TotalLaps   int
	TotalTime   float64
	QualiPhases []map[string]any
	Frames      []ReplayFrameIndexRow
}

type ReplayWriteTx struct {
	tx        *sql.Tx
	sessionID int64
	closed    bool
}

type TelemetryChunkRow struct {
	DriverAbbr string
	Lap        int
	ChunkSeq   int
	Codec      string
	Payload    []byte
}

type TelemetryWriteTx struct {
	tx        *sql.Tx
	sessionID int64
	closed    bool
}
