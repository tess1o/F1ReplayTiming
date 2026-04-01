package main

import (
	"sync"
	"time"

	"f1replaytiming/backend/storage"
)

var (
	sessionTypePriority = []string{"R", "Q", "S", "SQ", "FP1", "FP2", "FP3"}
	sessionTypeLabels   = map[string]string{
		"R":   "Race",
		"Q":   "Qualifying",
		"S":   "Sprint",
		"SQ":  "Sprint Qualifying",
		"FP1": "Practice 1",
		"FP2": "Practice 2",
		"FP3": "Practice 3",
	}
	availableSeasons = []int{2024, 2025, 2026, 2027, 2028}
)

type app struct {
	dataDir         string
	processor       SessionProcessor
	store           *storage.Store
	replayChunkSize int
	replayCache     *replayCache
	downloads       *downloadManager
	staticData      *staticDataRefresher
	allowedOrigins  map[string]struct{}
	authEnabled     bool
	authPassphrase  string
	sessionLockMu   sync.Mutex
	sessionLocks    map[string]*sync.Mutex
	scheduleLockMu  sync.Mutex
	scheduleLocks   map[int]*sync.Mutex
	pitLossMu       sync.Mutex
	pitLossRaw      map[string]any
	pitLossLoadedAt time.Time
}

type replayCache struct {
	mu       sync.Mutex
	entries  map[string]*replayCacheEntry
	maxBytes int64
	ttl      time.Duration
	current  int64
}

type replayCacheEntry struct {
	key         string
	sizeBytes   int64
	sessionID   int64
	frames      []replayFrameMeta
	totalLaps   int
	totalTime   float64
	qualiPhases []map[string]any
	clients     int
	lastAccess  time.Time
	evictTimer  *time.Timer
}

type replayFrameMeta struct {
	Start        int64   `json:"start,omitempty"`
	End          int64   `json:"end,omitempty"`
	FrameSeq     int     `json:"frame_seq"`
	TimestampMS  int64   `json:"ts_ms"`
	Timestamp    float64 `json:"timestamp"`
	Lap          int     `json:"lap"`
	ChunkSeq     int     `json:"chunk_seq"`
	FrameInChunk int     `json:"frame_in_chunk"`
}

type replayIndexFile struct {
	Version       int               `json:"version"`
	ReplaySize    int64             `json:"replay_size"`
	ReplayModUnix int64             `json:"replay_mod_unix"`
	Frames        []replayFrameMeta `json:"frames"`
	TotalLaps     int               `json:"total_laps"`
	TotalTime     float64           `json:"total_time"`
	QualiPhases   []map[string]any  `json:"quali_phases"`
}

type pitLossValues struct {
	Green float64
	SC    float64
	VSC   float64
}
