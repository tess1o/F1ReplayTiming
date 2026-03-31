package main

import (
	"context"

	"f1replaytiming/backend/storage"
)

// SessionProcessor handles schedule + historical session artifact generation.
// Live streaming remains separate (phase 2).
type SessionProcessor interface {
	EnsureSchedule(ctx context.Context, year int) error
	ProcessSession(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error
}

func NewSessionProcessor(dataDir string, store *storage.Store, replayChunkFrames, telemetryChunkSamples int) SessionProcessor {
	return NewGoSessionProcessor(dataDir, store, replayChunkFrames, telemetryChunkSamples)
}
