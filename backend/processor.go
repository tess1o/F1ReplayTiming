package main

import "context"

// SessionProcessor handles schedule + historical session artifact generation.
// Live streaming remains separate (phase 2).
type SessionProcessor interface {
	EnsureSchedule(ctx context.Context, year int) error
	ProcessSession(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error
}

func NewSessionProcessor(mode, dataDir, workerPath, pythonBin string) SessionProcessor {
	switch mode {
	case "", "go":
		return NewGoSessionProcessor(dataDir)
	case "python":
		return nil
	default:
		// Unknown mode: fall back to Go so deployments don't silently disable processing.
		return NewGoSessionProcessor(dataDir)
	}
}
