package main

import (
	"context"
	"embed"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"f1replaytiming/backend/storage"
)

//go:embed migrations/*.sql
var migrationFiles embed.FS

//go:embed pit_loss.json
var pitLossJSON []byte

func openStore(dataDir string) *storage.Store {
	sqlitePath := strings.TrimSpace(os.Getenv("SQLITE_PATH"))
	if sqlitePath == "" {
		sqlitePath = filepath.Join(dataDir, "f1.db")
	}
	if err := os.MkdirAll(filepath.Dir(sqlitePath), 0o755); err != nil {
		log.Fatalf("failed to create sqlite directory (%s): %v", filepath.Dir(sqlitePath), err)
	}
	busyTimeout := 5000
	if raw := strings.TrimSpace(os.Getenv("SQLITE_BUSY_TIMEOUT_MS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			busyTimeout = n
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	st, err := storage.Open(ctx, sqlitePath, busyTimeout, migrationFiles)
	if err != nil {
		log.Fatalf("failed to open sqlite store (%s): %v", sqlitePath, err)
	}
	if err := bootstrapStaticArtifacts(st); err != nil {
		log.Fatalf("failed to bootstrap static artifacts: %v", err)
	}
	return st
}

func bootstrapStaticArtifacts(st *storage.Store) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := st.GetJSONArtifact(ctx, "pit_loss.json"); err == nil {
		return nil
	}
	return st.PutJSONArtifact(ctx, "pit_loss.json", pitLossJSON)
}
