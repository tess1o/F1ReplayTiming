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

//go:embed assets/pit_loss.json
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
	if err := bootstrapStaticArtifacts(st, dataDir); err != nil {
		log.Fatalf("failed to bootstrap static artifacts: %v", err)
	}
	return st
}

func bootstrapStaticArtifacts(st *storage.Store, dataDir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := upsertArtifactFromOverridesOrDefault(
		ctx,
		st,
		"pit_loss.json",
		pitLossJSON,
		[]string{
			strings.TrimSpace(os.Getenv("PIT_LOSS_PATH")),
			filepath.Join(dataDir, "static", "pit_loss.json"),
		},
	); err != nil {
		return err
	}
	return upsertArtifactFromOverridesOrDefault(
		ctx,
		st,
		"circuit_metadata.json",
		embeddedCircuitMetadata,
		[]string{
			strings.TrimSpace(os.Getenv("CIRCUIT_METADATA_PATH")),
			filepath.Join(dataDir, "static", "circuit_metadata.json"),
		},
	)
}

func upsertArtifactFromOverridesOrDefault(ctx context.Context, st *storage.Store, artifactPath string, defaultBody []byte, overridePaths []string) error {
	for _, p := range overridePaths {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		b, err := os.ReadFile(p)
		if err != nil {
			continue
		}
		if len(strings.TrimSpace(string(b))) == 0 {
			continue
		}
		return st.PutJSONArtifact(ctx, artifactPath, b)
	}

	if _, err := st.GetJSONArtifact(ctx, artifactPath); err == nil {
		return nil
	}
	return st.PutJSONArtifact(ctx, artifactPath, defaultBody)
}
