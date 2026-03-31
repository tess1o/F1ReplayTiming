package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"testing/fstest"
	"time"
)

func TestOpenAppliesMigrations(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "f1.db")
	migs := fstest.MapFS{
		"migrations/0001_init.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE test_a (id INTEGER PRIMARY KEY);`)},
		"migrations/0002_next.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE test_b (id INTEGER PRIMARY KEY);`)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := Open(ctx, dbPath, 5000, migs)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	rawDB, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer rawDB.Close()

	var c int
	if err := rawDB.QueryRow(`SELECT COUNT(*) FROM schema_migrations`).Scan(&c); err != nil {
		t.Fatalf("count migrations: %v", err)
	}
	if c != 2 {
		t.Fatalf("expected 2 applied migrations, got %d", c)
	}
}

func TestOpenFailsOnChecksumMismatch(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "f1.db")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	migsA := fstest.MapFS{
		"migrations/0001_init.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE test_same_version (id INTEGER PRIMARY KEY);`)},
	}
	st, err := Open(ctx, dbPath, 5000, migsA)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	_ = st.Close()

	migsB := fstest.MapFS{
		"migrations/0001_init.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE test_same_version (id INTEGER PRIMARY KEY, v TEXT);`)},
	}
	st2, err := Open(ctx, dbPath, 5000, migsB)
	if err == nil {
		_ = st2.Close()
		t.Fatalf("expected checksum mismatch error")
	}
}

func TestOpenRollsBackFailedMigration(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "f1.db")
	migs := fstest.MapFS{
		"migrations/0001_init.sql": &fstest.MapFile{Data: []byte(`CREATE TABLE tx_a (id INTEGER PRIMARY KEY);`)},
		"migrations/0002_bad.sql":  &fstest.MapFile{Data: []byte(`THIS IS INVALID SQL;`)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := Open(ctx, dbPath, 5000, migs)
	if err == nil {
		_ = st.Close()
		t.Fatalf("expected migration failure")
	}

	rawDB, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer rawDB.Close()

	var c int
	err = rawDB.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='tx_a'`).Scan(&c)
	if err != nil {
		t.Fatalf("query sqlite_master: %v", err)
	}
	if c != 0 {
		t.Fatalf("expected tx_a to be rolled back")
	}
}

func TestReplayChunkRoundTrip(t *testing.T) {
	frames := []*ReplayFramePayload{
		{TimestampMs: 1000, Lap: 1, FrameJson: []byte(`{"timestamp":1.0}`)},
		{TimestampMs: 1500, Lap: 1, FrameJson: []byte(`{"timestamp":1.5}`)},
	}
	payload, err := EncodeReplayChunk(CodecProtobufZstd, ReplaySchemaVersion, frames)
	if err != nil {
		t.Fatalf("encode replay chunk: %v", err)
	}
	dec, err := DecodeReplayChunk(payload, CodecProtobufZstd)
	if err != nil {
		t.Fatalf("decode replay chunk: %v", err)
	}
	if len(dec.Frames) != 2 {
		t.Fatalf("expected 2 frames, got %d", len(dec.Frames))
	}
	if string(dec.Frames[1].FrameJson) != `{"timestamp":1.5}` {
		t.Fatalf("unexpected second frame payload")
	}
}

func TestTelemetryChunkRoundTrip(t *testing.T) {
	body := []byte(`{"driver":"VER","lap":10}`)
	payload, err := EncodeTelemetryChunk(CodecProtobufZstd, ReplaySchemaVersion, "VER", 10, body)
	if err != nil {
		t.Fatalf("encode telemetry chunk: %v", err)
	}
	dec, err := DecodeTelemetryChunk(payload, CodecProtobufZstd)
	if err != nil {
		t.Fatalf("decode telemetry chunk: %v", err)
	}
	if dec.DriverAbbr != "VER" || dec.Lap != 10 {
		t.Fatalf("unexpected telemetry metadata")
	}
	if string(dec.PayloadJson) != string(body) {
		t.Fatalf("unexpected telemetry payload body")
	}
}

func TestLiveStreamRunLifecycle(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "f1.db")
	migs := fstest.MapFS{
		"migrations/0001_init.sql": &fstest.MapFile{Data: []byte(`
			CREATE TABLE live_stream_runs (
				id INTEGER PRIMARY KEY AUTOINCREMENT,
				year INTEGER NOT NULL,
				round INTEGER NOT NULL,
				session_type TEXT NOT NULL,
				status TEXT NOT NULL,
				started_at TEXT NOT NULL,
				ended_at TEXT,
				last_error TEXT
			);
		`)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	st, err := Open(ctx, dbPath, 5000, migs)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer st.Close()

	run1, err := st.StartLiveStreamRun(ctx, 2026, 2, "R")
	if err != nil || run1 <= 0 {
		t.Fatalf("start run1: id=%d err=%v", run1, err)
	}
	run2, err := st.StartLiveStreamRun(ctx, 2026, 2, "R")
	if err != nil || run2 <= 0 {
		t.Fatalf("start run2: id=%d err=%v", run2, err)
	}
	if err := st.FinishLiveStreamRun(ctx, run2, "finished", ""); err != nil {
		t.Fatalf("finish run2: %v", err)
	}

	rawDB, err := sql.Open("sqlite", "file:"+dbPath)
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	defer rawDB.Close()

	type statusRow struct {
		ID     int64
		Status string
	}
	rows, err := rawDB.Query(`SELECT id, status FROM live_stream_runs ORDER BY id`)
	if err != nil {
		t.Fatalf("query live runs: %v", err)
	}
	defer rows.Close()
	got := make([]statusRow, 0, 2)
	for rows.Next() {
		var r statusRow
		if err := rows.Scan(&r.ID, &r.Status); err != nil {
			t.Fatalf("scan live run: %v", err)
		}
		got = append(got, r)
	}
	if len(got) != 2 {
		t.Fatalf("expected 2 live runs, got %d", len(got))
	}
	if got[0].Status != "stale" {
		t.Fatalf("expected first run to become stale, got %q", got[0].Status)
	}
	if got[1].Status != "finished" {
		t.Fatalf("expected second run finished, got %q", got[1].Status)
	}
}
