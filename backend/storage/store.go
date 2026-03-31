package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

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

type SessionKey struct {
	Year        int
	Round       int
	SessionType string
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

func Open(ctx context.Context, sqlitePath string, busyTimeoutMS int, migrations fs.FS) (*Store, error) {
	if strings.TrimSpace(sqlitePath) == "" {
		return nil, errors.New("sqlite path is required")
	}
	dsn := fmt.Sprintf("file:%s", sqlitePath)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := pingWithContext(ctx, db); err != nil {
		_ = db.Close()
		return nil, err
	}

	s := &Store{db: db}
	if err := s.applyPragmas(ctx, busyTimeoutMS); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := s.runMigrations(ctx, migrations); err != nil {
		_ = db.Close()
		return nil, err
	}
	return s, nil
}

func pingWithContext(ctx context.Context, db *sql.DB) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return db.PingContext(ctx)
}

func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *Store) applyPragmas(ctx context.Context, busyTimeoutMS int) error {
	if busyTimeoutMS <= 0 {
		busyTimeoutMS = 5000
	}
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
		fmt.Sprintf("PRAGMA busy_timeout = %d", busyTimeoutMS),
	}
	for _, q := range pragmas {
		if _, err := s.db.ExecContext(ctx, q); err != nil {
			return fmt.Errorf("apply pragma %q: %w", q, err)
		}
	}
	return nil
}

func (s *Store) runMigrations(ctx context.Context, migFS fs.FS) error {
	if migFS == nil {
		return errors.New("migrations filesystem is nil")
	}
	migs, err := loadMigrations(migFS)
	if err != nil {
		return err
	}
	if len(migs) == 0 {
		return errors.New("no migration files found")
	}

	conn, err := s.db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return fmt.Errorf("begin immediate migration txn: %w", err)
	}
	rollback := func(e error) error {
		_, _ = conn.ExecContext(context.Background(), "ROLLBACK")
		return e
	}

	if _, err := conn.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			checksum TEXT NOT NULL,
			applied_at TEXT NOT NULL
		)
	`); err != nil {
		return rollback(err)
	}

	existing := map[int]string{}
	rows, err := conn.QueryContext(ctx, "SELECT version, checksum FROM schema_migrations ORDER BY version")
	if err != nil {
		return rollback(err)
	}
	for rows.Next() {
		var v int
		var checksum string
		if err := rows.Scan(&v, &checksum); err != nil {
			rows.Close()
			return rollback(err)
		}
		existing[v] = checksum
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return rollback(err)
	}
	rows.Close()

	for _, m := range migs {
		if appliedChecksum, ok := existing[m.Version]; ok {
			if appliedChecksum != m.Checksum {
				return rollback(fmt.Errorf("migration checksum mismatch for version %d", m.Version))
			}
			continue
		}
		if _, err := conn.ExecContext(ctx, m.SQL); err != nil {
			return rollback(fmt.Errorf("apply migration %d (%s): %w", m.Version, m.Path, err))
		}
		if _, err := conn.ExecContext(ctx,
			"INSERT INTO schema_migrations(version, checksum, applied_at) VALUES(?, ?, ?)",
			m.Version,
			m.Checksum,
			time.Now().UTC().Format(time.RFC3339Nano),
		); err != nil {
			return rollback(err)
		}
	}

	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return rollback(err)
	}
	return nil
}

func loadMigrations(migFS fs.FS) ([]MigrationFile, error) {
	matches, err := fs.Glob(migFS, "migrations/*.sql")
	if err != nil {
		return nil, err
	}
	if len(matches) == 0 {
		matches, err = fs.Glob(migFS, "*.sql")
		if err != nil {
			return nil, err
		}
	}
	migs := make([]MigrationFile, 0, len(matches))
	for _, path := range matches {
		base := filepath.Base(path)
		parts := strings.SplitN(base, "_", 2)
		if len(parts) < 2 {
			return nil, fmt.Errorf("invalid migration filename %q", base)
		}
		v, err := strconv.Atoi(parts[0])
		if err != nil {
			return nil, fmt.Errorf("invalid migration version in %q", base)
		}
		raw, err := fs.ReadFile(migFS, path)
		if err != nil {
			return nil, err
		}
		hash := sha256.Sum256(raw)
		migs = append(migs, MigrationFile{
			Version:  v,
			Path:     path,
			Checksum: hex.EncodeToString(hash[:]),
			SQL:      string(raw),
		})
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].Version < migs[j].Version })
	for i := 1; i < len(migs); i++ {
		if migs[i].Version == migs[i-1].Version {
			return nil, fmt.Errorf("duplicate migration version %d", migs[i].Version)
		}
	}
	return migs, nil
}

func (s *Store) PutJSONArtifact(ctx context.Context, path string, body []byte) error {
	path = strings.TrimSpace(path)
	if path == "" {
		return errors.New("artifact path is empty")
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO json_artifacts(path, body, updated_at)
		VALUES(?, ?, ?)
		ON CONFLICT(path) DO UPDATE SET body=excluded.body, updated_at=excluded.updated_at
	`, path, body, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) GetJSONArtifact(ctx context.Context, path string) ([]byte, error) {
	var body []byte
	err := s.db.QueryRowContext(ctx, "SELECT body FROM json_artifacts WHERE path = ?", path).Scan(&body)
	if err != nil {
		return nil, err
	}
	return body, nil
}

func (s *Store) UpsertSchedule(ctx context.Context, year int, payload map[string]any) error {
	b, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO seasons(year, payload_json, updated_at)
		VALUES(?, ?, ?)
		ON CONFLICT(year) DO UPDATE SET payload_json=excluded.payload_json, updated_at=excluded.updated_at
	`, year, b, updatedAt); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM event_sessions WHERE event_id IN (SELECT id FROM events WHERE year = ?)", year); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM events WHERE year = ?", year); err != nil {
		return err
	}

	events, _ := payload["events"].([]any)
	for _, eAny := range events {
		e, ok := eAny.(map[string]any)
		if !ok {
			continue
		}
		res, err := tx.ExecContext(ctx, `
			INSERT INTO events(year, round_number, country, event_name, location, event_date, status)
			VALUES(?, ?, ?, ?, ?, ?, ?)
		`,
			year,
			asInt(e["round_number"]),
			asString(e["country"]),
			asString(e["event_name"]),
			asString(e["location"]),
			asString(e["event_date"]),
			asString(e["status"]),
		)
		if err != nil {
			return err
		}
		eventID, _ := res.LastInsertId()
		sessions, _ := e["sessions"].([]any)
		for _, sAny := range sessions {
			sm, ok := sAny.(map[string]any)
			if !ok {
				continue
			}
			if _, err := tx.ExecContext(ctx, `
				INSERT INTO event_sessions(event_id, name, session_type, date_utc, available, download_state, downloaded, last_error, updated_at)
				VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)
			`,
				eventID,
				asString(sm["name"]),
				asString(sm["session_type"]),
				asString(sm["date_utc"]),
				boolToInt(asBool(sm["available"])),
				asString(sm["download_state"]),
				boolToInt(asBool(sm["downloaded"])),
				asString(sm["last_error"]),
				asString(sm["updated_at"]),
			); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}

func (s *Store) UpsertSessionInfo(ctx context.Context, year, round int, sessionType string, info map[string]any) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() {
		_ = tx.Rollback()
	}()
	updatedAt := time.Now().UTC().Format(time.RFC3339Nano)
	_, err = tx.ExecContext(ctx, `
		INSERT INTO sessions(year, round, session_type, event_name, circuit, country, ready, updated_at)
		VALUES(?, ?, ?, ?, ?, ?, 0, ?)
		ON CONFLICT(year, round, session_type) DO UPDATE SET
			event_name=excluded.event_name,
			circuit=excluded.circuit,
			country=excluded.country,
			updated_at=excluded.updated_at
	`, year, round, strings.ToUpper(strings.TrimSpace(sessionType)), asString(info["event_name"]), asString(info["circuit"]), asString(info["country"]), updatedAt)
	if err != nil {
		return 0, err
	}
	var sessionID int64
	err = tx.QueryRowContext(ctx, "SELECT id FROM sessions WHERE year = ? AND round = ? AND session_type = ?", year, round, strings.ToUpper(strings.TrimSpace(sessionType))).Scan(&sessionID)
	if err != nil {
		return 0, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM drivers WHERE session_id = ?", sessionID); err != nil {
		return 0, err
	}
	drivers, _ := info["drivers"].([]any)
	for _, dAny := range drivers {
		d, ok := dAny.(map[string]any)
		if !ok {
			continue
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO drivers(session_id, abbreviation, driver_number, full_name, team_name, team_color)
			VALUES(?, ?, ?, ?, ?, ?)
		`, sessionID, asString(d["abbreviation"]), asString(d["driver_number"]), asString(d["full_name"]), asString(d["team_name"]), asString(d["team_color"])); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return sessionID, nil
}

func (s *Store) GetSessionID(ctx context.Context, year, round int, sessionType string) (int64, error) {
	var sessionID int64
	err := s.db.QueryRowContext(ctx, `SELECT id FROM sessions WHERE year = ? AND round = ? AND session_type = ?`, year, round, strings.ToUpper(strings.TrimSpace(sessionType))).Scan(&sessionID)
	if err != nil {
		return 0, err
	}
	return sessionID, nil
}

func (s *Store) ReplaceResults(ctx context.Context, sessionID int64, results []map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM results WHERE session_id = ?", sessionID); err != nil {
		return err
	}
	for _, r := range results {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO results(session_id, position, driver, abbreviation, team, team_color, grid_position, status, points, fastest_lap, gap_to_leader)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, sessionID, nilIfZeroInt(asInt(r["position"])), asString(r["driver"]), asString(r["abbreviation"]), asString(r["team"]), asString(r["team_color"]), nilIfZeroInt(asInt(r["grid_position"])), asString(r["status"]), asFloat(r["points"]), asString(r["fastest_lap"]), asString(r["gap_to_leader"]))
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE sessions SET updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ReplaceLaps(ctx context.Context, sessionID int64, laps []map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM laps WHERE session_id = ?", sessionID); err != nil {
		return err
	}
	for _, l := range laps {
		_, err := tx.ExecContext(ctx, `
			INSERT INTO laps(session_id, driver, lap_number, position, lap_time, time_seconds, sector1, sector2, sector3, compound, tyre_life, pit_in, pit_out)
			VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`,
			sessionID,
			asString(l["driver"]),
			asInt(l["lap_number"]),
			nilIfZeroInt(asInt(l["position"])),
			asString(l["lap_time"]),
			asFloatPtr(l["time"]),
			asString(l["sector1"]),
			asString(l["sector2"]),
			asString(l["sector3"]),
			asString(l["compound"]),
			nilIfZeroInt(asInt(l["tyre_life"])),
			boolToInt(asBool(l["pit_in"])),
			boolToInt(asBool(l["pit_out"])),
		)
		if err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE sessions SET updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ReplaceTrack(ctx context.Context, sessionID int64, track map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	norm, _ := track["norm"].(map[string]any)
	trackPts, _ := json.Marshal(track["track_points"])
	sectorBoundaries, _ := json.Marshal(track["sector_boundaries"])
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO track(session_id, rotation, circuit_name, x_min, y_min, scale, track_points_json, sector_boundaries_json)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(session_id) DO UPDATE SET
			rotation=excluded.rotation,
			circuit_name=excluded.circuit_name,
			x_min=excluded.x_min,
			y_min=excluded.y_min,
			scale=excluded.scale,
			track_points_json=excluded.track_points_json,
			sector_boundaries_json=excluded.sector_boundaries_json
	`, sessionID, asFloat(track["rotation"]), asString(track["circuit_name"]), asFloat(norm["x_min"]), asFloat(norm["y_min"]), asFloat(norm["scale"]), trackPts, sectorBoundaries); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM track_corners WHERE session_id = ?", sessionID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM track_marshal_sectors WHERE session_id = ?", sessionID); err != nil {
		return err
	}
	corners, _ := track["corners"].([]map[string]any)
	if corners == nil {
		if arr, ok := track["corners"].([]any); ok {
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					corners = append(corners, m)
				}
			}
		}
	}
	for idx, c := range corners {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO track_corners(session_id, corner_order, x, y, number, letter, angle)
			VALUES(?, ?, ?, ?, ?, ?, ?)
		`, sessionID, idx, asFloat(c["x"]), asFloat(c["y"]), nilIfZeroInt(asInt(c["number"])), asString(c["letter"]), asFloat(c["angle"])); err != nil {
			return err
		}
	}
	marshal, _ := track["marshal_sectors"].([]map[string]any)
	if marshal == nil {
		if arr, ok := track["marshal_sectors"].([]any); ok {
			for _, item := range arr {
				if m, ok := item.(map[string]any); ok {
					marshal = append(marshal, m)
				}
			}
		}
	}
	for idx, m := range marshal {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO track_marshal_sectors(session_id, sector_order, x, y, number)
			VALUES(?, ?, ?, ?, ?)
		`, sessionID, idx, asFloat(m["x"]), asFloat(m["y"]), nilIfZeroInt(asInt(m["number"]))); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE sessions SET updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	if err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) BeginReplayWrite(ctx context.Context, sessionID int64, schemaVersion int) (*ReplayWriteTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM replay_frame_index WHERE session_id = ?", sessionID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM replay_chunks WHERE session_id = ?", sessionID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "UPDATE sessions SET ready = 0, replay_schema_version = ?, total_laps = 0, total_time = 0 WHERE id = ?", schemaVersion, sessionID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return &ReplayWriteTx{tx: tx, sessionID: sessionID}, nil
}

type ReplayWriteTx struct {
	tx        *sql.Tx
	sessionID int64
	closed    bool
}

func (w *ReplayWriteTx) InsertChunk(ctx context.Context, row ReplayChunkRow) error {
	if w.closed {
		return errors.New("replay writer is closed")
	}
	_, err := w.tx.ExecContext(ctx, `
		INSERT INTO replay_chunks(session_id, chunk_seq, start_ts_ms, end_ts_ms, frame_count, codec, payload_blob, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?, ?)
	`, w.sessionID, row.ChunkSeq, row.StartTSMS, row.EndTSMS, row.FrameCount, row.Codec, row.Payload, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (w *ReplayWriteTx) InsertFrameIndex(ctx context.Context, row ReplayFrameIndexRow) error {
	if w.closed {
		return errors.New("replay writer is closed")
	}
	_, err := w.tx.ExecContext(ctx, `
		INSERT INTO replay_frame_index(session_id, frame_seq, ts_ms, lap, chunk_seq, frame_in_chunk)
		VALUES(?, ?, ?, ?, ?, ?)
	`, w.sessionID, row.FrameSeq, row.TimestampMS, row.Lap, row.ChunkSeq, row.FrameInChunk)
	return err
}

func (w *ReplayWriteTx) Commit(ctx context.Context, totalLaps int, totalTime float64, qualiPhases []map[string]any) error {
	if w.closed {
		return errors.New("replay writer already closed")
	}
	qRaw, _ := json.Marshal(qualiPhases)
	if _, err := w.tx.ExecContext(ctx, `
		UPDATE sessions
		SET ready = 0,
			total_laps = ?,
			total_time = ?,
			quali_phases_json = ?,
			updated_at = ?
		WHERE id = ?
	`, totalLaps, totalTime, qRaw, time.Now().UTC().Format(time.RFC3339Nano), w.sessionID); err != nil {
		_ = w.tx.Rollback()
		w.closed = true
		return err
	}
	if err := w.tx.Commit(); err != nil {
		w.closed = true
		return err
	}
	w.closed = true
	return nil
}

func (s *Store) SetSessionReady(ctx context.Context, sessionID int64, ready bool) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE sessions
		SET ready = ?, updated_at = ?
		WHERE id = ?
	`, boolToInt(ready), time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	return err
}

func (w *ReplayWriteTx) Rollback() {
	if w == nil || w.closed {
		return
	}
	_ = w.tx.Rollback()
	w.closed = true
}

func (s *Store) ReplaceTelemetry(ctx context.Context, sessionID int64, rows []TelemetryChunkRow) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, "DELETE FROM telemetry_chunks WHERE session_id = ?", sessionID); err != nil {
		return err
	}
	for _, row := range rows {
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO telemetry_chunks(session_id, driver_abbr, lap, chunk_seq, codec, payload_blob, created_at)
			VALUES(?, ?, ?, ?, ?, ?, ?)
		`, sessionID, row.DriverAbbr, row.Lap, row.ChunkSeq, row.Codec, row.Payload, time.Now().UTC().Format(time.RFC3339Nano)); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, "UPDATE sessions SET updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	if err != nil {
		return err
	}
	return tx.Commit()
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

func (s *Store) BeginTelemetryWrite(ctx context.Context, sessionID int64) (*TelemetryWriteTx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM telemetry_chunks WHERE session_id = ?", sessionID); err != nil {
		_ = tx.Rollback()
		return nil, err
	}
	return &TelemetryWriteTx{tx: tx, sessionID: sessionID}, nil
}

func (w *TelemetryWriteTx) InsertChunk(ctx context.Context, row TelemetryChunkRow) error {
	if w.closed {
		return errors.New("telemetry writer is closed")
	}
	_, err := w.tx.ExecContext(ctx, `
		INSERT INTO telemetry_chunks(session_id, driver_abbr, lap, chunk_seq, codec, payload_blob, created_at)
		VALUES(?, ?, ?, ?, ?, ?, ?)
	`, w.sessionID, row.DriverAbbr, row.Lap, row.ChunkSeq, row.Codec, row.Payload, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (w *TelemetryWriteTx) Commit(ctx context.Context) error {
	if w.closed {
		return errors.New("telemetry writer already closed")
	}
	if _, err := w.tx.ExecContext(ctx, "UPDATE sessions SET updated_at = ? WHERE id = ?", time.Now().UTC().Format(time.RFC3339Nano), w.sessionID); err != nil {
		_ = w.tx.Rollback()
		w.closed = true
		return err
	}
	if err := w.tx.Commit(); err != nil {
		w.closed = true
		return err
	}
	w.closed = true
	return nil
}

func (w *TelemetryWriteTx) Rollback() {
	if w == nil || w.closed {
		return
	}
	_ = w.tx.Rollback()
	w.closed = true
}

func (s *Store) SessionReady(ctx context.Context, year, round int, sessionType string) (bool, time.Time, error) {
	var ready int
	var updatedRaw string
	err := s.db.QueryRowContext(ctx, `SELECT ready, updated_at FROM sessions WHERE year = ? AND round = ? AND session_type = ?`, year, round, strings.ToUpper(strings.TrimSpace(sessionType))).Scan(&ready, &updatedRaw)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, time.Time{}, nil
		}
		return false, time.Time{}, err
	}
	tm, _ := time.Parse(time.RFC3339Nano, updatedRaw)
	return ready == 1, tm, nil
}

func (s *Store) LoadReplayMeta(ctx context.Context, year, round int, sessionType string) (*ReplayMeta, error) {
	var out ReplayMeta
	var qualiRaw []byte
	err := s.db.QueryRowContext(ctx, `
		SELECT id, total_laps, total_time, COALESCE(quali_phases_json, '[]')
		FROM sessions
		WHERE year = ? AND round = ? AND session_type = ? AND ready = 1
	`, year, round, strings.ToUpper(strings.TrimSpace(sessionType))).Scan(&out.SessionID, &out.TotalLaps, &out.TotalTime, &qualiRaw)
	if err != nil {
		return nil, err
	}
	_ = json.Unmarshal(qualiRaw, &out.QualiPhases)

	rows, err := s.db.QueryContext(ctx, `
		SELECT frame_seq, ts_ms, lap, chunk_seq, frame_in_chunk
		FROM replay_frame_index
		WHERE session_id = ?
		ORDER BY frame_seq
	`, out.SessionID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	frames := make([]ReplayFrameIndexRow, 0, 4096)
	for rows.Next() {
		var row ReplayFrameIndexRow
		if err := rows.Scan(&row.FrameSeq, &row.TimestampMS, &row.Lap, &row.ChunkSeq, &row.FrameInChunk); err != nil {
			return nil, err
		}
		frames = append(frames, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	out.Frames = frames
	return &out, nil
}

func (s *Store) GetReplayChunkPayload(ctx context.Context, sessionID int64, chunkSeq int) ([]byte, string, error) {
	var payload []byte
	var codec string
	err := s.db.QueryRowContext(ctx, `
		SELECT payload_blob, codec
		FROM replay_chunks
		WHERE session_id = ? AND chunk_seq = ?
	`, sessionID, chunkSeq).Scan(&payload, &codec)
	if err != nil {
		return nil, "", err
	}
	return payload, codec, nil
}

func (s *Store) GetTelemetryPayload(ctx context.Context, year, round int, sessionType, driver string, lap int) ([]byte, string, error) {
	var sessionID int64
	err := s.db.QueryRowContext(ctx, `
		SELECT id FROM sessions WHERE year = ? AND round = ? AND session_type = ?
	`, year, round, strings.ToUpper(strings.TrimSpace(sessionType))).Scan(&sessionID)
	if err != nil {
		return nil, "", err
	}
	var payload []byte
	var codec string
	err = s.db.QueryRowContext(ctx, `
		SELECT payload_blob, codec
		FROM telemetry_chunks
		WHERE session_id = ? AND driver_abbr = ? AND lap = ?
		ORDER BY chunk_seq
		LIMIT 1
	`, sessionID, strings.ToUpper(strings.TrimSpace(driver)), lap).Scan(&payload, &codec)
	if err != nil {
		return nil, "", err
	}
	return payload, codec, nil
}

func (s *Store) SaveDownloadStateBlob(ctx context.Context, blob []byte) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO download_state(id, state_json, updated_at)
		VALUES(1, ?, ?)
		ON CONFLICT(id) DO UPDATE SET state_json=excluded.state_json, updated_at=excluded.updated_at
	`, blob, time.Now().UTC().Format(time.RFC3339Nano))
	return err
}

func (s *Store) LoadDownloadStateBlob(ctx context.Context) ([]byte, error) {
	var blob []byte
	err := s.db.QueryRowContext(ctx, "SELECT state_json FROM download_state WHERE id = 1").Scan(&blob)
	if err != nil {
		return nil, err
	}
	return blob, nil
}

func (s *Store) StartLiveStreamRun(ctx context.Context, year, round int, sessionType string) (int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback() }()

	now := time.Now().UTC().Format(time.RFC3339Nano)
	normalizedType := strings.ToUpper(strings.TrimSpace(sessionType))
	if _, err := tx.ExecContext(ctx, `
		UPDATE live_stream_runs
		SET status = 'stale',
		    ended_at = COALESCE(ended_at, ?),
		    last_error = CASE
		      WHEN COALESCE(last_error, '') = '' THEN 'interrupted'
		      ELSE last_error
		    END
		WHERE year = ? AND round = ? AND session_type = ? AND status = 'running'
	`, now, year, round, normalizedType); err != nil {
		return 0, err
	}

	res, err := tx.ExecContext(ctx, `
		INSERT INTO live_stream_runs(year, round, session_type, status, started_at)
		VALUES(?, ?, ?, 'running', ?)
	`, year, round, normalizedType, now)
	if err != nil {
		return 0, err
	}
	runID, err := res.LastInsertId()
	if err != nil {
		return 0, err
	}
	if err := tx.Commit(); err != nil {
		return 0, err
	}
	return runID, nil
}

func (s *Store) FinishLiveStreamRun(ctx context.Context, runID int64, status, lastError string) error {
	if runID <= 0 {
		return nil
	}
	status = strings.ToLower(strings.TrimSpace(status))
	switch status {
	case "finished", "stale", "failed":
	default:
		status = "stale"
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE live_stream_runs
		SET status = ?,
		    ended_at = COALESCE(ended_at, ?),
		    last_error = ?
		WHERE id = ?
	`, status, time.Now().UTC().Format(time.RFC3339Nano), strings.TrimSpace(lastError), runID)
	return err
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return ""
	}
}

func asInt(v any) int {
	switch x := v.(type) {
	case int:
		return x
	case int32:
		return int(x)
	case int64:
		return int(x)
	case float64:
		return int(x)
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(strings.TrimSpace(x))
		return i
	default:
		return 0
	}
}

func asFloat(v any) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case float32:
		return float64(x)
	case int:
		return float64(x)
	case int64:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f
	default:
		return 0
	}
}

func asFloatPtr(v any) any {
	if v == nil {
		return nil
	}
	f := asFloat(v)
	if f == 0 {
		if strings.TrimSpace(asString(v)) == "" {
			return nil
		}
	}
	return f
}

func asBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case int:
		return x != 0
	case int64:
		return x != 0
	case float64:
		return x != 0
	case string:
		x = strings.TrimSpace(strings.ToLower(x))
		return x == "1" || x == "true" || x == "yes"
	default:
		return false
	}
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func nilIfZeroInt(v int) any {
	if v == 0 {
		return nil
	}
	return v
}
