package storage

import (
	"context"
	"errors"
	"strings"
	"time"
)

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
