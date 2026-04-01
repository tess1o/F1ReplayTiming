package storage

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

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

func (w *ReplayWriteTx) Rollback() {
	if w == nil || w.closed {
		return
	}
	_ = w.tx.Rollback()
	w.closed = true
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
