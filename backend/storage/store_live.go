package storage

import (
	"context"
	"strings"
	"time"
)

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
