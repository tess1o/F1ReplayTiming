package storage

import (
	"context"
	"errors"
	"strings"
	"time"
)

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
