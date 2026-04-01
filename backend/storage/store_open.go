package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
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
