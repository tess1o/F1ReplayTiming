package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"time"
)

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

func (s *Store) SetSessionReady(ctx context.Context, sessionID int64, ready bool) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE sessions
		SET ready = ?, updated_at = ?
		WHERE id = ?
	`, boolToInt(ready), time.Now().UTC().Format(time.RFC3339Nano), sessionID)
	return err
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
