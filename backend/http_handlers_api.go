package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"f1replaytiming/backend/storage"
)

func (a *app) handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

func (a *app) handleAuthStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"auth_enabled": a.authEnabled})
}

func (a *app) handleAuthLogin(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Passphrase string `json:"passphrase"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid JSON"})
		return
	}
	if strings.TrimSpace(body.Passphrase) == "" || strings.TrimSpace(a.authPassphrase) == "" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid passphrase"})
		return
	}
	if strings.TrimSpace(body.Passphrase) != strings.TrimSpace(a.authPassphrase) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Invalid passphrase"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"token": a.makeToken(a.authPassphrase)})
}

func (a *app) handleAuthVerify(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"valid": true})
}

func (a *app) handleSeasons(w http.ResponseWriter, _ *http.Request) {
	now := time.Now().UTC().Year()
	seasons := make([]int, 0, len(availableSeasons))
	for _, y := range availableSeasons {
		if y <= now {
			seasons = append(seasons, y)
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"seasons": seasons})
}

func (a *app) handleSeasonEvents(w http.ResponseWriter, r *http.Request) {
	year, err := strconv.Atoi(r.PathValue("year"))
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid year"})
		return
	}

	data, err := a.buildEvents(year)
	if err != nil || data == nil {
		if err2 := a.ensureSchedule(year); err2 != nil {
			writeJSON(w, http.StatusNotFound, map[string]any{"detail": fmt.Sprintf("No schedule data for %d", year)})
			return
		}
		data, err = a.buildEvents(year)
	}
	if err != nil || data == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": fmt.Sprintf("No schedule data for %d", year)})
		return
	}
	writeJSON(w, http.StatusOK, data)
}

func (a *app) handleSession(w http.ResponseWriter, r *http.Request) {
	year, round, ok := parseYearRound(w, r)
	if !ok {
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))

	if data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "info.json")); err == nil {
		writeJSON(w, http.StatusOK, data)
		return
	}

	scheduleAny, _ := a.readJSONAny(filepath.Join("seasons", strconv.Itoa(year), "schedule.json"))
	if schedule, ok := scheduleAny.(map[string]any); ok {
		if events, ok := schedule["events"].([]any); ok && round > 0 && round <= len(events) {
			if evt, ok := events[round-1].(map[string]any); ok {
				writeJSON(w, http.StatusOK, map[string]any{
					"year":         year,
					"round_number": round,
					"event_name":   asString(evt["event_name"]),
					"circuit":      asString(evt["location"]),
					"country":      asString(evt["country"]),
					"session_type": defaultString(sessionTypeLabels[sessionType], sessionType),
					"drivers":      []any{},
				})
				return
			}
		}
	}

	writeJSON(w, http.StatusNotFound, map[string]any{
		"detail": fmt.Sprintf("Session data not available for %d Round %d (%s).", year, round, sessionType),
	})
}

func (a *app) handleTrack(w http.ResponseWriter, r *http.Request) {
	year, round, ok := parseYearRound(w, r)
	if !ok {
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))

	if data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "track.json")); err == nil {
		writeJSON(w, http.StatusOK, data)
		return
	}

	for _, alt := range sessionTypePriority {
		if alt == sessionType {
			continue
		}
		if data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), alt, "track.json")); err == nil {
			writeJSON(w, http.StatusOK, data)
			return
		}
	}
	for prev := year - 1; prev >= year-3; prev-- {
		for _, alt := range []string{"R", "Q"} {
			if data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(prev), strconv.Itoa(round), alt, "track.json")); err == nil {
				writeJSON(w, http.StatusOK, data)
				return
			}
		}
	}

	writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Track data not available for this session."})
}

func (a *app) handleLaps(w http.ResponseWriter, r *http.Request) {
	year, round, ok := parseYearRound(w, r)
	if !ok {
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))
	data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "laps.json"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Lap data not available for this session."})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"laps": data})
}

func (a *app) handleResults(w http.ResponseWriter, r *http.Request) {
	year, round, ok := parseYearRound(w, r)
	if !ok {
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))
	data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "results.json"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Results not available for this session."})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": data})
}

func (a *app) handleTelemetry(w http.ResponseWriter, r *http.Request) {
	year, round, ok := parseYearRound(w, r)
	if !ok {
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))
	driver := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("driver")))
	lapRaw := strings.TrimSpace(r.URL.Query().Get("lap"))
	lap, err := strconv.Atoi(lapRaw)
	if driver == "" || err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "driver and lap are required"})
		return
	}

	if a.store == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "Storage is not initialized"})
		return
	}
	payload, codec, err := a.store.GetTelemetryPayload(r.Context(), year, round, sessionType, driver, lap)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Telemetry not available for this lap"})
		return
	}
	decoded, err := storage.DecodeTelemetryChunk(payload, codec)
	if err != nil || len(decoded.PayloadJson) == 0 {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Telemetry not available for this lap"})
		return
	}
	var out any
	if err := json.Unmarshal(decoded.PayloadJson, &out); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Telemetry payload is invalid"})
		return
	}
	writeJSON(w, http.StatusOK, out)
}

func (a *app) handleLiveStatus(w http.ResponseWriter, _ *http.Request) {
	now := time.Now().UTC()
	year := now.Year()

	raw, err := a.readJSONAny(filepath.Join("seasons", strconv.Itoa(year), "schedule.json"))
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"live": nil})
		return
	}
	schedule, ok := raw.(map[string]any)
	if !ok {
		writeJSON(w, http.StatusOK, map[string]any{"live": nil})
		return
	}
	events, _ := schedule["events"].([]any)

	durations := map[string]time.Duration{
		"Race":              2 * time.Hour,
		"Qualifying":        70 * time.Minute,
		"Sprint":            1 * time.Hour,
		"Sprint Qualifying": 50 * time.Minute,
		"Sprint Shootout":   50 * time.Minute,
		"Practice 1":        1 * time.Hour,
		"Practice 2":        1 * time.Hour,
		"Practice 3":        1 * time.Hour,
	}
	sessionToType := map[string]string{
		"Race":              "R",
		"Qualifying":        "Q",
		"Sprint":            "S",
		"Sprint Qualifying": "SQ",
		"Sprint Shootout":   "SQ",
		"Practice 1":        "FP1",
		"Practice 2":        "FP2",
		"Practice 3":        "FP3",
	}

	for _, evtAny := range events {
		evt, ok := evtAny.(map[string]any)
		if !ok {
			continue
		}
		sessions, _ := evt["sessions"].([]any)
		for _, sAny := range sessions {
			s, ok := sAny.(map[string]any)
			if !ok {
				continue
			}
			dateUTC := asString(s["date_utc"])
			if dateUTC == "" {
				continue
			}
			ts, ok := parseDateMaybe(dateUTC)
			if !ok {
				continue
			}
			name := asString(s["name"])
			sessionType := sessionToType[name]
			if sessionType == "" {
				continue
			}
			duration := durations[name]
			if duration <= 0 {
				duration = time.Hour
			}
			windowStart := ts.Add(-15 * time.Minute)
			windowEnd := ts.Add(duration)
			if now.After(windowStart) && now.Before(windowEnd) {
				writeJSON(w, http.StatusOK, map[string]any{
					"live": map[string]any{
						"year":          year,
						"round_number":  asInt(evt["round_number"]),
						"event_name":    asString(evt["event_name"]),
						"country":       asString(evt["country"]),
						"session_name":  name,
						"session_type":  sessionType,
						"session_start": dateUTC,
						"pre_session":   now.Before(ts),
					},
				})
				return
			}
		}
	}

	writeJSON(w, http.StatusOK, map[string]any{"live": nil})
}
