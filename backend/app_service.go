package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func (a *app) ensureSchedule(year int) error {
	path := filepath.Join("seasons", strconv.Itoa(year), "schedule.json")
	forceRefresh := a.shouldForceScheduleRefresh(year)
	exists := a.fileExists(path)
	if exists && !forceRefresh {
		log.Printf("schedule: using cached schedule year=%d path=%s", year, path)
		return nil
	}
	log.Printf("schedule: rebuild requested year=%d path=%s cached=%t force_refresh=%t", year, path, exists, forceRefresh)

	a.scheduleLockMu.Lock()
	lock := a.scheduleLocks[year]
	if lock == nil {
		lock = &sync.Mutex{}
		a.scheduleLocks[year] = lock
	}
	a.scheduleLockMu.Unlock()

	lock.Lock()
	defer lock.Unlock()

	if a.fileExists(path) && !forceRefresh {
		log.Printf("schedule: another worker refreshed schedule year=%d path=%s", year, path)
		return nil
	}

	if a.processor == nil {
		log.Printf("schedule: processor missing year=%d", year)
		return errors.New("session processor is not initialized")
	}
	err := a.processor.EnsureSchedule(context.Background(), year)
	if err != nil {
		log.Printf("schedule: rebuild failed year=%d err=%v", year, err)
		return err
	}
	log.Printf("schedule: rebuild complete year=%d", year)
	return nil
}

func (a *app) shouldForceScheduleRefresh(year int) bool {
	now := time.Now().UTC()
	if year != now.Year() {
		return false
	}
	a.scheduleLockMu.Lock()
	defer a.scheduleLockMu.Unlock()
	last := a.scheduleRefresh[year]
	if !last.IsZero() && now.Sub(last) < scheduleRefreshIntervalCurrentSeason {
		return false
	}
	a.scheduleRefresh[year] = now
	log.Printf("schedule: force refresh granted year=%d interval=%s last=%s now=%s", year, scheduleRefreshIntervalCurrentSeason, last.Format(time.RFC3339), now.Format(time.RFC3339))
	return true
}

func (a *app) ensureSessionData(year, round int, sessionType string, onStatus func(string)) error {
	path := filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "replay.json")
	if a.fileExists(path) {
		return nil
	}

	key := fmt.Sprintf("%d_%d_%s", year, round, sessionType)
	a.sessionLockMu.Lock()
	lock := a.sessionLocks[key]
	if lock == nil {
		lock = &sync.Mutex{}
		a.sessionLocks[key] = lock
	}
	a.sessionLockMu.Unlock()

	lock.Lock()
	defer lock.Unlock()

	if a.fileExists(path) {
		return nil
	}

	if a.processor == nil {
		return errors.New("session processor is not initialized")
	}
	return a.processor.ProcessSession(context.Background(), year, round, sessionType, onStatus)
}

func (a *app) buildEvents(year int) (map[string]any, error) {
	raw, err := a.readJSONAny(filepath.Join("seasons", strconv.Itoa(year), "schedule.json"))
	if err != nil {
		return nil, err
	}
	root, ok := raw.(map[string]any)
	if !ok {
		return nil, errors.New("invalid schedule format")
	}
	b, _ := json.Marshal(root)
	_ = json.Unmarshal(b, &root)

	events, _ := root["events"].([]any)
	now := time.Now().UTC()
	lastPast := -1

	for i, evtAny := range events {
		evt, ok := evtAny.(map[string]any)
		if !ok {
			continue
		}
		hasPast := false
		sessions, _ := evt["sessions"].([]any)
		for _, sAny := range sessions {
			s, ok := sAny.(map[string]any)
			if !ok {
				continue
			}
			dateStr := asString(s["date_utc"])
			available := false
			if ts, ok := parseDateMaybe(dateStr); ok {
				available = ts.Before(now)
				if available {
					hasPast = true
				}
				if !strings.HasSuffix(dateStr, "Z") {
					s["date_utc"] = strings.ReplaceAll(dateStr, " ", "T") + "Z"
				}
			}
			s["available"] = available

			sessionType := normalizeSessionType(asString(s["session_type"]))
			if sessionType == "" {
				sessionType = normalizeSessionType(sessionNameToType[asString(s["name"])])
			}
			if sessionType != "" {
				s["session_type"] = sessionType
				st := a.sessionDownloadStatus(year, asInt(evt["round_number"]), sessionType)
				s["download_state"] = st.DownloadState
				s["downloaded"] = st.Downloaded
				if strings.TrimSpace(st.LastError) != "" {
					s["last_error"] = st.LastError
				}
				if strings.TrimSpace(st.UpdatedAt) != "" {
					s["updated_at"] = st.UpdatedAt
				}
			}
		}
		if hasPast {
			evt["status"] = "available"
			lastPast = i
		} else {
			evt["status"] = "future"
		}
	}
	if lastPast >= 0 {
		if evt, ok := events[lastPast].(map[string]any); ok {
			evt["status"] = "latest"
		}
	}
	return root, nil
}

func parseDateMaybe(s string) (time.Time, bool) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339, strings.ReplaceAll(s, " ", "T")); err == nil {
		return t.UTC(), true
	}
	if strings.HasSuffix(s, "Z") {
		if t, err := time.Parse("2006-01-02T15:04:05Z", strings.ReplaceAll(s, " ", "T")); err == nil {
			return t.UTC(), true
		}
	}
	return time.Time{}, false
}

func (a *app) readJSONAny(rel string) (any, error) {
	if a.store == nil {
		return nil, errors.New("sqlite store is not configured")
	}
	b, err := a.store.GetJSONArtifact(context.Background(), filepath.ToSlash(rel))
	if err != nil {
		return nil, err
	}
	var out any
	if err := json.Unmarshal(b, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (a *app) fileExists(rel string) bool {
	if a.store == nil {
		return false
	}
	_, err := a.store.GetJSONArtifact(context.Background(), filepath.ToSlash(rel))
	return err == nil
}
