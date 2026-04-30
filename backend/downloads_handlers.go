package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
)

type downloadEnqueueRequest struct {
	Mode        string `json:"mode"`
	Year        int    `json:"year"`
	Round       int    `json:"round"`
	SessionType string `json:"session_type"`
}

func (a *app) handleDownloadsQueue(w http.ResponseWriter, _ *http.Request) {
	if a.downloads == nil {
		writeJSON(w, http.StatusOK, queueSnapshot{})
		return
	}
	writeJSON(w, http.StatusOK, a.downloads.queueSnapshot())
}

func (a *app) handleDownloadSessionStatus(w http.ResponseWriter, r *http.Request) {
	year, errY := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("year")))
	round, errR := strconv.Atoi(strings.TrimSpace(r.URL.Query().Get("round")))
	sessionType := normalizeSessionType(r.URL.Query().Get("type"))
	if errY != nil || errR != nil || sessionType == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "year, round and type are required"})
		return
	}
	writeJSON(w, http.StatusOK, a.sessionDownloadStatus(year, round, sessionType))
}

func (a *app) handleDownloadEnqueue(w http.ResponseWriter, r *http.Request) {
	if a.downloads == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "download manager is not available"})
		return
	}

	var req downloadEnqueueRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid JSON"})
		return
	}
	req.Mode = strings.TrimSpace(req.Mode)
	if req.Year <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "year is required"})
		return
	}
	log.Printf("downloads: enqueue request mode=%s year=%d round=%d session_type=%s", req.Mode, req.Year, req.Round, strings.ToUpper(strings.TrimSpace(req.SessionType)))

	events, err := a.loadScheduleEvents(req.Year)
	if err != nil {
		log.Printf("downloads: enqueue schedule unavailable year=%d err=%v", req.Year, err)
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": err.Error()})
		return
	}

	targets, counts, err := collectTargetsForMode(req, events)
	if err != nil {
		log.Printf("downloads: enqueue target selection failed mode=%s year=%d round=%d session_type=%s err=%v", req.Mode, req.Year, req.Round, strings.ToUpper(strings.TrimSpace(req.SessionType)), err)
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	log.Printf("downloads: enqueue target selection mode=%s year=%d requested_round=%d requested_type=%s targets=%d skipped_future=%d",
		req.Mode, req.Year, req.Round, strings.ToUpper(strings.TrimSpace(req.SessionType)), len(targets), counts.SkippedFuture)

	if len(targets) == 0 && counts.SkippedFuture == 0 {
		log.Printf("downloads: enqueue no-op mode=%s year=%d round=%d session_type=%s", req.Mode, req.Year, req.Round, strings.ToUpper(strings.TrimSpace(req.SessionType)))
		writeJSON(w, http.StatusOK, map[string]any{
			"mode":   req.Mode,
			"counts": counts,
		})
		return
	}

	enq := a.downloads.enqueue(targets, "api:"+req.Mode)
	counts.Enqueued += enq.Enqueued
	counts.AlreadyPresent += enq.AlreadyPresent
	counts.AlreadyDownloaded += enq.AlreadyDownloaded
	log.Printf("downloads: enqueue result mode=%s year=%d requested_round=%d requested_type=%s enqueued=%d already_present=%d already_downloaded=%d skipped_future=%d",
		req.Mode, req.Year, req.Round, strings.ToUpper(strings.TrimSpace(req.SessionType)),
		counts.Enqueued, counts.AlreadyPresent, counts.AlreadyDownloaded, counts.SkippedFuture)

	writeJSON(w, http.StatusOK, map[string]any{
		"mode":   req.Mode,
		"counts": counts,
	})
}

func (a *app) handleDownloadRetryFailed(w http.ResponseWriter, r *http.Request) {
	if a.downloads == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "download manager is not available"})
		return
	}
	var req struct {
		Year int `json:"year"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid JSON"})
		return
	}
	if req.Year <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "year is required"})
		return
	}
	keys := a.downloads.failedKeysByYear(req.Year)
	enq := a.downloads.enqueue(keys, "api:retry_failed")
	writeJSON(w, http.StatusOK, map[string]any{
		"year": req.Year,
		"counts": enqueueCounts{
			Enqueued:          enq.Enqueued,
			AlreadyPresent:    enq.AlreadyPresent,
			SkippedFuture:     0,
			AlreadyDownloaded: enq.AlreadyDownloaded,
		},
	})
}

func (a *app) loadScheduleEvents(year int) ([]any, error) {
	if err := a.ensureSchedule(year); err != nil {
		log.Printf("downloads: ensureSchedule failed year=%d err=%v", year, err)
		return nil, fmt.Errorf("No schedule for %d", year)
	}
	root, err := a.buildEvents(year)
	if err != nil || root == nil {
		if err != nil {
			log.Printf("downloads: buildEvents failed year=%d err=%v", year, err)
		}
		return nil, fmt.Errorf("No schedule for %d", year)
	}
	events, _ := root["events"].([]any)
	log.Printf("downloads: schedule events loaded year=%d events=%d", year, len(events))
	return events, nil
}

func collectTargetsForMode(req downloadEnqueueRequest, events []any) ([]sessionKey, enqueueCounts, error) {
	targets := make([]sessionKey, 0)
	counts := enqueueCounts{}
	addSession := func(year int, round int, typ string, available bool) {
		if typ == "" {
			return
		}
		if !available {
			counts.SkippedFuture++
			return
		}
		targets = append(targets, sessionKey{Year: year, Round: round, SessionType: typ})
	}

	switch req.Mode {
	case "session":
		t := normalizeSessionType(req.SessionType)
		if req.Round <= 0 || t == "" {
			return nil, counts, fmt.Errorf("round and session_type are required for mode=session")
		}
		matched := false
		knownRounds := make([]int, 0, len(events))
		for _, eAny := range events {
			evt, ok := eAny.(map[string]any)
			if !ok {
				continue
			}
			roundNumber := asInt(evt["round_number"])
			if roundNumber > 0 {
				knownRounds = append(knownRounds, roundNumber)
			}
			if roundNumber != req.Round {
				continue
			}
			sessions, _ := evt["sessions"].([]any)
			for _, sAny := range sessions {
				s, ok := sAny.(map[string]any)
				if !ok {
					continue
				}
				typ := normalizeSessionType(asString(s["session_type"]))
				if typ == "" {
					typ = normalizeSessionType(sessionNameToType[asString(s["name"])])
				}
				if typ != t {
					continue
				}
				matched = true
				addSession(req.Year, req.Round, typ, asBool(s["available"]))
			}
		}
		if !matched {
			return nil, counts, fmt.Errorf("session not found in schedule (round=%d type=%s known_rounds=%v)", req.Round, t, knownRounds)
		}
	case "weekend", "season_all", "season_races", "season_races_quali":
		if req.Mode == "weekend" && req.Round <= 0 {
			return nil, counts, fmt.Errorf("round is required for mode=weekend")
		}
		allow := func(typ string) bool { return true }
		switch req.Mode {
		case "season_races":
			allow = func(typ string) bool { return typ == "R" || typ == "S" }
		case "season_races_quali":
			allow = func(typ string) bool { return typ == "R" || typ == "S" || typ == "Q" || typ == "SQ" }
		}
		for _, eAny := range events {
			evt, ok := eAny.(map[string]any)
			if !ok {
				continue
			}
			round := asInt(evt["round_number"])
			if req.Mode == "weekend" && round != req.Round {
				continue
			}
			sessions, _ := evt["sessions"].([]any)
			for _, sAny := range sessions {
				s, ok := sAny.(map[string]any)
				if !ok {
					continue
				}
				typ := normalizeSessionType(asString(s["session_type"]))
				if typ == "" {
					typ = normalizeSessionType(sessionNameToType[asString(s["name"])])
				}
				if typ == "" || !allow(typ) {
					continue
				}
				addSession(req.Year, round, typ, asBool(s["available"]))
			}
		}
	default:
		return nil, counts, fmt.Errorf("invalid mode")
	}

	return targets, counts, nil
}
