package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

var (
	sessionTypePriority = []string{"R", "Q", "S", "SQ", "FP1", "FP2", "FP3"}
	sessionTypeLabels   = map[string]string{
		"R":   "Race",
		"Q":   "Qualifying",
		"S":   "Sprint",
		"SQ":  "Sprint Qualifying",
		"FP1": "Practice 1",
		"FP2": "Practice 2",
		"FP3": "Practice 3",
	}
	availableSeasons = []int{2024, 2025, 2026, 2027, 2028}
)

type app struct {
	dataDir         string
	workerPath      string
	pythonBin       string
	processorMode   string
	processor       SessionProcessor
	replayCache     *replayCache
	downloads       *downloadManager
	allowedOrigins  map[string]struct{}
	authEnabled     bool
	authPassphrase  string
	sessionLockMu   sync.Mutex
	sessionLocks    map[string]*sync.Mutex
	scheduleLockMu  sync.Mutex
	scheduleLocks   map[int]*sync.Mutex
	pitLossMu       sync.Mutex
	pitLossRaw      map[string]any
	pitLossLoadedAt time.Time
}

type replayCache struct {
	mu       sync.Mutex
	entries  map[string]*replayCacheEntry
	maxBytes int64
	ttl      time.Duration
	current  int64
}

type replayCacheEntry struct {
	key         string
	sizeBytes   int64
	replayPath  string
	frames      []replayFrameMeta
	totalLaps   int
	totalTime   float64
	qualiPhases []map[string]any
	clients     int
	lastAccess  time.Time
	evictTimer  *time.Timer
}

type replayFrameMeta struct {
	Start     int64   `json:"start"`
	End       int64   `json:"end"`
	Timestamp float64 `json:"timestamp"`
	Lap       int     `json:"lap"`
}

type replayIndexFile struct {
	Version       int               `json:"version"`
	ReplaySize    int64             `json:"replay_size"`
	ReplayModUnix int64             `json:"replay_mod_unix"`
	Frames        []replayFrameMeta `json:"frames"`
	TotalLaps     int               `json:"total_laps"`
	TotalTime     float64           `json:"total_time"`
	QualiPhases   []map[string]any  `json:"quali_phases"`
}

type pitLossValues struct {
	Green float64
	SC    float64
	VSC   float64
}

func main() {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8000"
	}

	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = filepath.Clean(filepath.Join("..", "data-fetcher", "data"))
	}

	workerPath := strings.TrimSpace(os.Getenv("PY_WORKER_PATH"))
	if workerPath == "" {
		workerPath = filepath.Clean(filepath.Join("..", "data-fetcher", "worker_bridge.py"))
	}
	pythonBin := strings.TrimSpace(os.Getenv("PYTHON_BIN"))
	if pythonBin == "" {
		pythonBin = "python3"
	}

	maxCacheMB := int64(256)
	if raw := strings.TrimSpace(os.Getenv("REPLAY_CACHE_MAX_MB")); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			maxCacheMB = n
		}
	}
	cacheTTL := 5 * time.Minute
	if raw := strings.TrimSpace(os.Getenv("REPLAY_CACHE_TTL_SECONDS")); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			cacheTTL = time.Duration(n) * time.Second
		}
	}

	app := &app{
		dataDir:        dataDir,
		workerPath:     workerPath,
		pythonBin:      pythonBin,
		processorMode:  strings.ToLower(defaultString(strings.TrimSpace(os.Getenv("PROCESSOR_MODE")), "go")),
		replayCache:    newReplayCache(maxCacheMB*1024*1024, cacheTTL),
		allowedOrigins: buildAllowedOrigins(),
		authEnabled:    isTrue(os.Getenv("AUTH_ENABLED")),
		authPassphrase: os.Getenv("AUTH_PASSPHRASE"),
		sessionLocks:   make(map[string]*sync.Mutex),
		scheduleLocks:  make(map[int]*sync.Mutex),
	}
	app.processor = NewSessionProcessor(app.processorMode, dataDir, workerPath, pythonBin)
	app.downloads = newDownloadManager(app, dataDir)
	app.downloads.start()

	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", app.handleHealth)
	mux.HandleFunc("GET /api/auth/status", app.handleAuthStatus)
	mux.HandleFunc("POST /api/auth/login", app.handleAuthLogin)
	mux.HandleFunc("GET /api/auth/verify", app.handleAuthVerify)

	mux.HandleFunc("GET /api/seasons", app.handleSeasons)
	mux.HandleFunc("GET /api/seasons/{year}/events", app.handleSeasonEvents)
	mux.HandleFunc("GET /api/sessions/{year}/{round}", app.handleSession)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/track", app.handleTrack)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/laps", app.handleLaps)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/results", app.handleResults)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/telemetry", app.handleTelemetry)
	mux.HandleFunc("GET /api/live/status", app.handleLiveStatus)
	mux.HandleFunc("GET /api/downloads/queue", app.handleDownloadsQueue)
	mux.HandleFunc("GET /api/downloads/session-status", app.handleDownloadSessionStatus)
	mux.HandleFunc("POST /api/downloads/enqueue", app.handleDownloadEnqueue)
	mux.HandleFunc("POST /api/downloads/retry-failed", app.handleDownloadRetryFailed)

	mux.HandleFunc("GET /ws/replay/{year}/{round}", app.handleReplayWebSocket)
	mux.HandleFunc("GET /ws/live/{year}/{round}", app.handleLiveWebSocket)

	handler := app.withMiddleware(mux)
	log.Printf("Go backend listening on :%s (data_dir=%s processor_mode=%s)", port, dataDir, app.processorMode)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func newReplayCache(maxBytes int64, ttl time.Duration) *replayCache {
	return &replayCache{
		entries:  make(map[string]*replayCacheEntry),
		maxBytes: maxBytes,
		ttl:      ttl,
	}
}

func (a *app) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.applyCORS(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/api") && a.authEnabled {
			if r.URL.Path != "/api/auth/status" && r.URL.Path != "/api/auth/login" && r.URL.Path != "/api/health" {
				token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
				if !a.verifyToken(token) {
					writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Unauthorized"})
					return
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}

func (a *app) applyCORS(w http.ResponseWriter, r *http.Request) {
	if len(a.allowedOrigins) == 0 {
		return
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return
	}
	if !a.isAllowedOrigin(origin) {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
}

func buildAllowedOrigins() map[string]struct{} {
	var origins []string
	if o := strings.TrimSpace(os.Getenv("FRONTEND_URL")); o != "" {
		origins = append(origins, o)
	}
	if extra := strings.TrimSpace(os.Getenv("EXTRA_ORIGINS")); extra != "" {
		for _, o := range strings.Split(extra, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				origins = append(origins, o)
			}
		}
	}
	for _, o := range append([]string{}, origins...) {
		if strings.HasPrefix(o, "https://") {
			origins = append(origins, "http://"+strings.TrimPrefix(o, "https://"))
		} else if strings.HasPrefix(o, "http://") && !strings.Contains(o, "localhost") {
			origins = append(origins, "https://"+strings.TrimPrefix(o, "http://"))
		}
	}
	out := make(map[string]struct{})
	for _, o := range origins {
		out[o] = struct{}{}
	}
	return out
}

func (a *app) isAllowedOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	_, ok := a.allowedOrigins[origin]
	return ok
}

func (a *app) wsOriginAllowed(r *http.Request) bool {
	// In same-origin proxy deployments, CORS is not needed and WS requests may be
	// forwarded without explicit origin allow-list config.
	if len(a.allowedOrigins) == 0 {
		return true
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	return a.isAllowedOrigin(origin)
}

func isTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (a *app) makeToken(passphrase string) string {
	h := sha256.Sum256([]byte("f1replay:" + passphrase))
	return hex.EncodeToString(h[:])
}

func (a *app) verifyToken(token string) bool {
	if !a.authEnabled || token == "" || a.authPassphrase == "" {
		return false
	}
	expected := a.makeToken(a.authPassphrase)
	if len(token) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

func (a *app) checkWSAuth(r *http.Request) bool {
	if !a.authEnabled {
		return true
	}
	return a.verifyToken(strings.TrimSpace(r.URL.Query().Get("token")))
}

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

	data, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "telemetry", driver+".json"))
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Telemetry not available for this driver"})
		return
	}
	m, ok := data.(map[string]any)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Telemetry not available for this driver"})
		return
	}
	lapData, ok := m[strconv.Itoa(lap)]
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": "Telemetry not available for this lap"})
		return
	}
	writeJSON(w, http.StatusOK, lapData)
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

func (a *app) handleReplayWebSocket(w http.ResponseWriter, r *http.Request) {
	if !a.checkWSAuth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}

	year, err1 := strconv.Atoi(r.PathValue("year"))
	round, err2 := strconv.Atoi(r.PathValue("round"))
	if err1 != nil || err2 != nil {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))

	upgrader := websocket.Upgrader{
		CheckOrigin: a.wsOriginAllowed,
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	send := func(v any) error {
		b, _ := json.Marshal(v)
		return conn.WriteMessage(websocket.TextMessage, b)
	}

	_ = send(map[string]any{"type": "status", "message": "Loading session data..."})

	if !a.isSessionDownloaded(year, round, sessionType) {
		st := a.sessionDownloadStatus(year, round, sessionType)
		msg := "Session data is not downloaded yet."
		if st.DownloadState == downloadStateProcessing && strings.TrimSpace(st.Message) != "" {
			msg = st.Message
		} else if st.DownloadState == downloadStateQueued {
			msg = "Session is queued for download."
		} else if st.DownloadState == downloadStateFailed && strings.TrimSpace(st.LastError) != "" {
			msg = "Session download failed. Please retry from Downloads."
		}
		_ = send(map[string]any{
			"type":           "error",
			"message":        msg,
			"download_state": st.DownloadState,
		})
		return
	}

	key := fmt.Sprintf("%d_%d_%s", year, round, sessionType)
	entry, err := a.replayCache.getOrLoad(key, func() (*replayCacheEntry, error) {
		return a.loadReplayEntry(year, round, sessionType)
	})
	if err != nil || len(entry.frames) == 0 {
		_ = send(map[string]any{"type": "error", "message": "No position data available"})
		return
	}

	a.replayCache.connect(entry.key)
	defer a.replayCache.disconnect(entry.key)
	replayFile, err := os.Open(entry.replayPath)
	if err != nil {
		_ = send(map[string]any{"type": "error", "message": "Replay file is not readable"})
		return
	}
	defer replayFile.Close()

	pitLoss := a.resolvePitLoss(year, round, sessionType)
	isRace := sessionType == "R" || sessionType == "S"

	_ = send(map[string]any{
		"type":         "ready",
		"total_frames": len(entry.frames),
		"total_time":   entry.totalTime,
		"total_laps":   entry.totalLaps,
		"quali_phases": entry.qualiPhases,
	})

	if firstRaw, err := readFrameAt(replayFile, entry.frames[0]); err == nil {
		if b, err := prepareFramePayload(firstRaw, isRace, pitLoss); err == nil {
			_ = conn.WriteMessage(websocket.TextMessage, b)
		}
	} else {
		_ = send(map[string]any{"type": "error", "message": "Failed to read replay frame"})
		return
	}

	cmdCh := make(chan string, 16)
	errCh := make(chan error, 1)
	go func() {
		defer close(cmdCh)
		for {
			_, msg, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				return
			}
			cmdCh <- strings.ToLower(strings.TrimSpace(string(msg)))
		}
	}()

	playing := false
	speed := 1.0
	idx := 0
	anchorWall := time.Now()
	anchorSession := entry.frames[0].Timestamp

	resetAnchor := func() {
		if idx >= 0 && idx < len(entry.frames) {
			anchorWall = time.Now()
			anchorSession = entry.frames[idx].Timestamp
		}
	}

	sendFrameAt := func(i int) bool {
		if i < 0 || i >= len(entry.frames) {
			return false
		}
		frameRaw, err := readFrameAt(replayFile, entry.frames[i])
		if err != nil {
			return false
		}
		if b, err := prepareFramePayload(frameRaw, isRace, pitLoss); err == nil {
			_ = conn.WriteMessage(websocket.TextMessage, b)
			return true
		}
		return false
	}

	findByTime := func(target float64) int {
		for i, frame := range entry.frames {
			if frame.Timestamp >= target {
				return i
			}
		}
		return len(entry.frames) - 1
	}
	findByLap := func(target int) int {
		for i, frame := range entry.frames {
			if frame.Lap >= target {
				return i
			}
		}
		return len(entry.frames) - 1
	}

	handleCmd := func(cmd string) {
		switch {
		case cmd == "play":
			playing = true
			resetAnchor()
		case cmd == "pause":
			playing = false
		case cmd == "reset":
			idx = 0
			playing = false
			if !sendFrameAt(idx) {
				return
			}
			resetAnchor()
		case strings.HasPrefix(cmd, "speed:"):
			v, err := strconv.ParseFloat(strings.TrimPrefix(cmd, "speed:"), 64)
			if err == nil {
				if v < 0.25 {
					v = 0.25
				}
				if v > 50 {
					v = 50
				}
				speed = v
				resetAnchor()
			}
		case strings.HasPrefix(cmd, "seek:"):
			v, err := strconv.ParseFloat(strings.TrimPrefix(cmd, "seek:"), 64)
			if err == nil {
				idx = findByTime(v)
				if !sendFrameAt(idx) {
					return
				}
				resetAnchor()
			}
		case strings.HasPrefix(cmd, "seeklap:"):
			v, err := strconv.Atoi(strings.TrimPrefix(cmd, "seeklap:"))
			if err == nil {
				idx = findByLap(v)
				if !sendFrameAt(idx) {
					return
				}
				resetAnchor()
			}
		}
	}

	for {
		select {
		case cmd, ok := <-cmdCh:
			if !ok {
				return
			}
			handleCmd(cmd)
		case <-time.After(20 * time.Millisecond):
			if !playing {
				continue
			}
			if idx >= len(entry.frames) {
				playing = false
				_ = send(map[string]any{"type": "finished"})
				continue
			}
			now := time.Now()
			targetWall := anchorWall.Add(time.Duration((entry.frames[idx].Timestamp - anchorSession) / speed * float64(time.Second)))
			if now.Before(targetWall) {
				continue
			}
			if !sendFrameAt(idx) {
				return
			}
			idx++
		case <-errCh:
			return
		}
	}
}

func (a *app) handleLiveWebSocket(w http.ResponseWriter, r *http.Request) {
	if !a.checkWSAuth(r) {
		http.Error(w, "Unauthorized", http.StatusUnauthorized)
		return
	}
	if a.processor != nil && strings.EqualFold(a.processorMode, "go") {
		http.Error(w, "Live stream is not available in Phase 1 Go processor mode", http.StatusNotImplemented)
		return
	}
	year, err1 := strconv.Atoi(r.PathValue("year"))
	round, err2 := strconv.Atoi(r.PathValue("round"))
	if err1 != nil || err2 != nil {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}
	sessionType := strings.ToUpper(defaultString(r.URL.Query().Get("type"), "R"))
	source := strings.TrimSpace(r.URL.Query().Get("source"))
	if source == "" {
		source = "auto"
	}
	speed := defaultString(r.URL.Query().Get("speed"), "10")

	upgrader := websocket.Upgrader{
		CheckOrigin: a.wsOriginAllowed,
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	args := []string{
		a.workerPath,
		"live-stream",
		"--year", strconv.Itoa(year),
		"--round", strconv.Itoa(round),
		"--type", sessionType,
		"--source", source,
		"--speed", speed,
	}

	cmd := exec.CommandContext(ctx, a.pythonBin, args...)
	cmd.Env = os.Environ()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "Failed to start live worker"})
		return
	}
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": "Failed to start live worker"})
		return
	}

	go func() {
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("live-worker: %s", scanner.Text())
		}
	}()

	done := make(chan struct{})
	go func() {
		defer close(done)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Bytes()
			if err := conn.WriteMessage(websocket.TextMessage, append([]byte{}, line...)); err != nil {
				cancel()
				return
			}
		}
	}()

	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			break
		}
	}

	cancel()
	_ = cmd.Wait()
	<-done
}

func (a *app) ensureSchedule(year int) error {
	if a.fileExists(filepath.Join("seasons", strconv.Itoa(year), "schedule.json")) {
		return nil
	}

	a.scheduleLockMu.Lock()
	lock := a.scheduleLocks[year]
	if lock == nil {
		lock = &sync.Mutex{}
		a.scheduleLocks[year] = lock
	}
	a.scheduleLockMu.Unlock()

	lock.Lock()
	defer lock.Unlock()

	if a.fileExists(filepath.Join("seasons", strconv.Itoa(year), "schedule.json")) {
		return nil
	}

	if a.processor != nil {
		return a.processor.EnsureSchedule(context.Background(), year)
	}
	return a.runWorker("ensure-schedule", "--year", strconv.Itoa(year))
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

	if a.processor != nil {
		return a.processor.ProcessSession(context.Background(), year, round, sessionType, onStatus)
	}
	return a.runWorkerStreaming([]string{"process-session", "--year", strconv.Itoa(year), "--round", strconv.Itoa(round), "--type", sessionType}, onStatus)
}

func (a *app) runWorker(args ...string) error {
	return a.runWorkerStreaming(args, nil)
}

func (a *app) runWorkerStreaming(args []string, onStatus func(string)) error {
	full := append([]string{a.workerPath}, args...)
	cmd := exec.Command(a.pythonBin, full...)
	cmd.Env = os.Environ()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, _ := cmd.StderrPipe()

	if err := cmd.Start(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var statusErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stderr)
		for scanner.Scan() {
			log.Printf("worker stderr: %s", scanner.Text())
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			line := scanner.Bytes()
			var evt map[string]any
			if err := json.Unmarshal(line, &evt); err != nil {
				log.Printf("worker out: %s", string(line))
				continue
			}
			typ := asString(evt["type"])
			if typ == "status" && onStatus != nil {
				onStatus(asString(evt["message"]))
			}
			if typ == "error" {
				statusErr = errors.New(defaultString(asString(evt["message"]), "worker failed"))
			}
		}
	}()

	waitErr := cmd.Wait()
	wg.Wait()
	if statusErr != nil {
		return statusErr
	}
	if waitErr != nil {
		return waitErr
	}
	return nil
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
	full := filepath.Join(a.dataDir, rel)
	b, err := os.ReadFile(full)
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
	_, err := os.Stat(filepath.Join(a.dataDir, rel))
	return err == nil
}

func parseYearRound(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	year, err1 := strconv.Atoi(r.PathValue("year"))
	round, err2 := strconv.Atoi(r.PathValue("round"))
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid path"})
		return 0, 0, false
	}
	return year, round, true
}

func defaultString(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
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
	case float64:
		return int(x)
	case int:
		return x
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(x)
		return i
	default:
		return 0
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (c *replayCache) getOrLoad(key string, loader func() (*replayCacheEntry, error)) (*replayCacheEntry, error) {
	c.mu.Lock()
	if e, ok := c.entries[key]; ok {
		e.lastAccess = time.Now()
		if e.evictTimer != nil {
			e.evictTimer.Stop()
			e.evictTimer = nil
		}
		c.mu.Unlock()
		return e, nil
	}
	c.mu.Unlock()

	e, err := loader()
	if err != nil {
		return nil, err
	}
	e.key = key
	e.lastAccess = time.Now()

	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.entries[key]; ok {
		return existing, nil
	}
	c.entries[key] = e
	c.current += e.sizeBytes
	c.evictToBudgetLocked()
	return e, nil
}

func (c *replayCache) connect(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if e, ok := c.entries[key]; ok {
		e.clients++
		e.lastAccess = time.Now()
		if e.evictTimer != nil {
			e.evictTimer.Stop()
			e.evictTimer = nil
		}
	}
}

func (c *replayCache) disconnect(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return
	}
	if e.clients > 0 {
		e.clients--
	}
	e.lastAccess = time.Now()
	if e.clients == 0 {
		if e.evictTimer != nil {
			e.evictTimer.Stop()
		}
		e.evictTimer = time.AfterFunc(c.ttl, func() {
			c.mu.Lock()
			defer c.mu.Unlock()
			e2, ok := c.entries[key]
			if !ok || e2.clients > 0 {
				return
			}
			c.current -= e2.sizeBytes
			delete(c.entries, key)
		})
	}
}

func (c *replayCache) evictToBudgetLocked() {
	if c.current <= c.maxBytes {
		return
	}
	keys := make([]string, 0, len(c.entries))
	for k := range c.entries {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return c.entries[keys[i]].lastAccess.Before(c.entries[keys[j]].lastAccess)
	})
	for _, k := range keys {
		if c.current <= c.maxBytes {
			break
		}
		e := c.entries[k]
		if e.clients > 0 {
			continue
		}
		if e.evictTimer != nil {
			e.evictTimer.Stop()
		}
		c.current -= e.sizeBytes
		delete(c.entries, k)
	}
}

func (a *app) loadReplayEntry(year, round int, sessionType string) (*replayCacheEntry, error) {
	rel := filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "replay.json")
	replayPath := filepath.Join(a.dataDir, rel)
	replayStat, err := os.Stat(replayPath)
	if err != nil {
		return nil, err
	}
	indexPath := filepath.Join(a.dataDir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "replay.index.json")

	index, err := loadReplayIndex(indexPath, replayStat.Size(), replayStat.ModTime().Unix())
	if err != nil {
		index, err = buildReplayIndexFromReplay(replayPath, replayStat)
		if err != nil {
			return nil, err
		}
		if writeErr := writeReplayIndex(indexPath, index); writeErr != nil {
			log.Printf("warning: failed to write replay sidecar index %s: %v", indexPath, writeErr)
		}
	}

	if len(index.Frames) == 0 {
		return &replayCacheEntry{
			replayPath: replayPath,
			frames:     []replayFrameMeta{},
			sizeBytes:  0,
		}, nil
	}

	return &replayCacheEntry{
		sizeBytes:   estimateReplayIndexBytes(index.Frames, index.QualiPhases),
		replayPath:  replayPath,
		frames:      index.Frames,
		totalLaps:   index.TotalLaps,
		totalTime:   index.TotalTime,
		qualiPhases: index.QualiPhases,
	}, nil
}

func loadReplayIndex(indexPath string, replaySize int64, replayModUnix int64) (*replayIndexFile, error) {
	raw, err := os.ReadFile(indexPath)
	if err != nil {
		return nil, err
	}
	var idx replayIndexFile
	if err := json.Unmarshal(raw, &idx); err != nil {
		return nil, err
	}
	if idx.Version != 1 || idx.ReplaySize != replaySize || (idx.ReplayModUnix != 0 && idx.ReplayModUnix != replayModUnix) {
		return nil, errors.New("sidecar index is stale or incompatible")
	}
	return &idx, nil
}

func writeReplayIndex(indexPath string, idx *replayIndexFile) error {
	if idx == nil {
		return errors.New("nil replay index")
	}
	body, err := json.Marshal(idx)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(indexPath), 0o755); err != nil {
		return err
	}
	tmpPath := indexPath + ".tmp"
	if err := os.WriteFile(tmpPath, body, 0o644); err != nil {
		return err
	}
	return os.Rename(tmpPath, indexPath)
}

func buildReplayIndexFromReplay(replayPath string, st os.FileInfo) (*replayIndexFile, error) {
	f, err := os.Open(replayPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 1024*1024)
	offset := int64(0)
	readByte := func() (byte, error) {
		b, err := reader.ReadByte()
		if err != nil {
			return 0, err
		}
		offset++
		return b, nil
	}
	skipSpace := func() error {
		for {
			b, err := readByte()
			if err != nil {
				return err
			}
			switch b {
			case ' ', '\n', '\r', '\t':
				continue
			default:
				if err := reader.UnreadByte(); err != nil {
					return err
				}
				offset--
				return nil
			}
		}
	}

	if err := skipSpace(); err != nil {
		return nil, err
	}
	b, err := readByte()
	if err != nil {
		return nil, err
	}
	if b != '[' {
		return nil, errors.New("replay must be a JSON array")
	}

	frames := make([]replayFrameMeta, 0, 4096)
	qualiPhases := make([]map[string]any, 0, 3)
	seenPhases := make(map[string]struct{})
	totalLaps := 0
	totalTime := 0.0

	for {
		if err := skipSpace(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("unexpected EOF while reading replay")
			}
			return nil, err
		}
		peek, err := readByte()
		if err != nil {
			return nil, err
		}
		if peek == ']' {
			break
		}
		if peek != '{' {
			return nil, fmt.Errorf("expected replay frame object at byte %d", offset-1)
		}

		frameStart := offset - 1
		frameBuf := bytes.NewBuffer(make([]byte, 0, 4096))
		frameBuf.WriteByte('{')
		depth := 1
		inString := false
		escaped := false
		for depth > 0 {
			ch, err := readByte()
			if err != nil {
				return nil, errors.New("unterminated frame object in replay")
			}
			frameBuf.WriteByte(ch)
			if inString {
				if escaped {
					escaped = false
					continue
				}
				if ch == '\\' {
					escaped = true
					continue
				}
				if ch == '"' {
					inString = false
				}
				continue
			}
			switch ch {
			case '"':
				inString = true
			case '{':
				depth++
			case '}':
				depth--
			}
		}
		frameEnd := offset

		var meta struct {
			Timestamp  float64 `json:"timestamp"`
			Lap        int     `json:"lap"`
			TotalLaps  int     `json:"total_laps"`
			QualiPhase *struct {
				Phase string `json:"phase"`
			} `json:"quali_phase"`
		}
		if err := json.Unmarshal(frameBuf.Bytes(), &meta); err != nil {
			return nil, fmt.Errorf("invalid replay frame metadata: %w", err)
		}
		frames = append(frames, replayFrameMeta{
			Start:     frameStart,
			End:       frameEnd,
			Timestamp: meta.Timestamp,
			Lap:       meta.Lap,
		})
		totalTime = meta.Timestamp
		totalLaps = meta.TotalLaps
		if meta.QualiPhase != nil && meta.QualiPhase.Phase != "" {
			if _, ok := seenPhases[meta.QualiPhase.Phase]; !ok {
				seenPhases[meta.QualiPhase.Phase] = struct{}{}
				qualiPhases = append(qualiPhases, map[string]any{
					"phase":     meta.QualiPhase.Phase,
					"timestamp": meta.Timestamp,
				})
			}
		}

		if err := skipSpace(); err != nil {
			if errors.Is(err, io.EOF) {
				return nil, errors.New("unexpected EOF after frame object")
			}
			return nil, err
		}
		sep, err := readByte()
		if err != nil {
			return nil, err
		}
		if sep == ',' {
			continue
		}
		if sep == ']' {
			break
		}
		return nil, fmt.Errorf("expected ',' or ']' after replay frame at byte %d", offset-1)
	}

	return &replayIndexFile{
		Version:       1,
		ReplaySize:    st.Size(),
		ReplayModUnix: st.ModTime().Unix(),
		Frames:        frames,
		TotalLaps:     totalLaps,
		TotalTime:     totalTime,
		QualiPhases:   qualiPhases,
	}, nil
}

func estimateReplayIndexBytes(frames []replayFrameMeta, qualiPhases []map[string]any) int64 {
	// Approximate in-memory footprint for replay index cache entry.
	return int64(len(frames))*40 + int64(len(qualiPhases))*96
}

func readFrameAt(f *os.File, frame replayFrameMeta) (json.RawMessage, error) {
	if frame.End <= frame.Start {
		return nil, errors.New("invalid replay frame span")
	}
	size := frame.End - frame.Start
	buf := make([]byte, size)
	_, err := f.ReadAt(buf, frame.Start)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(buf), nil
}

func (a *app) resolvePitLoss(year, round int, sessionType string) pitLossValues {
	if !(sessionType == "R" || sessionType == "S") {
		return pitLossValues{}
	}

	a.pitLossMu.Lock()
	if a.pitLossRaw == nil || time.Since(a.pitLossLoadedAt) > 10*time.Minute {
		if raw, err := a.readJSONAny("pit_loss.json"); err == nil {
			if m, ok := raw.(map[string]any); ok {
				a.pitLossRaw = m
				a.pitLossLoadedAt = time.Now()
			}
		}
	}
	pit := a.pitLossRaw
	a.pitLossMu.Unlock()

	if pit == nil {
		return pitLossValues{Green: 22.0, SC: 10.0, VSC: 14.5}
	}
	eventName := ""
	if infoRaw, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "info.json")); err == nil {
		if info, ok := infoRaw.(map[string]any); ok {
			eventName = asString(info["event_name"])
		}
	}

	if circuits, ok := pit["circuits"].(map[string]any); ok && eventName != "" {
		if entryAny, ok := circuits[eventName]; ok {
			if entry, ok := entryAny.(map[string]any); ok {
				return pitLossValues{
					Green: asFloat(entry["pit_loss_green"], 22.0),
					SC:    asFloat(entry["pit_loss_sc"], 10.0),
					VSC:   asFloat(entry["pit_loss_vsc"], 14.5),
				}
			}
		}
	}
	if ga, ok := pit["global_averages"].(map[string]any); ok {
		return pitLossValues{
			Green: asFloat(ga["green"], 22.0),
			SC:    asFloat(ga["sc"], 10.0),
			VSC:   asFloat(ga["vsc"], 14.5),
		}
	}
	return pitLossValues{Green: 22.0, SC: 10.0, VSC: 14.5}
}

func asFloat(v any, def float64) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err == nil {
			return f
		}
	}
	return def
}

func prepareFramePayload(raw json.RawMessage, isRace bool, pit pitLossValues) ([]byte, error) {
	if !isRace || pit.Green <= 0 {
		return wrapFrameRaw(raw), nil
	}
	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		return wrapFrameRaw(raw), nil
	}
	addPitPredictions(frame, pit)
	b, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	return wrapFrameRaw(b), nil
}

func wrapFrameRaw(raw []byte) []byte {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		fallback, _ := json.Marshal(map[string]any{"type": "frame"})
		return fallback
	}
	out := make([]byte, 0, len(raw)+16)
	out = append(out, []byte(`{"type":"frame",`)...)
	out = append(out, raw[1:]...)
	return out
}

func addPitPredictions(frame map[string]any, pit pitLossValues) {
	lap := asInt(frame["lap"])
	if lap < 5 {
		return
	}
	status := asString(frame["status"])
	selected := pit.Green
	if status == "sc" {
		selected = pit.SC
	} else if status == "vsc" {
		selected = pit.VSC
	}
	driversAny, ok := frame["drivers"].([]any)
	if !ok {
		return
	}
	type pair struct {
		abbr string
		gap  float64
	}
	gaps := make([]pair, 0, len(driversAny))
	for _, dAny := range driversAny {
		d, ok := dAny.(map[string]any)
		if !ok {
			continue
		}
		if asBool(d["retired"]) || asBool(d["in_pit"]) {
			continue
		}
		abbr := asString(d["abbr"])
		if abbr == "" {
			continue
		}
		if asInt(d["position"]) == 1 {
			gaps = append(gaps, pair{abbr: abbr, gap: 0})
			continue
		}
		if g, ok := parseGap(asString(d["gap"])); ok {
			gaps = append(gaps, pair{abbr: abbr, gap: g})
		}
	}
	if len(gaps) == 0 {
		return
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].gap < gaps[j].gap })

	for _, dAny := range driversAny {
		d, ok := dAny.(map[string]any)
		if !ok {
			continue
		}
		if asBool(d["retired"]) || asBool(d["in_pit"]) {
			d["pit_prediction"] = nil
			continue
		}
		abbr := asString(d["abbr"])
		if abbr == "" {
			d["pit_prediction"] = nil
			continue
		}
		current := 0.0
		if asInt(d["position"]) == 1 {
			current = 0
		} else {
			g, ok := parseGap(asString(d["gap"]))
			if !ok {
				d["pit_prediction"] = nil
				continue
			}
			current = g
		}
		projected := current + selected
		others := make([]float64, 0, len(gaps)-1)
		for _, p := range gaps {
			if p.abbr != abbr {
				others = append(others, p.gap)
			}
		}
		pred := 1
		for _, g := range others {
			if projected > g {
				pred++
			} else {
				break
			}
		}
		if pred > len(others)+1 {
			pred = len(others) + 1
		}
		currentPos := asInt(d["position"])
		if pred > currentPos {
			d["pit_prediction"] = pred
			behindIdx := pred - 1
			if behindIdx < len(others) {
				margin := others[behindIdx] - projected
				if margin < 0 {
					margin = 0
				}
				d["pit_prediction_margin"] = roundTo(margin, 3)
			} else {
				d["pit_prediction_margin"] = nil
			}
			aheadIdx := pred - 2
			if aheadIdx >= 0 && aheadIdx < len(others) {
				fa := projected - others[aheadIdx]
				if fa < 0 {
					fa = 0
				}
				d["pit_prediction_free_air"] = roundTo(fa, 1)
			} else {
				d["pit_prediction_free_air"] = nil
			}
		} else {
			d["pit_prediction"] = nil
			d["pit_prediction_margin"] = nil
			d["pit_prediction_free_air"] = nil
		}
	}
}

func asBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return isTrue(x)
	default:
		return false
	}
}

func parseGap(gap string) (float64, bool) {
	gap = strings.TrimSpace(gap)
	if gap == "" || strings.HasPrefix(gap, "LAP ") {
		return 0, false
	}
	if strings.Contains(strings.ToUpper(gap), "LAP") {
		return 0, false
	}
	gap = strings.TrimPrefix(gap, "+")
	v, err := strconv.ParseFloat(gap, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func roundTo(v float64, places int) float64 {
	pow := 1.0
	for i := 0; i < places; i++ {
		pow *= 10
	}
	return float64(int(v*pow+0.5)) / pow
}
