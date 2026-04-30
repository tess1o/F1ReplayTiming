package main

import (
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8000"
	}

	application := newAppFromEnv()
	defer func() {
		if err := application.store.Close(); err != nil {
			log.Printf("warning: failed to close sqlite store: %v", err)
		}
	}()

	application.downloads.start()
	if application.staticData != nil {
		application.staticData.start()
	}
	handler := application.withMiddleware(newHTTPMux(application))
	log.Printf("Go backend listening on :%s (data_dir=%s)", port, application.dataDir)
	if err := http.ListenAndServe(":"+port, handler); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}

func newAppFromEnv() *app {
	dataDir := strings.TrimSpace(os.Getenv("DATA_DIR"))
	if dataDir == "" {
		dataDir = filepath.Clean("data")
	}
	store := openStore(dataDir)

	replayChunkFrames := readPositiveIntEnv("REPLAY_CHUNK_FRAMES", 256)
	telemetryChunkSamples := readPositiveIntEnv("TELEMETRY_CHUNK_SAMPLES", 512)

	application := &app{
		dataDir:         dataDir,
		store:           store,
		replayChunkSize: replayChunkFrames,
		replayCache: newReplayCache(
			readPositiveInt64Env("REPLAY_CACHE_MAX_MB", 256)*1024*1024,
			readPositiveDurationSecondsEnv("REPLAY_CACHE_TTL_SECONDS", 5*time.Minute),
		),
		allowedOrigins:  buildAllowedOrigins(),
		authEnabled:     isTrue(os.Getenv("AUTH_ENABLED")),
		authPassphrase:  os.Getenv("AUTH_PASSPHRASE"),
		sessionLocks:    make(map[string]*sync.Mutex),
		scheduleLocks:   make(map[int]*sync.Mutex),
		scheduleRefresh: make(map[int]time.Time),
	}
	application.processor = NewSessionProcessor(dataDir, store, replayChunkFrames, telemetryChunkSamples)
	application.downloads = newDownloadManager(application, dataDir)
	application.staticData = newStaticDataRefresher(application)
	return application
}

func newHTTPMux(a *app) *http.ServeMux {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/health", a.handleHealth)
	mux.HandleFunc("GET /api/auth/status", a.handleAuthStatus)
	mux.HandleFunc("POST /api/auth/login", a.handleAuthLogin)
	mux.HandleFunc("GET /api/auth/verify", a.handleAuthVerify)

	mux.HandleFunc("GET /api/seasons", a.handleSeasons)
	mux.HandleFunc("GET /api/seasons/{year}/events", a.handleSeasonEvents)
	mux.HandleFunc("GET /api/sessions/{year}/{round}", a.handleSession)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/track", a.handleTrack)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/laps", a.handleLaps)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/q3-lines", a.handleQ3Lines)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/results", a.handleResults)
	mux.HandleFunc("GET /api/sessions/{year}/{round}/telemetry", a.handleTelemetry)
	mux.HandleFunc("GET /api/live/status", a.handleLiveStatus)
	mux.HandleFunc("GET /api/static-data/status", a.handleStaticDataStatus)
	mux.HandleFunc("POST /api/static-data/refresh", a.handleStaticDataRefresh)
	mux.HandleFunc("GET /api/downloads/queue", a.handleDownloadsQueue)
	mux.HandleFunc("GET /api/downloads/session-status", a.handleDownloadSessionStatus)
	mux.HandleFunc("POST /api/downloads/enqueue", a.handleDownloadEnqueue)
	mux.HandleFunc("POST /api/downloads/retry-failed", a.handleDownloadRetryFailed)

	mux.HandleFunc("GET /ws/replay/{year}/{round}", a.handleReplayWebSocket)
	mux.HandleFunc("GET /ws/live/{year}/{round}", a.handleLiveWebSocket)
	return mux
}

func readPositiveIntEnv(name string, def int) int {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func readPositiveInt64Env(name string, def int64) int64 {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		if n, err := strconv.ParseInt(raw, 10, 64); err == nil && n > 0 {
			return n
		}
	}
	return def
}

func readPositiveDurationSecondsEnv(name string, def time.Duration) time.Duration {
	if raw := strings.TrimSpace(os.Getenv(name)); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return time.Duration(n) * time.Second
		}
	}
	return def
}
