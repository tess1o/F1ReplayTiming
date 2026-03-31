package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
)

const (
	downloadStateNotDownloaded = "not_downloaded"
	downloadStateQueued        = "queued"
	downloadStateProcessing    = "processing"
	downloadStateDownloaded    = "downloaded"
	downloadStateFailed        = "failed"

	defaultDownloadTimeoutMinutes = 25
	defaultDownloadMaxAttempts    = 3
	defaultDownloadRetryBaseSec   = 15
	maxRecentJobs                 = 200
)

var sessionNameToType = map[string]string{
	"Race":              "R",
	"Qualifying":        "Q",
	"Sprint":            "S",
	"Sprint Qualifying": "SQ",
	"Sprint Shootout":   "SQ",
	"Practice 1":        "FP1",
	"Practice 2":        "FP2",
	"Practice 3":        "FP3",
}

type sessionKey struct {
	Year        int
	Round       int
	SessionType string
}

func (k sessionKey) String() string {
	return fmt.Sprintf("%d_%d_%s", k.Year, k.Round, strings.ToUpper(strings.TrimSpace(k.SessionType)))
}

type downloadJob struct {
	ID          string     `json:"id"`
	Year        int        `json:"year"`
	Round       int        `json:"round"`
	SessionType string     `json:"session_type"`
	State       string     `json:"state"`
	Attempt     int        `json:"attempt"`
	MaxAttempts int        `json:"max_attempts"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
	StartedAt   *time.Time `json:"started_at,omitempty"`
	FinishedAt  *time.Time `json:"finished_at,omitempty"`
	LastError   string     `json:"last_error,omitempty"`
	Message     string     `json:"message,omitempty"`
	Source      string     `json:"source,omitempty"`
}

type downloadStatePersist struct {
	Queue  []*downloadJob `json:"queue"`
	Active *downloadJob   `json:"active,omitempty"`
	Recent []*downloadJob `json:"recent"`
}

type enqueueCounts struct {
	Enqueued          int `json:"enqueued"`
	AlreadyPresent    int `json:"already_present"`
	SkippedFuture     int `json:"skipped_future"`
	AlreadyDownloaded int `json:"already_downloaded"`
}

type sessionDownloadStatus struct {
	Year          int    `json:"year"`
	Round         int    `json:"round"`
	SessionType   string `json:"session_type"`
	DownloadState string `json:"download_state"`
	Downloaded    bool   `json:"downloaded"`
	LastError     string `json:"last_error,omitempty"`
	UpdatedAt     string `json:"updated_at,omitempty"`
	Message       string `json:"message,omitempty"`
	QueuePosition int    `json:"queue_position,omitempty"`
	Attempt       int    `json:"attempt,omitempty"`
	MaxAttempts   int    `json:"max_attempts,omitempty"`
}

type queueSummary struct {
	Queued       int `json:"queued"`
	Processing   int `json:"processing"`
	RecentFailed int `json:"recent_failed"`
	RecentDone   int `json:"recent_done"`
}

type queueSnapshot struct {
	ActiveJob  *downloadJob   `json:"active_job"`
	QueuedJobs []*downloadJob `json:"queued_jobs"`
	RecentJobs []*downloadJob `json:"recent_jobs"`
	Summary    queueSummary   `json:"summary"`
}

type downloadManager struct {
	app         *app
	statePath   string
	timeout     time.Duration
	maxAttempts int
	retryBase   time.Duration

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*downloadJob
	active *downloadJob
	recent []*downloadJob
}

func newDownloadManager(app *app, dataDir string) *downloadManager {
	timeoutMinutes := parsePositiveIntEnv("DOWNLOAD_JOB_TIMEOUT_MINUTES", defaultDownloadTimeoutMinutes)
	maxAttempts := parsePositiveIntEnv("DOWNLOAD_JOB_MAX_ATTEMPTS", defaultDownloadMaxAttempts)
	retryBaseSec := parsePositiveIntEnv("DOWNLOAD_JOB_RETRY_BASE_SECONDS", defaultDownloadRetryBaseSec)

	m := &downloadManager{
		app:         app,
		statePath:   filepath.Join(dataDir, "downloads", "state.json"),
		timeout:     time.Duration(timeoutMinutes) * time.Minute,
		maxAttempts: maxAttempts,
		retryBase:   time.Duration(retryBaseSec) * time.Second,
	}
	m.cond = sync.NewCond(&m.mu)
	m.loadState()
	return m
}

func parsePositiveIntEnv(name string, def int) int {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func (m *downloadManager) start() {
	go m.loop()
}

func (m *downloadManager) loop() {
	for {
		m.mu.Lock()
		for len(m.queue) == 0 {
			m.cond.Wait()
		}
		job := m.queue[0]
		m.queue = m.queue[1:]
		now := time.Now().UTC()
		job.State = downloadStateProcessing
		job.StartedAt = &now
		job.UpdatedAt = now
		job.Message = "Starting session processing"
		job.LastError = ""
		job.Attempt = 0
		m.active = job
		m.persistLocked()
		m.mu.Unlock()

		m.processActiveJob(job)

		m.mu.Lock()
		if m.active != nil && m.active.ID == job.ID {
			finished := time.Now().UTC()
			if m.active.FinishedAt == nil {
				m.active.FinishedAt = &finished
			}
			m.active.UpdatedAt = finished
			m.appendRecentLocked(m.active)
			m.active = nil
			m.persistLocked()
		}
		m.mu.Unlock()
	}
}

func (m *downloadManager) processActiveJob(job *downloadJob) {
	for attempt := 1; attempt <= job.MaxAttempts; attempt++ {
		m.setActiveAttempt(job.ID, attempt)
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		err := m.app.runProcessSessionWorker(ctx, job.Year, job.Round, job.SessionType, func(msg string) {
			m.setActiveMessage(job.ID, msg, false)
		})
		cancel()

		if err == nil {
			m.finishActiveSuccess(job.ID)
			return
		}

		timedOut := errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "deadline exceeded")
		if isRetryableJobError(err, timedOut) && attempt < job.MaxAttempts {
			backoff := m.retryBase * time.Duration(1<<(attempt-1))
			m.setActiveRetry(job.ID, err.Error(), backoff)
			time.Sleep(backoff)
			continue
		}

		m.finishActiveFailure(job.ID, err.Error())
		return
	}
}

func isRetryableJobError(err error, timedOut bool) bool {
	if err == nil {
		return false
	}
	if timedOut {
		return true
	}
	s := strings.ToLower(err.Error())
	signals := []string{
		"429",
		"rate limit",
		"too many requests",
		"timeout",
		"timed out",
		"temporary",
		"temporarily",
		"connection reset",
		"connection refused",
		"network",
		"service unavailable",
		"gateway timeout",
		"eof",
	}
	for _, sig := range signals {
		if strings.Contains(s, sig) {
			return true
		}
	}
	return false
}

func (m *downloadManager) setActiveAttempt(jobID string, attempt int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.ID != jobID {
		return
	}
	m.active.Attempt = attempt
	m.active.UpdatedAt = time.Now().UTC()
	m.active.Message = fmt.Sprintf("Processing (attempt %d/%d)", attempt, m.active.MaxAttempts)
	m.persistLocked()
}

func (m *downloadManager) setActiveMessage(jobID, message string, persist bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.ID != jobID {
		return
	}
	m.active.Message = strings.TrimSpace(message)
	m.active.UpdatedAt = time.Now().UTC()
	if persist {
		m.persistLocked()
	}
}

func (m *downloadManager) setActiveRetry(jobID, errMsg string, backoff time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.ID != jobID {
		return
	}
	m.active.LastError = errMsg
	m.active.Message = fmt.Sprintf("Retrying in %s after transient error", backoff.Round(time.Second))
	m.active.UpdatedAt = time.Now().UTC()
	m.persistLocked()
}

func (m *downloadManager) finishActiveSuccess(jobID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.ID != jobID {
		return
	}
	now := time.Now().UTC()
	m.active.State = downloadStateDownloaded
	m.active.Message = "Download complete"
	m.active.LastError = ""
	m.active.UpdatedAt = now
	m.active.FinishedAt = &now
	m.persistLocked()
}

func (m *downloadManager) finishActiveFailure(jobID, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.active == nil || m.active.ID != jobID {
		return
	}
	now := time.Now().UTC()
	m.active.State = downloadStateFailed
	m.active.LastError = errMsg
	m.active.Message = "Download failed"
	m.active.UpdatedAt = now
	m.active.FinishedAt = &now
	m.persistLocked()
}

func (m *downloadManager) appendRecentLocked(job *downloadJob) {
	cpy := cloneJob(job)
	m.recent = append(m.recent, cpy)
	if len(m.recent) > maxRecentJobs {
		m.recent = m.recent[len(m.recent)-maxRecentJobs:]
	}
}

func cloneJob(job *downloadJob) *downloadJob {
	if job == nil {
		return nil
	}
	cpy := *job
	if job.StartedAt != nil {
		t := *job.StartedAt
		cpy.StartedAt = &t
	}
	if job.FinishedAt != nil {
		t := *job.FinishedAt
		cpy.FinishedAt = &t
	}
	return &cpy
}

func (m *downloadManager) loadState() {
	b, err := os.ReadFile(m.statePath)
	if err != nil {
		return
	}
	var persisted downloadStatePersist
	if err := json.Unmarshal(b, &persisted); err != nil {
		log.Printf("downloads: could not parse state file: %v", err)
		return
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[string]struct{})
	appendIfValid := func(job *downloadJob) {
		if job == nil {
			return
		}
		k := sessionKey{Year: job.Year, Round: job.Round, SessionType: job.SessionType}
		if m.app.isSessionDownloaded(k.Year, k.Round, k.SessionType) {
			return
		}
		ks := k.String()
		if _, ok := seen[ks]; ok {
			return
		}
		seen[ks] = struct{}{}
		job.SessionType = strings.ToUpper(strings.TrimSpace(job.SessionType))
		job.State = downloadStateQueued
		job.Message = "Queued"
		job.LastError = ""
		job.StartedAt = nil
		job.FinishedAt = nil
		job.UpdatedAt = time.Now().UTC()
		if job.MaxAttempts <= 0 {
			job.MaxAttempts = m.maxAttempts
		}
		m.queue = append(m.queue, job)
	}

	if persisted.Active != nil {
		appendIfValid(persisted.Active)
	}
	for _, job := range persisted.Queue {
		appendIfValid(job)
	}

	m.recent = nil
	for _, job := range persisted.Recent {
		if job == nil {
			continue
		}
		m.recent = append(m.recent, cloneJob(job))
	}
	if len(m.recent) > maxRecentJobs {
		m.recent = m.recent[len(m.recent)-maxRecentJobs:]
	}

	m.persistLocked()
}

func (m *downloadManager) persistLocked() {
	state := downloadStatePersist{
		Queue:  m.queue,
		Active: m.active,
		Recent: m.recent,
	}
	b, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Printf("downloads: persist marshal failed: %v", err)
		return
	}

	dir := filepath.Dir(m.statePath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("downloads: persist mkdir failed: %v", err)
		return
	}
	tmp := m.statePath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		log.Printf("downloads: persist write failed: %v", err)
		return
	}
	if err := os.Rename(tmp, m.statePath); err != nil {
		log.Printf("downloads: persist rename failed: %v", err)
	}
}

func (m *downloadManager) enqueue(keys []sessionKey, source string) enqueueCounts {
	res := enqueueCounts{}
	seenInput := make(map[string]struct{})

	m.mu.Lock()
	defer m.mu.Unlock()

	for _, key := range keys {
		key.SessionType = strings.ToUpper(strings.TrimSpace(key.SessionType))
		if key.Year <= 0 || key.Round <= 0 || key.SessionType == "" {
			continue
		}
		ks := key.String()
		if _, ok := seenInput[ks]; ok {
			continue
		}
		seenInput[ks] = struct{}{}

		if m.app.isSessionDownloaded(key.Year, key.Round, key.SessionType) {
			res.AlreadyDownloaded++
			continue
		}
		if m.containsLocked(ks) {
			res.AlreadyPresent++
			continue
		}

		now := time.Now().UTC()
		job := &downloadJob{
			ID:          fmt.Sprintf("%s_%d", ks, now.UnixNano()),
			Year:        key.Year,
			Round:       key.Round,
			SessionType: key.SessionType,
			State:       downloadStateQueued,
			Attempt:     0,
			MaxAttempts: m.maxAttempts,
			CreatedAt:   now,
			UpdatedAt:   now,
			Message:     "Queued",
			Source:      source,
		}
		m.queue = append(m.queue, job)
		res.Enqueued++
	}

	if res.Enqueued > 0 {
		m.persistLocked()
		m.cond.Broadcast()
	}
	return res
}

func (m *downloadManager) containsLocked(key string) bool {
	if m.active != nil {
		if (sessionKey{Year: m.active.Year, Round: m.active.Round, SessionType: m.active.SessionType}).String() == key {
			return true
		}
	}
	for _, job := range m.queue {
		if (sessionKey{Year: job.Year, Round: job.Round, SessionType: job.SessionType}).String() == key {
			return true
		}
	}
	return false
}

func (m *downloadManager) queueSnapshot() queueSnapshot {
	m.mu.Lock()
	defer m.mu.Unlock()

	out := queueSnapshot{
		ActiveJob:  cloneJob(m.active),
		QueuedJobs: make([]*downloadJob, 0, len(m.queue)),
		RecentJobs: make([]*downloadJob, 0, len(m.recent)),
	}
	for _, q := range m.queue {
		out.QueuedJobs = append(out.QueuedJobs, cloneJob(q))
	}
	start := 0
	if len(m.recent) > 50 {
		start = len(m.recent) - 50
	}
	for i := len(m.recent) - 1; i >= start; i-- {
		out.RecentJobs = append(out.RecentJobs, cloneJob(m.recent[i]))
	}

	recentFailed := 0
	recentDone := 0
	for _, j := range m.recent {
		switch j.State {
		case downloadStateFailed:
			recentFailed++
		case downloadStateDownloaded:
			recentDone++
		}
	}
	out.Summary = queueSummary{
		Queued:       len(m.queue),
		Processing:   boolToInt(m.active != nil),
		RecentFailed: recentFailed,
		RecentDone:   recentDone,
	}
	return out
}

func boolToInt(v bool) int {
	if v {
		return 1
	}
	return 0
}

func (m *downloadManager) statusForSession(key sessionKey) sessionDownloadStatus {
	state := sessionDownloadStatus{
		Year:          key.Year,
		Round:         key.Round,
		SessionType:   strings.ToUpper(strings.TrimSpace(key.SessionType)),
		DownloadState: downloadStateNotDownloaded,
		Downloaded:    false,
	}

	ks := key.String()
	m.mu.Lock()
	if m.active != nil {
		if (sessionKey{Year: m.active.Year, Round: m.active.Round, SessionType: m.active.SessionType}).String() == ks {
			state.DownloadState = m.active.State
			state.Downloaded = m.active.State == downloadStateDownloaded
			state.Attempt = m.active.Attempt
			state.MaxAttempts = m.active.MaxAttempts
			state.Message = m.active.Message
			state.LastError = m.active.LastError
			state.UpdatedAt = m.active.UpdatedAt.UTC().Format(time.RFC3339)
			m.mu.Unlock()
			return state
		}
	}

	for idx, job := range m.queue {
		if (sessionKey{Year: job.Year, Round: job.Round, SessionType: job.SessionType}).String() == ks {
			state.DownloadState = downloadStateQueued
			state.QueuePosition = idx + 1
			state.Attempt = job.Attempt
			state.MaxAttempts = job.MaxAttempts
			state.Message = job.Message
			state.UpdatedAt = job.UpdatedAt.UTC().Format(time.RFC3339)
			m.mu.Unlock()
			return state
		}
	}

	var recent *downloadJob
	for i := len(m.recent) - 1; i >= 0; i-- {
		job := m.recent[i]
		if (sessionKey{Year: job.Year, Round: job.Round, SessionType: job.SessionType}).String() != ks {
			continue
		}
		recent = cloneJob(job)
		break
	}
	m.mu.Unlock()

	if m.app.isSessionDownloaded(key.Year, key.Round, key.SessionType) {
		state.DownloadState = downloadStateDownloaded
		state.Downloaded = true
		if mod, ok := m.app.sessionDataUpdatedAt(key.Year, key.Round, key.SessionType); ok {
			state.UpdatedAt = mod.UTC().Format(time.RFC3339)
		}
		return state
	}

	if recent != nil && recent.State == downloadStateFailed {
		state.DownloadState = downloadStateFailed
		state.LastError = recent.LastError
		state.Message = recent.Message
		state.Attempt = recent.Attempt
		state.MaxAttempts = recent.MaxAttempts
		state.UpdatedAt = recent.UpdatedAt.UTC().Format(time.RFC3339)
		return state
	}

	return state
}

func (m *downloadManager) failedKeysByYear(year int) []sessionKey {
	m.mu.Lock()
	defer m.mu.Unlock()

	seen := make(map[string]struct{})
	out := make([]sessionKey, 0)
	for i := len(m.recent) - 1; i >= 0; i-- {
		job := m.recent[i]
		if job.Year != year {
			continue
		}
		key := sessionKey{Year: job.Year, Round: job.Round, SessionType: job.SessionType}
		ks := key.String()
		if _, ok := seen[ks]; ok {
			continue
		}
		seen[ks] = struct{}{}
		if job.State != downloadStateFailed {
			continue
		}
		if m.app.isSessionDownloaded(key.Year, key.Round, key.SessionType) {
			continue
		}
		out = append(out, key)
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Year != out[j].Year {
			return out[i].Year < out[j].Year
		}
		if out[i].Round != out[j].Round {
			return out[i].Round < out[j].Round
		}
		return out[i].SessionType < out[j].SessionType
	})
	return out
}

func (a *app) runProcessSessionWorker(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error {
	if a.processor != nil {
		return a.processor.ProcessSession(ctx, year, round, sessionType, onStatus)
	}
	args := []string{
		a.workerPath,
		"process-session",
		"--year", strconv.Itoa(year),
		"--round", strconv.Itoa(round),
		"--type", strings.ToUpper(strings.TrimSpace(sessionType)),
	}
	cmd := exec.CommandContext(ctx, a.pythonBin, args...)
	cmd.Env = os.Environ()

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	stderr, err := cmd.StderrPipe()
	if err != nil {
		return err
	}

	if err := cmd.Start(); err != nil {
		return err
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var statusErr error

	wg.Add(1)
	go func() {
		defer wg.Done()
		s := bufio.NewScanner(stderr)
		buf := make([]byte, 0, 64*1024)
		s.Buffer(buf, 1024*1024)
		for s.Scan() {
			log.Printf("worker stderr: %s", s.Text())
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		s := bufio.NewScanner(stdout)
		buf := make([]byte, 0, 64*1024)
		s.Buffer(buf, 1024*1024)
		for s.Scan() {
			line := s.Bytes()
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
				mu.Lock()
				statusErr = errors.New(defaultString(asString(evt["message"]), "worker failed"))
				mu.Unlock()
			}
		}
	}()

	waitErr := cmd.Wait()
	wg.Wait()

	if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return ctx.Err()
	}

	mu.Lock()
	errCopy := statusErr
	mu.Unlock()
	if errCopy != nil {
		return errCopy
	}
	if waitErr != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return waitErr
	}
	return nil
}

func (a *app) isSessionDownloaded(year, round int, sessionType string) bool {
	base := filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), strings.ToUpper(strings.TrimSpace(sessionType)))
	return a.fileExists(filepath.Join(base, "replay.json")) && a.fileExists(filepath.Join(base, "info.json"))
}

func (a *app) sessionDataUpdatedAt(year, round int, sessionType string) (time.Time, bool) {
	p := filepath.Join(a.dataDir, "sessions", strconv.Itoa(year), strconv.Itoa(round), strings.ToUpper(strings.TrimSpace(sessionType)), "replay.json")
	st, err := os.Stat(p)
	if err != nil {
		return time.Time{}, false
	}
	return st.ModTime(), true
}

func (a *app) sessionDownloadStatus(year, round int, sessionType string) sessionDownloadStatus {
	if a.downloads == nil {
		status := sessionDownloadStatus{
			Year:          year,
			Round:         round,
			SessionType:   strings.ToUpper(strings.TrimSpace(sessionType)),
			DownloadState: downloadStateNotDownloaded,
		}
		if a.isSessionDownloaded(year, round, sessionType) {
			status.Downloaded = true
			status.DownloadState = downloadStateDownloaded
			if mod, ok := a.sessionDataUpdatedAt(year, round, sessionType); ok {
				status.UpdatedAt = mod.UTC().Format(time.RFC3339)
			}
		}
		return status
	}
	return a.downloads.statusForSession(sessionKey{Year: year, Round: round, SessionType: sessionType})
}

func normalizeSessionType(raw string) string {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	switch raw {
	case "R", "Q", "S", "SQ", "FP1", "FP2", "FP3":
		return raw
	default:
		return ""
	}
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

	var req struct {
		Mode        string `json:"mode"`
		Year        int    `json:"year"`
		Round       int    `json:"round"`
		SessionType string `json:"session_type"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid JSON"})
		return
	}

	req.Mode = strings.TrimSpace(req.Mode)
	if req.Year <= 0 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "year is required"})
		return
	}
	if err := a.ensureSchedule(req.Year); err != nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": fmt.Sprintf("No schedule for %d", req.Year)})
		return
	}

	root, err := a.buildEvents(req.Year)
	if err != nil || root == nil {
		writeJSON(w, http.StatusNotFound, map[string]any{"detail": fmt.Sprintf("No schedule for %d", req.Year)})
		return
	}
	events, _ := root["events"].([]any)

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
			writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "round and session_type are required for mode=session"})
			return
		}
		matched := false
		for _, eAny := range events {
			evt, ok := eAny.(map[string]any)
			if !ok || asInt(evt["round_number"]) != req.Round {
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
				addSession(req.Year, req.Round, typ, asBoolValue(s["available"]))
			}
		}
		if !matched {
			writeJSON(w, http.StatusNotFound, map[string]any{"detail": "session not found in schedule"})
			return
		}
	case "weekend", "season_all", "season_races", "season_races_quali":
		if req.Mode == "weekend" && req.Round <= 0 {
			writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "round is required for mode=weekend"})
			return
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
				addSession(req.Year, round, typ, asBoolValue(s["available"]))
			}
		}
	default:
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "invalid mode"})
		return
	}

	if len(targets) == 0 && counts.SkippedFuture == 0 {
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

func asBoolValue(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		b, _ := strconv.ParseBool(strings.TrimSpace(x))
		return b
	default:
		return false
	}
}
