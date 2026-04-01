package main

import (
	"fmt"
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

func (j *downloadJob) key() sessionKey {
	if j == nil {
		return sessionKey{}
	}
	return sessionKey{Year: j.Year, Round: j.Round, SessionType: j.SessionType}
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
	timeout     time.Duration
	maxAttempts int
	retryBase   time.Duration

	mu     sync.Mutex
	cond   *sync.Cond
	queue  []*downloadJob
	active *downloadJob
	recent []*downloadJob
}
