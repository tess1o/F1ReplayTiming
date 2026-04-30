package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"
)

func newDownloadManager(app *app, _ string) *downloadManager {
	timeoutMinutes := readPositiveIntEnv("DOWNLOAD_JOB_TIMEOUT_MINUTES", defaultDownloadTimeoutMinutes)
	maxAttempts := readPositiveIntEnv("DOWNLOAD_JOB_MAX_ATTEMPTS", defaultDownloadMaxAttempts)
	retryBaseSec := readPositiveIntEnv("DOWNLOAD_JOB_RETRY_BASE_SECONDS", defaultDownloadRetryBaseSec)

	m := &downloadManager{
		app:         app,
		timeout:     time.Duration(timeoutMinutes) * time.Minute,
		maxAttempts: maxAttempts,
		retryBase:   time.Duration(retryBaseSec) * time.Second,
	}
	m.cond = sync.NewCond(&m.mu)
	m.loadState()
	return m
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
	log.Printf("downloads: worker start job=%s year=%d round=%d type=%s max_attempts=%d timeout=%s",
		job.ID, job.Year, job.Round, job.SessionType, job.MaxAttempts, m.timeout)
	for attempt := 1; attempt <= job.MaxAttempts; attempt++ {
		m.setActiveAttempt(job.ID, attempt)
		log.Printf("downloads: worker attempt job=%s year=%d round=%d type=%s attempt=%d/%d",
			job.ID, job.Year, job.Round, job.SessionType, attempt, job.MaxAttempts)
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		err := m.app.runProcessSessionWorker(ctx, job.Year, job.Round, job.SessionType, func(msg string) {
			m.setActiveMessage(job.ID, msg, false)
		})
		cancel()

		if err == nil {
			log.Printf("downloads: worker success job=%s year=%d round=%d type=%s attempt=%d",
				job.ID, job.Year, job.Round, job.SessionType, attempt)
			m.finishActiveSuccess(job.ID)
			return
		}
		log.Printf("downloads: worker failure job=%s year=%d round=%d type=%s attempt=%d err=%v",
			job.ID, job.Year, job.Round, job.SessionType, attempt, err)

		timedOut := errors.Is(err, context.DeadlineExceeded) || strings.Contains(strings.ToLower(err.Error()), "deadline exceeded")
		if isRetryableJobError(err, timedOut) && attempt < job.MaxAttempts {
			backoff := m.retryBase * time.Duration(1<<(attempt-1))
			log.Printf("downloads: worker retry scheduled job=%s year=%d round=%d type=%s backoff=%s",
				job.ID, job.Year, job.Round, job.SessionType, backoff.Round(time.Second))
			m.setActiveRetry(job.ID, err.Error(), backoff)
			time.Sleep(backoff)
			continue
		}

		log.Printf("downloads: worker giving up job=%s year=%d round=%d type=%s err=%v", job.ID, job.Year, job.Round, job.SessionType, err)
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
