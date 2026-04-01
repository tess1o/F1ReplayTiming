package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sort"
	"strings"
	"time"
)

func (m *downloadManager) loadState() {
	if m.app.store == nil {
		return
	}
	b, err := m.app.store.LoadDownloadStateBlob(context.Background())
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
		k := job.key()
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
	b, err := json.Marshal(state)
	if err != nil {
		log.Printf("downloads: persist marshal failed: %v", err)
		return
	}
	if m.app.store == nil {
		return
	}
	if err := m.app.store.SaveDownloadStateBlob(context.Background(), b); err != nil {
		log.Printf("downloads: persist sqlite failed: %v", err)
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
	if m.active != nil && m.active.key().String() == key {
		return true
	}
	for _, job := range m.queue {
		if job.key().String() == key {
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
	processing := 0
	if m.active != nil {
		processing = 1
	}
	out.Summary = queueSummary{
		Queued:       len(m.queue),
		Processing:   processing,
		RecentFailed: recentFailed,
		RecentDone:   recentDone,
	}
	return out
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
	if m.active != nil && m.active.key().String() == ks {
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

	for idx, job := range m.queue {
		if job.key().String() == ks {
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
		if job.key().String() != ks {
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
		key := job.key()
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
