package main

import (
	"context"
	"errors"
	"strings"
	"time"
)

func (a *app) runProcessSessionWorker(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error {
	if a.processor == nil {
		return errors.New("session processor is not initialized")
	}
	return a.processor.ProcessSession(ctx, year, round, sessionType, onStatus)
}

func (a *app) isSessionDownloaded(year, round int, sessionType string) bool {
	if a.store == nil {
		return false
	}
	ready, _, err := a.store.SessionReady(context.Background(), year, round, strings.ToUpper(strings.TrimSpace(sessionType)))
	return err == nil && ready
}

func (a *app) sessionDataUpdatedAt(year, round int, sessionType string) (time.Time, bool) {
	if a.store == nil {
		return time.Time{}, false
	}
	_, updated, err := a.store.SessionReady(context.Background(), year, round, strings.ToUpper(strings.TrimSpace(sessionType)))
	if err != nil || updated.IsZero() {
		return time.Time{}, false
	}
	return updated, true
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
