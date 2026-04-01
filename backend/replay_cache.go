package main

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"time"
)

func newReplayCache(maxBytes int64, ttl time.Duration) *replayCache {
	return &replayCache{
		entries:  make(map[string]*replayCacheEntry),
		maxBytes: maxBytes,
		ttl:      ttl,
	}
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
	if a.store == nil {
		return nil, errors.New("sqlite store is not configured")
	}
	meta, err := a.store.LoadReplayMeta(context.Background(), year, round, sessionType)
	if err != nil {
		return nil, err
	}
	if len(meta.Frames) == 0 {
		return &replayCacheEntry{
			frames:    []replayFrameMeta{},
			sizeBytes: 0,
		}, nil
	}
	frames := make([]replayFrameMeta, 0, len(meta.Frames))
	for _, f := range meta.Frames {
		frames = append(frames, replayFrameMeta{
			FrameSeq:     f.FrameSeq,
			TimestampMS:  f.TimestampMS,
			Timestamp:    float64(f.TimestampMS) / 1000.0,
			Lap:          f.Lap,
			ChunkSeq:     f.ChunkSeq,
			FrameInChunk: f.FrameInChunk,
		})
	}
	return &replayCacheEntry{
		sessionID:   meta.SessionID,
		sizeBytes:   estimateReplayIndexBytes(frames, meta.QualiPhases),
		frames:      frames,
		totalLaps:   meta.TotalLaps,
		totalTime:   meta.TotalTime,
		qualiPhases: meta.QualiPhases,
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
