package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"f1replaytiming/backend/storage"
	"github.com/gorilla/websocket"
)

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
	if a.store == nil {
		_ = send(map[string]any{"type": "error", "message": "Storage is not initialized"})
		return
	}
	type cachedReplayChunk struct {
		payloads [][]byte
	}
	chunkCache := make(map[int]*cachedReplayChunk, 8)

	pitLoss := a.resolvePitLoss(year, round, sessionType)
	isRace := sessionType == "R" || sessionType == "S"

	_ = send(map[string]any{
		"type":         "ready",
		"total_frames": len(entry.frames),
		"total_time":   entry.totalTime,
		"total_laps":   entry.totalLaps,
		"quali_phases": entry.qualiPhases,
	})

	readFrameRaw := func(meta replayFrameMeta) (json.RawMessage, error) {
		cached, ok := chunkCache[meta.ChunkSeq]
		if !ok {
			payload, codec, err := a.store.GetReplayChunkPayload(r.Context(), entry.sessionID, meta.ChunkSeq)
			if err != nil {
				return nil, err
			}
			decoded, err := storage.DecodeReplayChunk(payload, codec)
			if err != nil {
				return nil, err
			}
			cached = &cachedReplayChunk{payloads: make([][]byte, 0, len(decoded.Frames))}
			for _, frame := range decoded.Frames {
				prepared, err := prepareFramePayload(frame.FrameJson, isRace, pitLoss)
				if err != nil {
					prepared = wrapFrameRaw(frame.FrameJson)
				}
				cached.payloads = append(cached.payloads, prepared)
			}
			if len(chunkCache) >= 8 {
				for k := range chunkCache {
					delete(chunkCache, k)
					break
				}
			}
			chunkCache[meta.ChunkSeq] = cached
		}
		if meta.FrameInChunk < 0 || meta.FrameInChunk >= len(cached.payloads) {
			return nil, errors.New("frame index out of bounds")
		}
		return cached.payloads[meta.FrameInChunk], nil
	}

	if firstRaw, err := readFrameRaw(entry.frames[0]); err == nil {
		_ = conn.WriteMessage(websocket.TextMessage, firstRaw)
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
		frameRaw, err := readFrameRaw(entry.frames[i])
		if err != nil {
			return false
		}
		_ = conn.WriteMessage(websocket.TextMessage, frameRaw)
		return true
	}

	findByTime := func(target float64) int {
		targetMS := int64(math.Round(target * 1000.0))
		idx := sort.Search(len(entry.frames), func(i int) bool {
			return entry.frames[i].TimestampMS >= targetMS
		})
		if idx >= len(entry.frames) {
			return len(entry.frames) - 1
		}
		return idx
	}
	findByLap := func(target int) int {
		idx := sort.Search(len(entry.frames), func(i int) bool {
			return entry.frames[i].Lap >= target
		})
		if idx >= len(entry.frames) {
			return len(entry.frames) - 1
		}
		return idx
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

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	_ = source
	speedF, _ := strconv.ParseFloat(speed, 64)
	if speedF <= 0 {
		speedF = 10
	}

	if err := conn.WriteJSON(map[string]any{"type": "status", "message": "Connecting to live timing stream..."}); err != nil {
		return
	}
	if err := a.runLiveWebSocket(ctx, conn, year, round, sessionType, speedF); err != nil {
		_ = conn.WriteJSON(map[string]any{"type": "error", "message": err.Error()})
	}
}
