package main

import (
	"context"
	"log"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gorilla/websocket"
)

func (a *app) runLiveWebSocket(ctx context.Context, conn *websocket.Conn, year, round int, sessionType string, speed float64) error {
	_ = speed
	pit := a.resolvePitLoss(year, round, sessionType)
	trackNorm, trackPoints := a.findLiveTrackData(year, round, sessionType)
	state := newLiveStateManager(sessionType, pit, trackNorm, trackPoints)

	runID := int64(0)
	if a.store != nil {
		if id, err := a.store.StartLiveStreamRun(ctx, year, round, sessionType); err != nil {
			log.Printf("live run start warning: %v", err)
		} else {
			runID = id
		}
	}

	finalStatus := "stale"
	finalError := ""
	defer func() {
		if a.store != nil && runID > 0 {
			if err := a.store.FinishLiveStreamRun(context.Background(), runID, finalStatus, finalError); err != nil {
				log.Printf("live run finalize warning: %v", err)
			}
		}
	}()

	if err := conn.WriteJSON(map[string]any{
		"type":         "ready",
		"mode":         "live",
		"total_frames": 0,
		"total_time":   0,
		"total_laps":   0,
		"quali_phases": nil,
	}); err != nil {
		return err
	}

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	client := newLiveSignalRClient()
	streamErrCh := make(chan error, 1)
	go func() {
		streamErrCh <- client.stream(streamCtx, func(topic string, data map[string]any, ts float64) {
			state.Process(topic, data, ts)
		})
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-readDone:
			return nil
		case err := <-streamErrCh:
			if err != nil {
				finalStatus = "failed"
				finalError = err.Error()
				return err
			}
			return nil
		case <-ticker.C:
			frame := state.Frame()
			payload := mapWithType("frame", frame)
			if err := conn.WriteJSON(payload); err != nil {
				return err
			}
			if state.SessionEnded() {
				finalStatus = "finished"
				_ = conn.WriteJSON(map[string]any{
					"type":    "finished",
					"message": "Session ended. Full replay with track positions and telemetry will be available shortly.",
				})
				return nil
			}
		}
	}
}

func mapWithType(typ string, payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+1)
	out["type"] = typ
	for k, v := range payload {
		out[k] = v
	}
	return out
}

func (a *app) findLiveTrackData(year, round int, sessionType string) (*liveTrackNorm, []liveTrackPoint) {
	tryRead := func(y, r int, typ string) (*liveTrackNorm, []liveTrackPoint, bool) {
		raw, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(y), strconv.Itoa(r), typ, "track.json"))
		if err != nil {
			return nil, nil, false
		}
		root, ok := raw.(map[string]any)
		if !ok {
			return nil, nil, false
		}
		normRaw, _ := root["norm"].(map[string]any)
		scale := asFloat(normRaw["scale"], 0)
		if scale <= 0 {
			return nil, nil, false
		}
		norm := &liveTrackNorm{
			XMin:  asFloat(normRaw["x_min"], 0),
			YMin:  asFloat(normRaw["y_min"], 0),
			Scale: scale,
		}
		pointsAny, _ := root["track_points"].([]any)
		points := make([]liveTrackPoint, 0, len(pointsAny))
		for _, item := range pointsAny {
			p, _ := item.(map[string]any)
			if p == nil {
				continue
			}
			points = append(points, liveTrackPoint{
				X: asFloat(p["x"], 0),
				Y: asFloat(p["y"], 0),
			})
		}
		return norm, points, true
	}

	if norm, points, ok := tryRead(year, round, sessionType); ok {
		return norm, points
	}
	for _, alt := range sessionTypePriority {
		if alt == sessionType {
			continue
		}
		if norm, points, ok := tryRead(year, round, alt); ok {
			return norm, points
		}
	}
	for prev := year - 1; prev >= year-3; prev-- {
		for _, alt := range []string{"R", "Q"} {
			if norm, points, ok := tryRead(prev, round, alt); ok {
				return norm, points
			}
		}
	}
	return nil, nil
}
