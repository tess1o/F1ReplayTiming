package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strings"
)

func (p *GoSessionProcessor) parseTimingDataStream(ctx context.Context, sessionPath string, feed sessionFeed, driverByNum map[string]driverMeta, timingApp map[string]any) (map[string][]timingState, []map[string]any, map[string]timingState, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()

	perDriver := map[string][]timingState{}
	current := map[string]timingState{}
	lapSeen := map[string]map[int]struct{}{}
	lapsOut := make([]map[string]any, 0, 4096)
	stintMap := extractTimingAppStints(timingApp, driverByNum)

	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 256*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if len(line) < 13 {
			continue
		}
		ts, ok := parseStreamTimestamp(line[:12])
		if !ok {
			continue
		}
		payload := strings.TrimSpace(line[12:])
		var root map[string]any
		if err := json.Unmarshal([]byte(payload), &root); err != nil {
			continue
		}
		linesAny, ok := root["Lines"].(map[string]any)
		if !ok {
			continue
		}
		for key, lineRaw := range linesAny {
			ln, ok := lineRaw.(map[string]any)
			if !ok {
				continue
			}
			racing := asString(ln["RacingNumber"])
			if racing == "" {
				racing = key
			}
			prev := current[racing]
			next := prev
			next.T = ts
			if v := asInt(ln["NumberOfLaps"]); v > 0 {
				next.Lap = v
			}
			if v := asInt(ln["Position"]); v > 0 {
				next.Position = v
			}
			if v := asString(ln["GapToLeader"]); v != "" || ln["GapToLeader"] == "" {
				next.Gap = v
			}
			if iv, ok := ln["IntervalToPositionAhead"].(map[string]any); ok {
				next.Interval = asString(iv["Value"])
			}
			if v, ok := ln["InPit"].(bool); ok {
				next.InPit = v
			}
			if v, ok := ln["PitOut"].(bool); ok {
				next.PitOut = v
			}
			if v, ok := ln["Retired"].(bool); ok {
				next.Retired = v
			}
			current[racing] = next
			perDriver[racing] = append(perDriver[racing], next)

			lastLapTime := ""
			if ll, ok := ln["LastLapTime"].(map[string]any); ok {
				lastLapTime = strings.TrimSpace(asString(ll["Value"]))
			}
			if lastLapTime != "" && next.Lap > 0 {
				seen, ok := lapSeen[racing]
				if !ok {
					seen = map[int]struct{}{}
					lapSeen[racing] = seen
				}
				if _, exists := seen[next.Lap]; !exists {
					seen[next.Lap] = struct{}{}
					meta := driverByNum[racing]
					compound, tyreLife := stintForLap(stintMap[meta.Abbr], next.Lap)
					lapsOut = append(lapsOut, map[string]any{
						"driver":     meta.Abbr,
						"lap_number": next.Lap,
						"position":   next.Position,
						"lap_time":   lastLapTime,
						"time":       nil,
						"sector1":    nil,
						"sector2":    nil,
						"sector3":    nil,
						"compound":   compound,
						"tyre_life":  tyreLife,
						"pit_in":     next.InPit,
						"pit_out":    next.PitOut,
					})
				}
			}
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, nil, nil, err
	}
	sort.Slice(lapsOut, func(i, j int) bool {
		di := asString(lapsOut[i]["driver"])
		dj := asString(lapsOut[j]["driver"])
		if di != dj {
			return di < dj
		}
		return asInt(lapsOut[i]["lap_number"]) < asInt(lapsOut[j]["lap_number"])
	})
	return perDriver, lapsOut, current, nil
}

func (p *GoSessionProcessor) parsePositionStream(ctx context.Context, sessionPath string, feed sessionFeed) (map[string][]posSample, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out := map[string][]posSample{}
	lastAccepted := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 256*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if len(line) < 13 {
			continue
		}
		ts, ok := parseStreamTimestamp(line[:12])
		if !ok {
			continue
		}
		payload := strings.TrimSpace(line[12:])
		raw, err := decodeMaybeZStreamPayload(payload)
		if err != nil {
			continue
		}
		var root map[string]any
		if err := json.Unmarshal(raw, &root); err != nil {
			continue
		}
		posList, ok := root["Position"].([]any)
		if !ok {
			continue
		}
		for _, item := range posList {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			entries, ok := obj["Entries"].(map[string]any)
			if !ok {
				continue
			}
			for racing, val := range entries {
				ev, ok := val.(map[string]any)
				if !ok {
					continue
				}
				if prev, ok := lastAccepted[racing]; ok && p.rawMinDelta > 0 && (ts-prev) < p.rawMinDelta {
					continue
				}
				x := asFloat(ev["X"], 0)
				y := asFloat(ev["Y"], 0)
				// Position.z uses explicit (0,0) placeholders for absent points.
				// Treat these as missing to avoid rendering parked/retired cars at
				// an artificial normalized map location.
				if x == 0 && y == 0 {
					continue
				}
				out[racing] = append(out[racing], posSample{T: ts, X: x, Y: y})
				lastAccepted[racing] = ts
			}
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	for k := range out {
		sort.Slice(out[k], func(i, j int) bool { return out[k][i].T < out[k][j].T })
	}
	return out, nil
}

func (p *GoSessionProcessor) parseCarDataStream(ctx context.Context, sessionPath string, feed sessionFeed) (map[string][]carSample, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out := map[string][]carSample{}
	lastAccepted := map[string]float64{}
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 256*1024), 8*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if len(line) < 13 {
			continue
		}
		ts, ok := parseStreamTimestamp(line[:12])
		if !ok {
			continue
		}
		payload := strings.TrimSpace(line[12:])
		raw, err := decodeMaybeZStreamPayload(payload)
		if err != nil {
			continue
		}
		var root map[string]any
		if err := json.Unmarshal(raw, &root); err != nil {
			continue
		}
		list, ok := root["Entries"].([]any)
		if !ok {
			continue
		}
		for _, item := range list {
			obj, ok := item.(map[string]any)
			if !ok {
				continue
			}
			cars, ok := obj["Cars"].(map[string]any)
			if !ok {
				continue
			}
			for racing, carAny := range cars {
				if prev, ok := lastAccepted[racing]; ok && p.rawMinDelta > 0 && (ts-prev) < p.rawMinDelta {
					continue
				}
				carObj, ok := carAny.(map[string]any)
				if !ok {
					continue
				}
				channels, ok := carObj["Channels"].(map[string]any)
				if !ok {
					continue
				}
				c := carSample{
					T:        ts,
					RPM:      asFloat(channels["0"], 0),
					Speed:    asFloat(channels["2"], 0),
					Gear:     asInt(channels["3"]),
					Throttle: asFloat(channels["4"], 0),
					Brake:    asInt(channels["5"]) > 0,
					DRS:      asInt(channels["45"]),
				}
				out[racing] = append(out[racing], c)
				lastAccepted[racing] = ts
			}
		}
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	for k := range out {
		sort.Slice(out[k], func(i, j int) bool { return out[k][i].T < out[k][j].T })
	}
	return out, nil
}

func (p *GoSessionProcessor) parseTrackStatusStream(ctx context.Context, sessionPath string, feed sessionFeed) ([]trackStatusPoint, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out := make([]trackStatusPoint, 0, 128)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if len(line) < 13 {
			continue
		}
		ts, ok := parseStreamTimestamp(line[:12])
		if !ok {
			continue
		}
		payload := strings.TrimSpace(line[12:])
		var root map[string]any
		if err := json.Unmarshal([]byte(payload), &root); err != nil {
			continue
		}
		out = append(out, trackStatusPoint{T: ts, Status: asString(root["Status"])})
	}
	return out, nil
}

func (p *GoSessionProcessor) parseWeatherStream(ctx context.Context, sessionPath string, feed sessionFeed) ([]weatherPoint, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	out := make([]weatherPoint, 0, 256)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if len(line) < 13 {
			continue
		}
		ts, ok := parseStreamTimestamp(line[:12])
		if !ok {
			continue
		}
		payload := strings.TrimSpace(line[12:])
		var root map[string]any
		if err := json.Unmarshal([]byte(payload), &root); err != nil {
			continue
		}
		out = append(out, weatherPoint{T: ts, Values: map[string]any{
			"air_temp":       round1(asFloat(root["AirTemp"], 0)),
			"track_temp":     round1(asFloat(root["TrackTemp"], 0)),
			"humidity":       math.Round(asFloat(root["Humidity"], 0)),
			"rainfall":       asString(root["Rainfall"]) == "1",
			"wind_speed":     round1(asFloat(root["WindSpeed"], 0)),
			"wind_direction": asInt(root["WindDirection"]),
		}})
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return out, nil
}

func (p *GoSessionProcessor) parseSessionStatusStream(ctx context.Context, sessionPath string, feed sessionFeed) ([]sessionStatusPoint, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	out := make([]sessionStatusPoint, 0, 32)
	sc := bufio.NewScanner(resp.Body)
	sc.Buffer(make([]byte, 0, 64*1024), 2*1024*1024)
	for sc.Scan() {
		line := strings.TrimSpace(strings.TrimPrefix(sc.Text(), "\ufeff"))
		if len(line) < 13 {
			continue
		}
		ts, ok := parseStreamTimestamp(line[:12])
		if !ok {
			continue
		}
		payload := strings.TrimSpace(line[12:])
		var root map[string]any
		if err := json.Unmarshal([]byte(payload), &root); err != nil {
			continue
		}
		status := strings.TrimSpace(asString(root["Status"]))
		if status == "" {
			continue
		}
		out = append(out, sessionStatusPoint{T: ts, Status: status})
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return out, nil
}

func deriveReplayWindow(points []sessionStatusPoint, sessionType string) *replayWindow {
	if len(points) == 0 {
		return nil
	}
	sessionType = strings.ToUpper(strings.TrimSpace(sessionType))
	isRaceLike := sessionType == "R" || sessionType == "S"

	start := 0.0
	hasStart := false

	for _, pt := range points {
		s := strings.ToLower(strings.TrimSpace(pt.Status))
		switch s {
		case "started", "resumed":
			if !hasStart {
				start = pt.T
				hasStart = true
			}
		}
	}

	if !hasStart {
		return nil
	}

	end := 0.0
	hasEnd := false
	for _, pt := range points {
		if pt.T <= start {
			continue
		}
		s := strings.ToLower(strings.TrimSpace(pt.Status))
		if !isTerminalSessionStatus(s) {
			continue
		}
		if isRaceLike {
			return &replayWindow{Start: start, End: pt.T}
		}
		end = pt.T
		hasEnd = true
	}
	if hasEnd && end > start {
		return &replayWindow{Start: start, End: end}
	}
	return &replayWindow{Start: start}
}

func isTerminalSessionStatus(status string) bool {
	switch status {
	case "finished", "finalised", "aborted", "ended", "ends":
		return true
	default:
		return false
	}
}
