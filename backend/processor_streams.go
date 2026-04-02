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
	"strconv"
	"strings"
)

type lapSectorState struct {
	Time            string
	Color           string
	HasOverall      bool
	OverallFastest  bool
	HasPersonal     bool
	PersonalFastest bool
}

type pendingLapSectorUpdate struct {
	Lap       int
	Index     int
	ExpiresAt float64
	Sectors   [3]lapSectorState
}

func (p *GoSessionProcessor) parseTimingDataStream(ctx context.Context, sessionPath string, feed sessionFeed, driverByNum map[string]driverMeta, timingApp map[string]any, phaseTimeline []qualifyingPhasePoint) (map[string][]timingState, []map[string]any, map[string]timingState, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, nil, nil, err
	}
	defer resp.Body.Close()

	perDriver := map[string][]timingState{}
	current := map[string]timingState{}
	sectorByDriver := map[string][3]lapSectorState{}
	pendingLapByDriver := map[string]pendingLapSectorUpdate{}
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
			prevLap := prev.Lap
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
			pending, hasPending := pendingLapByDriver[racing]
			if hasPending && (ts > pending.ExpiresAt || (next.Lap > 0 && next.Lap > pending.Lap)) {
				delete(pendingLapByDriver, racing)
				hasPending = false
			}

			sectors := sectorByDriver[racing]
			sectorsUpdated := false
			pendingUpdated := false
			if raw, exists := ln["Sectors"]; exists {
				if hasPending && ts <= pending.ExpiresAt && next.Lap == pending.Lap {
					var changed bool
					pending.Sectors, changed = mergeSectorStates(pending.Sectors, raw)
					if changed {
						pendingLapByDriver[racing] = pending
						pendingUpdated = true
					}
				} else {
					var changed bool
					sectors, changed = mergeSectorStates(sectors, raw)
					sectorsUpdated = changed
				}
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
					phase := qualifyingPhaseAt(phaseTimeline, ts)
					lapOut := map[string]any{
						"driver":           meta.Abbr,
						"lap_number":       next.Lap,
						"position":         next.Position,
						"lap_time":         lastLapTime,
						"time":             round3(ts),
						"qualifying_phase": phase,
						"compound":         compound,
						"tyre_life":        tyreLife,
						"pit_in":           next.InPit,
						"pit_out":          next.PitOut,
					}
					assignLapSectorFields(lapOut, sectors, true)
					lapsOut = append(lapsOut, lapOut)
					pending = pendingLapSectorUpdate{
						Lap:       next.Lap,
						Index:     len(lapsOut) - 1,
						ExpiresAt: ts + 5.0,
						Sectors:   sectors,
					}
					pendingLapByDriver[racing] = pending
					hasPending = true
				}
			}

			if hasPending && (pendingUpdated || sectorsUpdated) {
				if idx := pending.Index; idx >= 0 && idx < len(lapsOut) {
					assignLapSectorFields(lapsOut[idx], pending.Sectors, false)
				}
			}
			// Prevent stale previous-lap values from leaking into the next lap.
			if prevLap > 0 && next.Lap > prevLap {
				sectorByDriver[racing] = [3]lapSectorState{}
			} else {
				sectorByDriver[racing] = sectors
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

func assignLapSectorFields(dst map[string]any, sectors [3]lapSectorState, overwrite bool) {
	timeKeys := []string{"sector1", "sector2", "sector3"}
	colorKeys := []string{"sector1_color", "sector2_color", "sector3_color"}
	for i := 0; i < 3; i++ {
		sector := sectors[i]
		if overwrite {
			dst[timeKeys[i]] = nilIfEmptyString(sector.Time)
			dst[colorKeys[i]] = nilIfEmptyString(sector.Color)
			continue
		}
		if strings.TrimSpace(sector.Time) != "" {
			dst[timeKeys[i]] = sector.Time
		}
		if strings.TrimSpace(sector.Color) != "" {
			dst[colorKeys[i]] = sector.Color
		}
	}
}

func mergeSectorStates(base [3]lapSectorState, raw any) ([3]lapSectorState, bool) {
	updated := false
	assign := func(index int, value any) {
		if index < 0 || index >= len(base) {
			return
		}
		sector := base[index]
		if parsed := parseSectorTimeValue(value); parsed != "" && parsed != sector.Time {
			sector.Time = parsed
			updated = true
		}
		if m, ok := value.(map[string]any); ok {
			prevColor := sector.Color
			overallUpdated := false
			personalUpdated := false
			if rawOverall, exists := m["OverallFastest"]; exists {
				sector.HasOverall = true
				sector.OverallFastest = asBool(rawOverall)
				overallUpdated = true
			}
			if rawPersonal, exists := m["PersonalFastest"]; exists {
				sector.HasPersonal = true
				sector.PersonalFastest = asBool(rawPersonal)
				personalUpdated = true
			}

			// Overall-fastest implies personal-fastest for that sector sample.
			if sector.HasOverall && sector.OverallFastest && !personalUpdated && !sector.HasPersonal {
				sector.HasPersonal = true
				sector.PersonalFastest = true
			}
			// When purple is taken away, official stream can emit only OverallFastest=false.
			// Keep that sector green unless PersonalFastest is explicitly false.
			if overallUpdated && !sector.OverallFastest && !personalUpdated && prevColor == "purple" {
				sector.HasPersonal = true
				sector.PersonalFastest = true
			}

			nextColor := resolveSectorColor(prevColor, sector)
			if nextColor != sector.Color {
				sector.Color = nextColor
				updated = true
			}
		}
		base[index] = sector
	}

	switch v := raw.(type) {
	case []any:
		for idx, item := range v {
			assign(idx, item)
		}
	case map[string]any:
		keys := make([]int, 0, len(v))
		items := make(map[int]any, len(v))
		for key, item := range v {
			idx, err := strconv.Atoi(strings.TrimSpace(key))
			if err != nil {
				continue
			}
			keys = append(keys, idx)
			items[idx] = item
		}
		sort.Ints(keys)
		for _, idx := range keys {
			assign(idx, items[idx])
		}
	}

	return base, updated
}

func resolveSectorColor(prevColor string, sector lapSectorState) string {
	if sector.HasOverall && sector.OverallFastest {
		return "purple"
	}
	if sector.HasPersonal && sector.PersonalFastest {
		return "green"
	}
	if sector.HasPersonal && !sector.PersonalFastest {
		return "yellow"
	}
	if sector.HasOverall && !sector.OverallFastest {
		if prevColor == "purple" || prevColor == "green" {
			return "green"
		}
		return prevColor
	}
	return prevColor
}

func mergeSectorTimes(base [3]string, raw any) [3]string {
	assign := func(index int, value any) {
		if index < 0 || index >= len(base) {
			return
		}
		sector := parseSectorTimeValue(value)
		if sector != "" {
			base[index] = sector
		}
	}

	switch v := raw.(type) {
	case []any:
		for idx, item := range v {
			assign(idx, item)
		}
	case map[string]any:
		keys := make([]int, 0, len(v))
		items := make(map[int]any, len(v))
		for key, item := range v {
			idx, err := strconv.Atoi(strings.TrimSpace(key))
			if err != nil {
				continue
			}
			keys = append(keys, idx)
			items[idx] = item
		}
		sort.Ints(keys)
		for _, idx := range keys {
			assign(idx, items[idx])
		}
	}

	return base
}

func parseSectorTimeValue(raw any) string {
	switch v := raw.(type) {
	case map[string]any:
		val := strings.TrimSpace(asString(v["Value"]))
		if val == "" {
			return ""
		}
		if _, ok := parseLapTimeToSeconds(val); !ok {
			return ""
		}
		return val
	default:
		val := strings.TrimSpace(asString(raw))
		if val == "" {
			return ""
		}
		if _, ok := parseLapTimeToSeconds(val); !ok {
			return ""
		}
		return val
	}
}

func nilIfEmptyString(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func (p *GoSessionProcessor) parseSessionDataStream(ctx context.Context, sessionPath string, feed sessionFeed) ([]qualifyingPhasePoint, error) {
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(feed.StreamPath, "/"))
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	points := make([]qualifyingPhasePoint, 0, 16)
	lastPart := 0

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

		seriesRaw := root["Series"]
		if seriesRaw == nil {
			continue
		}

		entries := make([]map[string]any, 0, 4)
		switch v := seriesRaw.(type) {
		case []any:
			for _, item := range v {
				if m, ok := item.(map[string]any); ok {
					entries = append(entries, m)
				}
			}
		case map[string]any:
			keys := make([]string, 0, len(v))
			for k := range v {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				if m, ok := v[k].(map[string]any); ok {
					entries = append(entries, m)
				}
			}
		}
		if len(entries) == 0 {
			continue
		}

		part := 0
		for i := len(entries) - 1; i >= 0; i-- {
			if qp := asInt(entries[i]["QualifyingPart"]); qp > 0 {
				part = qp
				break
			}
		}
		if part <= 0 || part == lastPart {
			continue
		}
		points = append(points, qualifyingPhasePoint{T: ts, Part: part})
		lastPart = part
	}
	if err := sc.Err(); err != nil && !errors.Is(err, io.EOF) {
		return nil, err
	}
	return points, nil
}

func qualifyingPhaseAt(points []qualifyingPhasePoint, t float64) any {
	if len(points) == 0 {
		return nil
	}
	idx := sort.Search(len(points), func(i int) bool { return points[i].T > t }) - 1
	if idx < 0 {
		return nil
	}
	part := points[idx].Part
	if part <= 0 {
		return nil
	}
	return fmt.Sprintf("Q%d", part)
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
