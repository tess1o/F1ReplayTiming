package main

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"f1replaytiming/backend/storage"
)

func (p *GoSessionProcessor) writeReplayFromTimelines(ctx context.Context, sessionID int64, drivers []driverMeta, timing map[string][]timingState, positions map[string][]posSample, cars map[string][]carSample, trackStatuses []trackStatusPoint, weather []weatherPoint, rc []raceControlPoint, sessionType string, window *replayWindow, stintsByAbbr map[string][]map[string]any) (map[string]any, error) {
	var tMin float64
	var tMax float64
	hasRange := false
	for _, arr := range positions {
		if len(arr) == 0 {
			continue
		}
		if !hasRange || arr[0].T < tMin {
			tMin = arr[0].T
		}
		if !hasRange || arr[len(arr)-1].T > tMax {
			tMax = arr[len(arr)-1].T
		}
		hasRange = true
	}
	if !hasRange {
		return nil, errors.New("no replay positions available")
	}
	startT := tMin
	endT := tMax
	if window != nil {
		if window.Start > startT && window.Start < endT {
			startT = window.Start
		}
		if window.End > startT && window.End < endT {
			endT = window.End
		}
	}
	if endT <= startT {
		return nil, errors.New("invalid replay time window")
	}
	rw, err := p.store.BeginReplayWrite(ctx, sessionID, storage.ReplaySchemaVersion)
	if err != nil {
		return nil, err
	}
	defer rw.Rollback()

	// Use the same normalization baseline as track.json to keep dots glued to the map.
	refTrack := pickRepresentativeTrackLap(positions, timing)
	xMin, yMin, scale := boundsForTrack(refTrack)
	totalLaps := 0
	for _, arr := range timing {
		if len(arr) == 0 {
			continue
		}
		l := arr[len(arr)-1].Lap
		if l > totalLaps {
			totalLaps = l
		}
	}
	abbrToNumber := make(map[string]string, len(drivers))
	hasCarData := make(map[string]bool, len(drivers))
	for _, d := range drivers {
		abbrToNumber[d.Abbr] = d.Number
		hasCarData[d.Number] = len(cars[d.Number]) > 0
	}
	chunkFrames := make([]*storage.ReplayFramePayload, 0, p.replayChunkFrames)
	chunkIndex := make([]storage.ReplayFrameIndexRow, 0, p.replayChunkFrames)
	allIndex := make([]storage.ReplayFrameIndexRow, 0, 4096)
	frameSeq := 0
	chunkSeq := 0
	totalTime := 0.0
	flushChunk := func() error {
		if len(chunkFrames) == 0 {
			return nil
		}
		payload, err := storage.EncodeReplayChunk(p.replayChunkCodec, storage.ReplaySchemaVersion, chunkFrames)
		if err != nil {
			return err
		}
		row := storage.ReplayChunkRow{
			ChunkSeq:   chunkSeq,
			StartTSMS:  chunkFrames[0].TimestampMs,
			EndTSMS:    chunkFrames[len(chunkFrames)-1].TimestampMs,
			FrameCount: len(chunkFrames),
			Codec:      p.replayChunkCodec,
			Payload:    payload,
		}
		if err := rw.InsertChunk(ctx, row); err != nil {
			return err
		}
		for _, idx := range chunkIndex {
			if err := rw.InsertFrameIndex(ctx, idx); err != nil {
				return err
			}
			allIndex = append(allIndex, idx)
		}
		chunkSeq++
		chunkFrames = chunkFrames[:0]
		chunkIndex = chunkIndex[:0]
		return nil
	}

	for t := startT; t <= endT; t += p.sampleEvery {
		driverRows := make([]map[string]any, 0, len(drivers))
		for _, d := range drivers {
			ps := nearestPosSampleWithin(positions[d.Number], t, 10.0)
			ts := latestTimingAt(timing[d.Number], t)
			cs := latestCarAt(cars[d.Number], t)
			row := map[string]any{
				"abbr":              d.Abbr,
				"x":                 0.0,
				"y":                 0.0,
				"color":             d.Color,
				"team":              d.Team,
				"position":          nil,
				"grid_position":     nil,
				"pit_start":         false,
				"in_pit":            false,
				"pit_time":          nil,
				"finished":          false,
				"compound":          nil,
				"tyre_life":         nil,
				"pit_stops":         0,
				"has_fastest_lap":   false,
				"flag":              nil,
				"gap":               nil,
				"interval":          nil,
				"best_lap_time":     nil,
				"no_timing":         true,
				"retired":           false,
				"relative_distance": 0.0,
				"speed":             0.0,
				"throttle":          0.0,
				"brake":             false,
				"gear":              0,
				"rpm":               0.0,
				"drs":               0,
				"tyre_history":      []any{},
			}
			if ps != nil {
				row["x"] = normalizeCoord(ps.X, xMin, scale)
				row["y"] = normalizeCoord(ps.Y, yMin, scale)
			}
			if ts != nil {
				row["position"] = zeroToNil(ts.Position)
				row["gap"] = ts.Gap
				row["interval"] = ts.Interval
				row["in_pit"] = ts.InPit
				row["retired"] = ts.Retired
				row["no_timing"] = false
				lapForTyre := lapForTyreState(sessionType, ts.Lap)
				compound, tyreLife, tyreHistory, pitStops := tyreStateForLap(stintsByAbbr[d.Abbr], lapForTyre)
				row["compound"] = compound
				row["tyre_life"] = tyreLife
				row["tyre_history"] = tyreHistory
				row["pit_stops"] = pitStops
			}
			if cs != nil {
				row["speed"] = round1(cs.Speed)
				row["throttle"] = round1(cs.Throttle)
				row["brake"] = cs.Brake
				row["gear"] = cs.Gear
				row["rpm"] = round1(cs.RPM)
				row["drs"] = cs.DRS
			}
			if !hasCarData[d.Number] {
				applyNoCarDataFallback(row, sessionType, t-startT)
			}
			driverRows = append(driverRows, row)
		}
		sort.Slice(driverRows, func(i, j int) bool {
			pi := asInt(driverRows[i]["position"])
			pj := asInt(driverRows[j]["position"])
			if pi == 0 {
				pi = 999
			}
			if pj == 0 {
				pj = 999
			}
			if pi == pj {
				return asString(driverRows[i]["abbr"]) < asString(driverRows[j]["abbr"])
			}
			return pi < pj
		})
		leadAbbr := ""
		if len(driverRows) > 0 {
			leadAbbr = asString(driverRows[0]["abbr"])
		}
		curLap := currentLapFromLeader(sessionType, totalLaps, leadAbbr, abbrToNumber, timing, t)
		frame := map[string]any{
			"timestamp":    round3(t - startT),
			"lap":          curLap,
			"total_laps":   totalLaps,
			"session_type": sessionType,
			"drivers":      driverRows,
			"status":       trackStatusAt(trackStatuses, t),
		}
		if wv := weatherAt(weather, t); wv != nil {
			frame["weather"] = wv
		}
		if len(rc) > 0 {
			msgs := raceControlAt(rc, t)
			if len(msgs) > 0 {
				frame["rc_messages"] = msgs
			}
		}
		frameRaw, err := json.Marshal(frame)
		if err != nil {
			return nil, err
		}
		frameTS := round3(t - startT)
		totalTime = frameTS
		tsMS := int64(math.Round(frameTS * 1000.0))
		chunkFrames = append(chunkFrames, &storage.ReplayFramePayload{
			TimestampMs: tsMS,
			Lap:         int32(curLap),
			FrameJson:   frameRaw,
		})
		chunkIndex = append(chunkIndex, storage.ReplayFrameIndexRow{
			FrameSeq:     frameSeq,
			TimestampMS:  tsMS,
			Lap:          curLap,
			ChunkSeq:     chunkSeq,
			FrameInChunk: len(chunkFrames) - 1,
		})
		frameSeq++
		if len(chunkFrames) >= p.replayChunkFrames {
			if err := flushChunk(); err != nil {
				return nil, err
			}
		}
	}
	if err := flushChunk(); err != nil {
		return nil, err
	}
	if err := rw.Commit(ctx, totalLaps, totalTime, []map[string]any{}); err != nil {
		return nil, err
	}
	idxOut := make([]map[string]any, 0, len(allIndex))
	for _, row := range allIndex {
		idxOut = append(idxOut, map[string]any{
			"frame_seq":      row.FrameSeq,
			"ts_ms":          row.TimestampMS,
			"lap":            row.Lap,
			"chunk_seq":      row.ChunkSeq,
			"frame_in_chunk": row.FrameInChunk,
		})
	}
	return map[string]any{
		"version":         storage.ReplaySchemaVersion,
		"replay_size":     len(allIndex),
		"replay_mod_unix": time.Now().Unix(),
		"frames":          idxOut,
		"total_time":      totalTime,
		"total_laps":      totalLaps,
		"quali_phases":    []map[string]any{},
	}, nil
}

func applyNoCarDataFallback(row map[string]any, sessionType string, elapsed float64) {
	if row == nil {
		return
	}
	// Keep no-car drivers off the track map: they never had usable telemetry.
	row["x"] = 0.0
	row["y"] = 0.0
	row["relative_distance"] = 0.0
	row["position"] = nil
	row["gap"] = nil
	row["interval"] = nil

	sessionType = strings.ToUpper(strings.TrimSpace(sessionType))
	isRace := sessionType == "R" || sessionType == "S"
	if isRace && elapsed >= 10 {
		row["retired"] = true
		row["no_timing"] = false
		return
	}
	row["no_timing"] = true
}

func currentLapFromLeader(sessionType string, totalLaps int, leaderAbbr string, abbrToNumber map[string]string, timing map[string][]timingState, t float64) int {
	sessionType = strings.ToUpper(strings.TrimSpace(sessionType))
	if leaderAbbr == "" {
		return 1
	}
	number := abbrToNumber[leaderAbbr]
	if number == "" {
		return 1
	}
	st := latestTimingAt(timing[number], t)
	if st == nil {
		return 1
	}
	// Prefer explicit lap from leader gap string (e.g. "LAP 17"), which matches
	// UI semantics directly when available.
	if lapFromGap := parseLapFromGapString(st.Gap); lapFromGap > 0 {
		return lapFromGap
	}
	if st.Lap <= 0 {
		return 1
	}
	if sessionType == "R" || sessionType == "S" {
		// TimingData NumberOfLaps is completed laps for race/sprint, but the UI shows
		// the current lap in progress.
		lap := st.Lap + 1
		if totalLaps > 0 && lap > totalLaps {
			lap = totalLaps
		}
		if lap < 1 {
			return 1
		}
		return lap
	}
	return st.Lap
}

func parseLapFromGapString(gap string) int {
	gap = strings.TrimSpace(strings.ToUpper(gap))
	if !strings.HasPrefix(gap, "LAP") {
		return 0
	}
	rest := strings.TrimSpace(strings.TrimPrefix(gap, "LAP"))
	if rest == "" {
		return 0
	}
	n, err := strconv.Atoi(rest)
	if err != nil || n <= 0 {
		return 0
	}
	return n
}
