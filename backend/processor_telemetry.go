package main

import (
	"context"
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"

	"f1replaytiming/backend/storage"
)

func (p *GoSessionProcessor) writeTelemetryFiles(ctx context.Context, sessionID int64, drivers []driverMeta, cars map[string][]carSample, pos map[string][]posSample, timing map[string][]timingState) error {
	tw, err := p.store.BeginTelemetryWrite(ctx, sessionID)
	if err != nil {
		return err
	}
	defer tw.Rollback()
	for _, d := range drivers {
		samples := cars[d.Number]
		if len(samples) == 0 {
			continue
		}
		byLap := map[int][]carSample{}
		for _, s := range samples {
			lap := lapAt(timing[d.Number], s.T)
			if lap <= 0 {
				continue
			}
			byLap[lap] = append(byLap[lap], s)
		}
		if len(byLap) == 0 {
			continue
		}
		laps := make([]int, 0, len(byLap))
		for lap := range byLap {
			laps = append(laps, lap)
		}
		sort.Ints(laps)
		for _, lap := range laps {
			arr := byLap[lap]
			step := 1
			if len(arr) > p.telemetryChunkSamples {
				step = len(arr) / p.telemetryChunkSamples
			}
			dist := make([]float64, 0, len(arr)/step+1)
			speed := make([]float64, 0, len(arr)/step+1)
			throttle := make([]float64, 0, len(arr)/step+1)
			brake := make([]int, 0, len(arr)/step+1)
			gear := make([]int, 0, len(arr)/step+1)
			rpm := make([]float64, 0, len(arr)/step+1)
			drs := make([]int, 0, len(arr)/step+1)
			rel := make([]float64, 0, len(arr)/step+1)
			for i := 0; i < len(arr); i += step {
				s := arr[i]
				dist = append(dist, float64(i+1))
				speed = append(speed, round1(s.Speed))
				throttle = append(throttle, round1(s.Throttle))
				if s.Brake {
					brake = append(brake, 100)
				} else {
					brake = append(brake, 0)
				}
				gear = append(gear, s.Gear)
				rpm = append(rpm, round1(s.RPM))
				drs = append(drs, s.DRS)
				if ps := nearestPosSample(pos[d.Number], s.T); ps != nil {
					rel = append(rel, ps.X+ps.Y)
				} else {
					rel = append(rel, 0.0)
				}
			}
			item := map[string]any{
				"driver":            d.Abbr,
				"lap":               lap,
				"distance":          dist,
				"speed":             speed,
				"throttle":          throttle,
				"brake":             brake,
				"gear":              gear,
				"rpm":               rpm,
				"drs":               drs,
				"relative_distance": rel,
			}
			body, err := json.Marshal(item)
			if err != nil {
				return err
			}
			chunkPayload, err := storage.EncodeTelemetryChunk(p.telemetryChunkCodec, storage.ReplaySchemaVersion, d.Abbr, lap, body)
			if err != nil {
				return err
			}
			if err := tw.InsertChunk(ctx, storage.TelemetryChunkRow{
				DriverAbbr: d.Abbr,
				Lap:        lap,
				ChunkSeq:   0,
				Codec:      p.telemetryChunkCodec,
				Payload:    chunkPayload,
			}); err != nil {
				return err
			}
		}
	}
	return tw.Commit(ctx)
}

func extractTimingAppStints(timingApp map[string]any, byNum map[string]driverMeta) map[string][]map[string]any {
	out := map[string][]map[string]any{}
	lines, ok := timingApp["Lines"].(map[string]any)
	if !ok {
		return out
	}
	for key, v := range lines {
		obj, ok := v.(map[string]any)
		if !ok {
			continue
		}
		num := asString(obj["RacingNumber"])
		if num == "" {
			num = key
		}
		abbr := byNum[num].Abbr
		if abbr == "" {
			continue
		}
		stints := normalizeTimingAppStints(obj["Stints"])
		out[abbr] = stints
	}
	return out
}

func normalizeTimingAppStints(raw any) []map[string]any {
	switch v := raw.(type) {
	case []any:
		out := make([]map[string]any, 0, len(v))
		for _, s := range v {
			m, ok := s.(map[string]any)
			if !ok {
				continue
			}
			out = append(out, m)
		}
		return out
	case map[string]any:
		type keyedStint struct {
			Index int
			Item  map[string]any
		}
		keyed := make([]keyedStint, 0, len(v))
		for k, s := range v {
			m, ok := s.(map[string]any)
			if !ok {
				continue
			}
			idx := math.MaxInt32
			if parsed, err := strconv.Atoi(strings.TrimSpace(k)); err == nil {
				idx = parsed
			}
			keyed = append(keyed, keyedStint{Index: idx, Item: m})
		}
		sort.SliceStable(keyed, func(i, j int) bool { return keyed[i].Index < keyed[j].Index })
		out := make([]map[string]any, 0, len(keyed))
		for _, item := range keyed {
			out = append(out, item.Item)
		}
		return out
	default:
		return nil
	}
}

func stintForLap(stints []map[string]any, lap int) (any, any) {
	compound, tyreLife, _, _ := tyreStateForLap(stints, lap)
	return compound, tyreLife
}

type stintState struct {
	StartLap  int
	EndLap    int
	StartLife int
	Compound  string
}

func lapForTyreState(sessionType string, completedLaps int) int {
	sessionType = strings.ToUpper(strings.TrimSpace(sessionType))
	if sessionType == "R" || sessionType == "S" {
		if completedLaps <= 0 {
			return 1
		}
		return completedLaps + 1
	}
	if completedLaps <= 0 {
		return 1
	}
	return completedLaps
}

func tyreStateForLap(stints []map[string]any, lap int) (any, any, []any, int) {
	if lap <= 0 {
		return nil, nil, []any{}, 0
	}
	parsed := parseStintStates(stints)
	if len(parsed) == 0 {
		return nil, nil, []any{}, 0
	}

	idx := -1
	for i := range parsed {
		if parsed[i].StartLap > lap {
			break
		}
		if parsed[i].EndLap > 0 && lap > parsed[i].EndLap {
			continue
		}
		idx = i
	}
	if idx < 0 {
		return nil, nil, []any{}, 0
	}

	current := parsed[idx]
	life := current.StartLife + (lap - current.StartLap + 1)
	if life < 1 {
		life = 1
	}
	history := make([]any, 0, idx)
	for i := 0; i < idx; i++ {
		if parsed[i].Compound != "" {
			history = append(history, parsed[i].Compound)
		}
	}
	return nilIfEmpty(current.Compound), nilIfZero(life), history, idx
}

func parseStintStates(stints []map[string]any) []stintState {
	out := make([]stintState, 0, len(stints))
	nextStartLap := 1
	for _, s := range stints {
		compound := asString(s["Compound"])
		if compound == "" {
			continue
		}
		start := 0
		totalLaps := asInt(s["TotalLaps"])
		if totalLaps > 0 && nextStartLap > 0 {
			start = nextStartLap
		} else {
			start = asInt(s["LapNumber"])
		}
		if start <= 0 {
			continue
		}
		end := 0
		if totalLaps > 0 {
			end = start + totalLaps - 1
			nextStartLap = end + 1
		}
		startLife := asInt(s["StartLaps"])
		if startLife < 0 {
			startLife = 0
		}
		out = append(out, stintState{
			StartLap:  start,
			EndLap:    end,
			StartLife: startLife,
			Compound:  strings.ToUpper(strings.TrimSpace(compound)),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].StartLap == out[j].StartLap {
			return out[i].Compound < out[j].Compound
		}
		return out[i].StartLap < out[j].StartLap
	})
	// De-duplicate repeated start laps, keeping the first entry.
	dedup := out[:0]
	lastStart := -1
	for _, s := range out {
		if s.StartLap == lastStart {
			continue
		}
		dedup = append(dedup, s)
		lastStart = s.StartLap
	}
	// If a stint has no explicit end, cap it by the next stint start.
	for i := 0; i+1 < len(dedup); i++ {
		if dedup[i].EndLap <= 0 {
			dedup[i].EndLap = dedup[i+1].StartLap - 1
		}
	}
	return dedup
}

func nilIfEmpty(v any) any {
	if s := asString(v); s == "" {
		return nil
	}
	return v
}

func nilIfZero(v any) any {
	if asInt(v) == 0 {
		return nil
	}
	return v
}
