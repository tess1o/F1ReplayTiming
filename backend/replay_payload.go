package main

import (
	"bytes"
	"encoding/json"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

func (a *app) resolvePitLoss(year, round int, sessionType string) pitLossValues {
	if !(sessionType == "R" || sessionType == "S") {
		return pitLossValues{}
	}

	a.pitLossMu.Lock()
	if a.pitLossRaw == nil || time.Since(a.pitLossLoadedAt) > 10*time.Minute {
		if raw, err := a.readJSONAny("pit_loss.json"); err == nil {
			if m, ok := raw.(map[string]any); ok {
				a.pitLossRaw = m
				a.pitLossLoadedAt = time.Now()
			}
		}
	}
	pit := a.pitLossRaw
	a.pitLossMu.Unlock()

	if pit == nil {
		return pitLossValues{Green: 22.0, SC: 10.0, VSC: 14.5}
	}
	eventName := ""
	if infoRaw, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "info.json")); err == nil {
		if info, ok := infoRaw.(map[string]any); ok {
			eventName = asString(info["event_name"])
		}
	}

	if circuits, ok := pit["circuits"].(map[string]any); ok && eventName != "" {
		if entryAny, ok := circuits[eventName]; ok {
			if entry, ok := entryAny.(map[string]any); ok {
				return pitLossValues{
					Green: asFloat(entry["pit_loss_green"], 22.0),
					SC:    asFloat(entry["pit_loss_sc"], 10.0),
					VSC:   asFloat(entry["pit_loss_vsc"], 14.5),
				}
			}
		}
	}
	if ga, ok := pit["global_averages"].(map[string]any); ok {
		return pitLossValues{
			Green: asFloat(ga["green"], 22.0),
			SC:    asFloat(ga["sc"], 10.0),
			VSC:   asFloat(ga["vsc"], 14.5),
		}
	}
	return pitLossValues{Green: 22.0, SC: 10.0, VSC: 14.5}
}

func prepareFramePayload(raw json.RawMessage, isRace bool, pit pitLossValues) ([]byte, error) {
	if !isRace || pit.Green <= 0 {
		return wrapFrameRaw(raw), nil
	}
	var frame map[string]any
	if err := json.Unmarshal(raw, &frame); err != nil {
		return wrapFrameRaw(raw), nil
	}
	addPitPredictions(frame, pit)
	b, err := json.Marshal(frame)
	if err != nil {
		return nil, err
	}
	return wrapFrameRaw(b), nil
}

func wrapFrameRaw(raw []byte) []byte {
	raw = bytes.TrimSpace(raw)
	if len(raw) == 0 || raw[0] != '{' {
		fallback, _ := json.Marshal(map[string]any{"type": "frame"})
		return fallback
	}
	out := make([]byte, 0, len(raw)+16)
	out = append(out, []byte(`{"type":"frame",`)...)
	out = append(out, raw[1:]...)
	return out
}

func addPitPredictions(frame map[string]any, pit pitLossValues) {
	lap := asInt(frame["lap"])
	if lap < 5 {
		return
	}
	status := asString(frame["status"])
	selected := pit.Green
	if status == "sc" {
		selected = pit.SC
	} else if status == "vsc" {
		selected = pit.VSC
	}
	driversAny, ok := frame["drivers"].([]any)
	if !ok {
		return
	}
	type pair struct {
		abbr string
		gap  float64
	}
	gaps := make([]pair, 0, len(driversAny))
	for _, dAny := range driversAny {
		d, ok := dAny.(map[string]any)
		if !ok {
			continue
		}
		if asBool(d["retired"]) || asBool(d["in_pit"]) {
			continue
		}
		abbr := asString(d["abbr"])
		if abbr == "" {
			continue
		}
		if asInt(d["position"]) == 1 {
			gaps = append(gaps, pair{abbr: abbr, gap: 0})
			continue
		}
		if g, ok := parseGap(asString(d["gap"])); ok {
			gaps = append(gaps, pair{abbr: abbr, gap: g})
		}
	}
	if len(gaps) == 0 {
		return
	}
	sort.Slice(gaps, func(i, j int) bool { return gaps[i].gap < gaps[j].gap })

	for _, dAny := range driversAny {
		d, ok := dAny.(map[string]any)
		if !ok {
			continue
		}
		if asBool(d["retired"]) || asBool(d["in_pit"]) {
			d["pit_prediction"] = nil
			continue
		}
		abbr := asString(d["abbr"])
		if abbr == "" {
			d["pit_prediction"] = nil
			continue
		}
		current := 0.0
		if asInt(d["position"]) == 1 {
			current = 0
		} else {
			g, ok := parseGap(asString(d["gap"]))
			if !ok {
				d["pit_prediction"] = nil
				continue
			}
			current = g
		}
		projected := current + selected
		others := make([]float64, 0, len(gaps)-1)
		for _, p := range gaps {
			if p.abbr != abbr {
				others = append(others, p.gap)
			}
		}
		pred := 1
		for _, g := range others {
			if projected > g {
				pred++
			} else {
				break
			}
		}
		if pred > len(others)+1 {
			pred = len(others) + 1
		}
		currentPos := asInt(d["position"])
		if pred > currentPos {
			d["pit_prediction"] = pred
			behindIdx := pred - 1
			if behindIdx < len(others) {
				margin := others[behindIdx] - projected
				if margin < 0 {
					margin = 0
				}
				d["pit_prediction_margin"] = roundTo(margin, 3)
			} else {
				d["pit_prediction_margin"] = nil
			}
			aheadIdx := pred - 2
			if aheadIdx >= 0 && aheadIdx < len(others) {
				fa := projected - others[aheadIdx]
				if fa < 0 {
					fa = 0
				}
				d["pit_prediction_free_air"] = roundTo(fa, 1)
			} else {
				d["pit_prediction_free_air"] = nil
			}
		} else {
			d["pit_prediction"] = nil
			d["pit_prediction_margin"] = nil
			d["pit_prediction_free_air"] = nil
		}
	}
}
