package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"math"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (p *GoSessionProcessor) ProcessSession(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error {
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	status := func(msg string) {
		if onStatus != nil {
			onStatus(msg)
		}
		log.Printf("processor(go): %s", msg)
	}
	sessionType = strings.ToUpper(strings.TrimSpace(sessionType))
	isQualifying := sessionType == "Q" || sessionType == "SQ"
	status("Fetching season index...")
	idx, err := p.fetchSeasonIndex(ctx, year)
	if err != nil {
		return err
	}
	meeting, sess, err := p.findMeetingSession(idx, round, sessionType)
	if err != nil {
		return err
	}
	sessionPath := strings.Trim(sess.Path, "/")
	baseOut := filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType)

	status("Fetching session feed manifest...")
	sidx, err := p.fetchSessionIndex(ctx, sessionPath)
	if err != nil {
		return err
	}

	feedRequire := []string{
		"SessionInfo", "DriverList", "TimingData", "TimingAppData",
		"Position.z", "CarData.z", "TrackStatus", "WeatherData",
		"RaceControlMessages", "SessionStatus",
	}
	for _, name := range feedRequire {
		if _, ok := sidx.Feeds[name]; !ok {
			return fmt.Errorf("missing required feed %q for %s", name, sessionPath)
		}
	}

	status("Downloading keyframes...")
	sessionInfoRaw, err := p.fetchFeedKeyframeJSON(ctx, sessionPath, sidx.Feeds["SessionInfo"])
	if err != nil {
		return err
	}
	driverListRaw, err := p.fetchFeedKeyframeJSON(ctx, sessionPath, sidx.Feeds["DriverList"])
	if err != nil {
		return err
	}
	if _, err := p.fetchFeedKeyframeJSON(ctx, sessionPath, sidx.Feeds["TimingData"]); err != nil {
		return err
	}
	timingAppRaw, err := p.fetchFeedKeyframeJSON(ctx, sessionPath, sidx.Feeds["TimingAppData"])
	if err != nil {
		return err
	}
	raceControlRaw, _ := p.fetchFeedKeyframeJSON(ctx, sessionPath, sidx.Feeds["RaceControlMessages"])
	circuitMeta, circuitName, err := p.resolveCircuitMetadata(sessionInfoRaw)
	if err != nil {
		return err
	}

	drivers, driverByNum := parseDriverList(driverListRaw)
	if len(drivers) == 0 {
		return errors.New("driver list is empty")
	}
	stintByAbbr := extractTimingAppStints(timingAppRaw, driverByNum)
	info := buildInfoJSON(year, round, sessionType, meeting, sessionInfoRaw, drivers)
	if err := p.writeJSONAtomic(filepath.Join(baseOut, "info.json"), info); err != nil {
		return err
	}
	sessionID, err := p.store.UpsertSessionInfo(ctx, year, round, sessionType, info)
	if err != nil {
		return fmt.Errorf("upsert session info: %w", err)
	}

	var phaseTimeline []qualifyingPhasePoint
	if isQualifying {
		if sessionDataFeed, ok := sidx.Feeds["SessionData"]; ok {
			status("Parsing qualifying phase stream...")
			phaseTimeline, _ = p.parseSessionDataStream(ctx, sessionPath, sessionDataFeed)
		}
	}

	status("Parsing timing stream...")
	timingTimeline, laps, latestState, err := p.parseTimingDataStream(ctx, sessionPath, sidx.Feeds["TimingData"], driverByNum, timingAppRaw, phaseTimeline)
	if err != nil {
		return err
	}
	results := buildResultsJSON(latestState, driverByNum)
	if err := p.writeJSONAtomic(filepath.Join(baseOut, "results.json"), results); err != nil {
		return err
	}
	if err := p.writeJSONAtomic(filepath.Join(baseOut, "laps.json"), laps); err != nil {
		return err
	}
	if err := p.store.ReplaceResults(ctx, sessionID, results); err != nil {
		return fmt.Errorf("store results: %w", err)
	}
	if err := p.store.ReplaceLaps(ctx, sessionID, laps); err != nil {
		return fmt.Errorf("store laps: %w", err)
	}

	status("Parsing position stream...")
	posTimeline, err := p.parsePositionStream(ctx, sessionPath, sidx.Feeds["Position.z"])
	if err != nil {
		return err
	}
	if len(posTimeline) == 0 {
		return errors.New("position stream is empty")
	}
	trackJSON := buildTrackJSON(posTimeline, timingTimeline, circuitMeta, circuitName)
	if err := p.writeJSONAtomic(filepath.Join(baseOut, "track.json"), trackJSON); err != nil {
		return err
	}
	if err := p.store.ReplaceTrack(ctx, sessionID, trackJSON); err != nil {
		return fmt.Errorf("store track: %w", err)
	}
	if isQualifying {
		q3Lines := buildQ3LinesJSON(drivers, driverByNum, laps, posTimeline, timingTimeline, trackJSON)
		if len(q3Lines) > 0 {
			if err := p.writeJSONAtomic(filepath.Join(baseOut, "q3_lines.json"), q3Lines); err != nil {
				return err
			}
		}
	}

	status("Parsing telemetry stream...")
	carTimeline, err := p.parseCarDataStream(ctx, sessionPath, sidx.Feeds["CarData.z"])
	if err != nil {
		return err
	}

	status("Parsing track/weather streams...")
	trackStatuses, _ := p.parseTrackStatusStream(ctx, sessionPath, sidx.Feeds["TrackStatus"])
	weatherPoints, _ := p.parseWeatherStream(ctx, sessionPath, sidx.Feeds["WeatherData"])
	sessionStatuses, _ := p.parseSessionStatusStream(ctx, sessionPath, sidx.Feeds["SessionStatus"])
	window := deriveReplayWindow(sessionStatuses, sessionType)
	rcPoints := parseRaceControlMessages(raceControlRaw)

	status("Writing replay frames (streaming)...")
	replayIdx, err := p.writeReplayFromTimelines(ctx, sessionID, drivers, timingTimeline, posTimeline, carTimeline, trackStatuses, weatherPoints, rcPoints, sessionType, window, stintByAbbr)
	if err != nil {
		return err
	}
	if replayIdx != nil {
		_ = p.writeJSONAtomic(filepath.Join(baseOut, "replay.index.json"), replayIdx)
	}

	status("Writing telemetry by driver...")
	if err := p.writeTelemetryFiles(ctx, sessionID, drivers, carTimeline, posTimeline, timingTimeline); err != nil {
		return err
	}
	if err := p.store.SetSessionReady(ctx, sessionID, true); err != nil {
		return err
	}

	status("Processing complete")
	return nil
}

func parseRaceControlMessages(raw map[string]any) []raceControlPoint {
	msgs, ok := raw["Messages"].([]any)
	if !ok {
		return nil
	}
	out := make([]raceControlPoint, 0, len(msgs))
	for _, m := range msgs {
		obj, ok := m.(map[string]any)
		if !ok {
			continue
		}
		ts := 0.0
		if utc := asString(obj["Utc"]); utc != "" {
			if t, err := time.Parse(time.RFC3339Nano, utc); err == nil {
				ts = float64(t.Unix())
			}
		}
		entry := map[string]any{
			"message":   asString(obj["Message"]),
			"category":  asString(obj["Category"]),
			"timestamp": ts,
		}
		if v := asString(obj["RacingNumber"]); v != "" {
			entry["racing_number"] = v
		}
		if v := asString(obj["Flag"]); v != "" {
			entry["flag"] = v
		}
		if v := asString(obj["Scope"]); v != "" {
			entry["scope"] = v
		}
		if v := asInt(obj["Sector"]); v > 0 {
			entry["sector"] = v
		}
		if v := asInt(obj["Lap"]); v > 0 {
			entry["lap"] = v
		}
		out = append(out, raceControlPoint{T: ts, Message: entry})
	}
	return out
}

func buildInfoJSON(year, round int, sessionType string, meeting *seasonMeeting, sessionInfo map[string]any, drivers []driverMeta) map[string]any {
	eventName := meeting.Name
	circuit := defaultString(meeting.Location, meeting.Circuit.ShortName)
	country := meeting.Country.Name
	if mt, ok := sessionInfo["Meeting"].(map[string]any); ok {
		if v := asString(mt["Name"]); v != "" {
			eventName = v
		}
		if v := asString(mt["Location"]); v != "" {
			circuit = v
		}
		if c, ok := mt["Country"].(map[string]any); ok {
			if v := asString(c["Name"]); v != "" {
				country = v
			}
		}
	}
	drv := make([]map[string]any, 0, len(drivers))
	for _, d := range drivers {
		drv = append(drv, map[string]any{
			"abbreviation":  d.Abbr,
			"driver_number": d.Number,
			"full_name":     d.Name,
			"team_name":     d.Team,
			"team_color":    d.Color,
		})
	}
	return map[string]any{
		"year":         year,
		"round_number": round,
		"event_name":   eventName,
		"circuit":      circuit,
		"country":      country,
		"session_type": sessionType,
		"drivers":      drv,
	}
}

func parseDriverList(raw map[string]any) ([]driverMeta, map[string]driverMeta) {
	out := make([]driverMeta, 0, 24)
	byNum := make(map[string]driverMeta, 24)
	for num, item := range raw {
		obj, ok := item.(map[string]any)
		if !ok {
			continue
		}
		color := strings.TrimSpace(asString(obj["TeamColour"]))
		if color == "" {
			color = "FFFFFF"
		}
		if !strings.HasPrefix(color, "#") {
			color = "#" + color
		}
		d := driverMeta{
			Number: num,
			Abbr:   strings.ToUpper(defaultString(asString(obj["Tla"]), num)),
			Name:   defaultString(asString(obj["FullName"]), asString(obj["BroadcastName"])),
			Team:   asString(obj["TeamName"]),
			Color:  color,
		}
		out = append(out, d)
		byNum[num] = d
	}
	sort.Slice(out, func(i, j int) bool {
		ni, _ := strconv.Atoi(out[i].Number)
		nj, _ := strconv.Atoi(out[j].Number)
		return ni < nj
	})
	return out, byNum
}

func buildResultsJSON(latest map[string]timingState, byNum map[string]driverMeta) []map[string]any {
	out := make([]map[string]any, 0, len(latest))
	for num, st := range latest {
		meta := byNum[num]
		status := "Finished"
		if st.Retired {
			status = "Retired"
		}
		out = append(out, map[string]any{
			"position":      zeroToNil(st.Position),
			"driver":        meta.Name,
			"abbreviation":  meta.Abbr,
			"team":          meta.Team,
			"team_color":    meta.Color,
			"grid_position": nil,
			"status":        status,
			"points":        0,
			"fastest_lap":   nil,
			"gap_to_leader": nil,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		pi := asInt(out[i]["position"])
		pj := asInt(out[j]["position"])
		if pi == 0 {
			pi = 999
		}
		if pj == 0 {
			pj = 999
		}
		return pi < pj
	})
	return out
}

func buildTrackJSON(pos map[string][]posSample, timing map[string][]timingState, meta circuitMetadataEntry, circuitName string) map[string]any {
	points := pickRepresentativeTrackLap(pos, timing)
	if len(points) == 0 {
		return map[string]any{
			"track_points": []any{},
			"rotation":     meta.Rotation,
			"circuit_name": circuitName,
			"norm": map[string]any{
				"x_min": 0.0, "y_min": 0.0, "scale": 1.0,
			},
			"sector_boundaries": nil,
			"corners":           []any{},
			"marshal_sectors":   []any{},
		}
	}
	xMin, yMin, scale := boundsForTrack(points)
	trackPts := make([]map[string]any, 0, len(points))
	step := 1
	if len(points) > 2000 {
		step = len(points) / 2000
	}
	for i := 0; i < len(points); i += step {
		pt := points[i]
		trackPts = append(trackPts, map[string]any{
			"x": normalizeCoord(pt.X, xMin, scale),
			"y": normalizeCoord(pt.Y, yMin, scale),
		})
	}
	corners := make([]map[string]any, 0, len(meta.Corners))
	for _, c := range meta.Corners {
		corners = append(corners, map[string]any{
			"x":      normalizeCoord(c.X, xMin, scale),
			"y":      normalizeCoord(c.Y, yMin, scale),
			"number": c.Number,
			"letter": c.Letter,
			"angle":  c.Angle,
		})
	}
	marshalSectors := make([]map[string]any, 0, len(meta.MarshalSectors))
	for _, s := range meta.MarshalSectors {
		marshalSectors = append(marshalSectors, map[string]any{
			"x":      normalizeCoord(s.X, xMin, scale),
			"y":      normalizeCoord(s.Y, yMin, scale),
			"number": s.Number,
		})
	}
	return map[string]any{
		"track_points": trackPts,
		"rotation":     meta.Rotation,
		"circuit_name": circuitName,
		"norm": map[string]any{
			"x_min": xMin,
			"y_min": yMin,
			"scale": scale,
		},
		"sector_boundaries": nil,
		"corners":           corners,
		"marshal_sectors":   marshalSectors,
	}
}

func buildQ3LinesJSON(drivers []driverMeta, byNum map[string]driverMeta, laps []map[string]any, pos map[string][]posSample, timing map[string][]timingState, track map[string]any) map[string]any {
	type bestLap struct {
		Lap          int
		TimeStr      string
		Seconds      float64
		EndTs        float64
		Sector1      string
		Sector2      string
		Sector3      string
		Sector1Color string
		Sector2Color string
		Sector3Color string
	}
	type q3OverallSectorLeader struct {
		Abbr       string
		Sector     float64
		LapSeconds float64
	}
	const phaseName = "Q3"
	const sectorTol = 0.001
	abbrToMeta := make(map[string]driverMeta, len(drivers))
	abbrToNumber := make(map[string]string, len(drivers))
	for _, d := range drivers {
		abbrToMeta[d.Abbr] = d
		abbrToNumber[d.Abbr] = d.Number
	}

	bestByDriver := make(map[string]bestLap, len(laps))
	overallSectorLeaders := [3]q3OverallSectorLeader{
		{Sector: math.MaxFloat64, LapSeconds: math.MaxFloat64},
		{Sector: math.MaxFloat64, LapSeconds: math.MaxFloat64},
		{Sector: math.MaxFloat64, LapSeconds: math.MaxFloat64},
	}
	driverSectorBest := make(map[string][3]float64, len(drivers))
	driverSectorCount := make(map[string][3]int, len(drivers))
	for _, lap := range laps {
		if strings.ToUpper(strings.TrimSpace(asString(lap["qualifying_phase"]))) != phaseName {
			continue
		}
		abbr := strings.ToUpper(strings.TrimSpace(asString(lap["driver"])))
		if abbr == "" {
			continue
		}
		lapNum := asInt(lap["lap_number"])
		if lapNum <= 0 {
			continue
		}
		lapTime := strings.TrimSpace(asString(lap["lap_time"]))
		secs, ok := parseLapTimeToSeconds(lapTime)
		if !ok {
			continue
		}
		lapEndTs := asFloat(lap["time"], 0)

		s1Raw := strings.TrimSpace(asString(lap["sector1"]))
		s2Raw := strings.TrimSpace(asString(lap["sector2"]))
		s3Raw := strings.TrimSpace(asString(lap["sector3"]))
		sectorRaw := [3]string{s1Raw, s2Raw, s3Raw}
		s1Color := normalizeOfficialSectorColor(asString(lap["sector1_color"]))
		s2Color := normalizeOfficialSectorColor(asString(lap["sector2_color"]))
		s3Color := normalizeOfficialSectorColor(asString(lap["sector3_color"]))
		if _, exists := driverSectorBest[abbr]; !exists {
			driverSectorBest[abbr] = [3]float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}
		}
		bestBySector := driverSectorBest[abbr]
		sectorCount := driverSectorCount[abbr]
		for i := 0; i < len(sectorRaw); i++ {
			sectorSeconds, ok := parseLapTimeToSeconds(sectorRaw[i])
			if !ok {
				continue
			}
			sectorCount[i]++
			if sectorSeconds < bestBySector[i]-sectorTol {
				bestBySector[i] = sectorSeconds
			}
			leader := overallSectorLeaders[i]
			betterSector := sectorSeconds < leader.Sector-sectorTol
			tiedSector := math.Abs(sectorSeconds-leader.Sector) <= sectorTol
			tieBreakBetter := tiedSector && (secs < leader.LapSeconds-sectorTol || (math.Abs(secs-leader.LapSeconds) <= sectorTol && (leader.Abbr == "" || abbr < leader.Abbr)))
			if betterSector || tieBreakBetter {
				overallSectorLeaders[i] = q3OverallSectorLeader{
					Abbr:       abbr,
					Sector:     sectorSeconds,
					LapSeconds: secs,
				}
			}
		}
		driverSectorBest[abbr] = bestBySector
		driverSectorCount[abbr] = sectorCount

		prev, exists := bestByDriver[abbr]
		if !exists || secs < prev.Seconds {
			bestByDriver[abbr] = bestLap{
				Lap:          lapNum,
				TimeStr:      lapTime,
				Seconds:      secs,
				EndTs:        lapEndTs,
				Sector1:      s1Raw,
				Sector2:      s2Raw,
				Sector3:      s3Raw,
				Sector1Color: s1Color,
				Sector2Color: s2Color,
				Sector3Color: s3Color,
			}
		}
	}
	if len(bestByDriver) == 0 {
		return nil
	}

	norm, _ := track["norm"].(map[string]any)
	xMin := asFloat(norm["x_min"], 0)
	yMin := asFloat(norm["y_min"], 0)
	scale := asFloat(norm["scale"], 1)
	if scale <= 0 {
		scale = 1
	}
	anchorX, anchorY, hasAnchor := extractTrackStartAnchorRaw(track, xMin, yMin, scale)

	entries := make([]map[string]any, 0, len(bestByDriver))
	for abbr, best := range bestByDriver {
		number := abbrToNumber[abbr]
		if number == "" {
			continue
		}
		driverPos := pos[number]
		driverTiming := timing[number]
		if len(driverPos) == 0 || len(driverTiming) == 0 {
			continue
		}

		lapPoints := make([]posSample, 0, 1024)
		if best.EndTs > 0 && best.Seconds > 0 {
			lapStartTs := best.EndTs - best.Seconds
			const boundaryPad = 0.35
			const boundarySnap = 1.5

			if ps := nearestPosSampleWithin(driverPos, lapStartTs, boundarySnap); ps != nil {
				lapPoints = append(lapPoints, *ps)
			}
			for _, p := range driverPos {
				if p.T < lapStartTs-boundaryPad || p.T > best.EndTs+boundaryPad {
					continue
				}
				lapPoints = append(lapPoints, p)
			}
			if pe := nearestPosSampleWithin(driverPos, best.EndTs, boundarySnap); pe != nil {
				lapPoints = append(lapPoints, *pe)
			}
		}
		// Fallback for older artifacts where lap completion timestamp may be unavailable.
		if len(lapPoints) < 10 {
			lapPoints = lapPoints[:0]
			for _, p := range driverPos {
				if lapAt(driverTiming, p.T) != best.Lap {
					continue
				}
				lapPoints = append(lapPoints, p)
			}
		}
		sort.Slice(lapPoints, func(i, j int) bool { return lapPoints[i].T < lapPoints[j].T })
		if len(lapPoints) < 10 {
			continue
		}
		lapPoints = sanitizeTrackLap(lapPoints)
		if len(lapPoints) < 10 {
			continue
		}
		if hasAnchor {
			lapPoints = rotateLapPointsToAnchor(lapPoints, anchorX, anchorY)
		}

		stepDist := make([]float64, len(lapPoints))
		totalDist := 0.0
		for i := 1; i < len(lapPoints); i++ {
			totalDist += pointDistance(lapPoints[i-1], lapPoints[i])
			stepDist[i] = totalDist
		}
		if totalDist <= 0 {
			continue
		}

		samples := make([]map[string]any, 0, len(lapPoints))
		for i := 0; i < len(lapPoints); i++ {
			lp := lapPoints[i]
			progress := stepDist[i] / totalDist
			if progress < 0 {
				progress = 0
			}
			if progress > 1 {
				progress = 1
			}
			elapsed := progress * best.Seconds
			samples = append(samples, map[string]any{
				"x": normalizeCoord(lp.X, xMin, scale),
				"y": normalizeCoord(lp.Y, yMin, scale),
				"t": round3(elapsed),
				"p": round6(progress),
			})
		}
		normalizeQ3SampleTimeline(samples, best.Seconds)
		samples = decimateLineSamples(samples, 1200)

		meta, ok := abbrToMeta[abbr]
		if !ok {
			meta = byNum[number]
		}
		sectorColors := map[string]any{"s1": nil, "s2": nil, "s3": nil}
		sectorRaw := [3]string{best.Sector1, best.Sector2, best.Sector3}
		officialColors := [3]string{best.Sector1Color, best.Sector2Color, best.Sector3Color}
		driverBestBySector := driverSectorBest[abbr]
		driverCountBySector := driverSectorCount[abbr]
		for i, key := range []string{"s1", "s2", "s3"} {
			color := normalizeOfficialSectorColor(officialColors[i])
			if color == "" {
				sectorSeconds, ok := parseLapTimeToSeconds(sectorRaw[i])
				if ok {
					leader := overallSectorLeaders[i]
					if leader.Abbr == abbr && leader.Sector < math.MaxFloat64 && math.Abs(sectorSeconds-leader.Sector) <= sectorTol {
						color = "purple"
					} else if driverCountBySector[i] <= 1 {
						color = "green"
					} else if driverBestBySector[i] < math.MaxFloat64 && math.Abs(sectorSeconds-driverBestBySector[i]) <= sectorTol {
						color = "green"
					} else {
						color = "yellow"
					}
				}
			}
			sectorColors[key] = nilIfEmptyString(color)
		}

		entries = append(entries, map[string]any{
			"abbr":             abbr,
			"driver_number":    number,
			"team":             meta.Team,
			"color":            meta.Color,
			"lap_number":       best.Lap,
			"lap_time":         best.TimeStr,
			"lap_time_seconds": round3(best.Seconds),
			"sector1":          nilIfEmptyString(best.Sector1),
			"sector2":          nilIfEmptyString(best.Sector2),
			"sector3":          nilIfEmptyString(best.Sector3),
			"sector_colors":    sectorColors,
			"phase":            phaseName,
			"samples":          samples,
		})
	}
	if len(entries) == 0 {
		return nil
	}

	sort.Slice(entries, func(i, j int) bool {
		ti := asFloat(entries[i]["lap_time_seconds"], math.MaxFloat64)
		tj := asFloat(entries[j]["lap_time_seconds"], math.MaxFloat64)
		if ti == tj {
			return asString(entries[i]["abbr"]) < asString(entries[j]["abbr"])
		}
		return ti < tj
	})

	defaultPair := make([]any, 0, 2)
	if len(entries) > 0 {
		defaultPair = append(defaultPair, asString(entries[0]["abbr"]))
	}
	if len(entries) > 1 {
		defaultPair = append(defaultPair, asString(entries[1]["abbr"]))
	}

	return map[string]any{
		"phase":        phaseName,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
		"drivers":      entries,
		"default_pair": defaultPair,
	}
}

func normalizeOfficialSectorColor(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "purple":
		return "purple"
	case "green":
		return "green"
	case "yellow":
		return "yellow"
	default:
		return ""
	}
}

func normalizeQ3SampleTimeline(samples []map[string]any, lapSeconds float64) {
	if len(samples) == 0 || lapSeconds <= 0 {
		return
	}
	firstT := asFloat(samples[0]["t"], 0)
	lastT := asFloat(samples[len(samples)-1]["t"], firstT)
	span := lastT - firstT
	if span <= 1e-6 {
		for i := range samples {
			progress := asFloat(samples[i]["p"], 0)
			if progress < 0 {
				progress = 0
			} else if progress > 1 {
				progress = 1
			}
			samples[i]["t"] = round3(progress * lapSeconds)
		}
	} else {
		for i := range samples {
			t := asFloat(samples[i]["t"], firstT)
			ratio := (t - firstT) / span
			if ratio < 0 {
				ratio = 0
			} else if ratio > 1 {
				ratio = 1
			}
			samples[i]["t"] = round3(ratio * lapSeconds)
		}
	}
	samples[0]["t"] = 0.0
	samples[len(samples)-1]["t"] = round3(lapSeconds)
}

func parseLapTimeToSeconds(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	parts := strings.Split(raw, ":")
	switch len(parts) {
	case 1:
		v, err := strconv.ParseFloat(parts[0], 64)
		if err != nil || v <= 0 {
			return 0, false
		}
		return v, true
	case 2:
		mins, err1 := strconv.Atoi(parts[0])
		secs, err2 := strconv.ParseFloat(parts[1], 64)
		if err1 != nil || err2 != nil || mins < 0 || secs < 0 {
			return 0, false
		}
		return float64(mins*60) + secs, true
	default:
		return 0, false
	}
}

func decimateLineSamples(samples []map[string]any, maxPoints int) []map[string]any {
	if len(samples) <= maxPoints || maxPoints <= 2 {
		return samples
	}
	step := int(math.Ceil(float64(len(samples)-1) / float64(maxPoints-1)))
	if step < 1 {
		step = 1
	}
	out := make([]map[string]any, 0, maxPoints)
	lastIdx := -1
	for i := 0; i < len(samples); i += step {
		out = append(out, samples[i])
		lastIdx = i
	}
	if lastIdx != len(samples)-1 {
		out = append(out, samples[len(samples)-1])
	}
	return out
}

func pickRepresentativeTrackLap(pos map[string][]posSample, timing map[string][]timingState) []posSample {
	best := []posSample{}
	bestScoreLen := -1
	bestClosureRatio := math.MaxFloat64
	fallback := []posSample{}
	fallbackCount := 0

	drivers := make([]string, 0, len(pos))
	for d := range pos {
		drivers = append(drivers, d)
	}
	sort.Strings(drivers)

	for _, driver := range drivers {
		arr := pos[driver]
		if len(arr) == 0 {
			continue
		}
		tl := timing[driver]
		if len(tl) == 0 {
			continue
		}

		byLap := map[int][]posSample{}
		maxLap := 0
		for _, p := range arr {
			lap := lapAt(tl, p.T)
			if lap <= 0 {
				continue
			}
			if lap > maxLap {
				maxLap = lap
			}
			byLap[lap] = append(byLap[lap], p)
		}
		if maxLap <= 1 {
			continue
		}

		for lap, pts := range byLap {
			// Skip outliers:
			// - lap 1 (formation/start),
			// - final lap (cool-down / finish behavior),
			// - too sparse samples.
			if lap <= 1 || lap >= maxLap || len(pts) < 120 {
				continue
			}
			pts = sanitizeTrackLap(pts)
			if len(pts) < 120 {
				continue
			}
			if len(pts) > fallbackCount {
				fallbackCount = len(pts)
				fallback = pts
			}

			closureRatio := trackClosureRatio(pts)
			if closureRatio > 0.10 {
				continue
			}
			if len(pts) > bestScoreLen || (len(pts) == bestScoreLen && closureRatio < bestClosureRatio) {
				bestScoreLen = len(pts)
				bestClosureRatio = closureRatio
				best = pts
			}
		}
	}

	if len(best) > 0 {
		return best
	}
	if len(fallback) > 0 {
		return fallback
	}

	// Fallback: previous behavior (driver with the most samples).
	driver := ""
	maxCount := 0
	for d, arr := range pos {
		if len(arr) > maxCount {
			maxCount = len(arr)
			driver = d
		}
	}
	if driver == "" {
		return nil
	}
	return sanitizeTrackLap(pos[driver])
}

func sanitizeTrackLap(points []posSample) []posSample {
	if len(points) < 3 {
		return points
	}

	steps := make([]float64, 0, len(points)-1)
	for i := 1; i < len(points); i++ {
		steps = append(steps, pointDistance(points[i-1], points[i]))
	}
	median := medianFloat64(steps)
	if median <= 0 {
		return points
	}
	threshold := median * 12.0

	out := make([]posSample, 0, len(points))
	out = append(out, points[0])
	for i := 1; i < len(points)-1; i++ {
		dPrev := pointDistance(points[i-1], points[i])
		dNext := pointDistance(points[i], points[i+1])
		// Remove obvious GPS spikes that create "teleport" lines.
		if dPrev > threshold && dNext > threshold {
			continue
		}
		out = append(out, points[i])
	}
	out = append(out, points[len(points)-1])
	if len(out) < 3 {
		return points
	}
	return out
}

func extractTrackStartAnchorRaw(track map[string]any, xMin, yMin, scale float64) (float64, float64, bool) {
	if scale <= 0 {
		return 0, 0, false
	}
	var first map[string]any
	switch trackPoints := track["track_points"].(type) {
	case []any:
		if len(trackPoints) == 0 {
			return 0, 0, false
		}
		m, ok := trackPoints[0].(map[string]any)
		if !ok {
			return 0, 0, false
		}
		first = m
	case []map[string]any:
		if len(trackPoints) == 0 {
			return 0, 0, false
		}
		first = trackPoints[0]
	default:
		return 0, 0, false
	}
	xNorm := asFloat(first["x"], math.NaN())
	yNorm := asFloat(first["y"], math.NaN())
	if math.IsNaN(xNorm) || math.IsNaN(yNorm) {
		return 0, 0, false
	}
	return xMin + xNorm*scale, yMin + yNorm*scale, true
}

func rotateLapPointsToAnchor(points []posSample, anchorX, anchorY float64) []posSample {
	if len(points) < 2 {
		return points
	}
	bestIdx := 0
	bestDist := math.MaxFloat64
	for i, pt := range points {
		d := math.Hypot(pt.X-anchorX, pt.Y-anchorY)
		if d < bestDist {
			bestDist = d
			bestIdx = i
		}
	}
	if bestIdx == 0 {
		return points
	}
	out := make([]posSample, 0, len(points))
	out = append(out, points[bestIdx:]...)
	out = append(out, points[:bestIdx]...)
	return out
}

func trackClosureRatio(points []posSample) float64 {
	if len(points) < 2 {
		return math.MaxFloat64
	}
	total := 0.0
	for i := 1; i < len(points); i++ {
		total += pointDistance(points[i-1], points[i])
	}
	if total <= 0 {
		return math.MaxFloat64
	}
	return pointDistance(points[0], points[len(points)-1]) / total
}

func pointDistance(a, b posSample) float64 {
	dx := a.X - b.X
	dy := a.Y - b.Y
	return math.Hypot(dx, dy)
}

func medianFloat64(values []float64) float64 {
	if len(values) == 0 {
		return 0
	}
	cp := append([]float64(nil), values...)
	sort.Float64s(cp)
	mid := len(cp) / 2
	if len(cp)%2 == 1 {
		return cp[mid]
	}
	return (cp[mid-1] + cp[mid]) / 2
}

func boundsForTrack(points []posSample) (xMin, yMin, scale float64) {
	if len(points) == 0 {
		return 0, 0, 1
	}
	xMin, xMax := points[0].X, points[0].X
	yMin, yMax := points[0].Y, points[0].Y
	for _, p := range points {
		if p.X < xMin {
			xMin = p.X
		}
		if p.X > xMax {
			xMax = p.X
		}
		if p.Y < yMin {
			yMin = p.Y
		}
		if p.Y > yMax {
			yMax = p.Y
		}
	}
	scale = math.Max(xMax-xMin, yMax-yMin)
	if scale <= 0 {
		scale = 1
	}
	return xMin, yMin, scale
}

func normalizeCoord(v, min, scale float64) float64 {
	return (v - min) / scale
}

func round6(v float64) float64 {
	return math.Round(v*1_000_000) / 1_000_000
}
