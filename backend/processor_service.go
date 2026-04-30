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
	err := p.processSessionViaLiveTiming(ctx, year, round, sessionType, onStatus)
	if err == nil {
		return nil
	}
	if !isLiveTimingBlockedError(err) {
		return err
	}
	log.Printf("processor(go): livetiming provider blocked, falling back to openf1 year=%d round=%d type=%s err=%v", year, round, strings.ToUpper(strings.TrimSpace(sessionType)), err)
	return p.processSessionViaOpenF1(ctx, year, round, sessionType, onStatus)
}

func (p *GoSessionProcessor) processSessionViaLiveTiming(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error {
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
	idx, seasonErr := p.fetchSeasonIndex(ctx, year)
	var meeting *seasonMeeting
	sessionPath := ""
	if seasonErr == nil {
		var sess *meetingSession
		var findErr error
		meeting, sess, findErr = p.findMeetingSession(idx, round, sessionType)
		if findErr == nil {
			sessionPath = strings.Trim(sess.Path, "/")
		}
	}
	if sessionPath == "" {
		status("Season index unavailable, trying schedule-derived session path fallback...")
		var fallbackErr error
		meeting, sessionPath, fallbackErr = p.resolveMeetingSessionPathFromSchedule(ctx, year, round, sessionType)
		if fallbackErr != nil && seasonErr != nil {
			return fmt.Errorf("fetch season index: %w; fallback failed: %v", seasonErr, fallbackErr)
		}
		if fallbackErr != nil {
			return fallbackErr
		}
	}
	if meeting == nil {
		return errors.New("meeting context is unavailable")
	}
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
	if err := p.writeTelemetryFiles(ctx, sessionID, drivers, carTimeline, posTimeline, timingTimeline, laps); err != nil {
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

func isLiveTimingBlockedError(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	if !strings.Contains(s, "livetiming.formula1.com") {
		return false
	}
	if strings.Contains(s, "-> 403") || strings.Contains(s, "request blocked") || strings.Contains(s, "cloudfront") {
		return true
	}
	return false
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

const (
	q3ComparePhase        = "Q3"
	q3SectorTolerance     = 0.001
	q3MinLinePointCount   = 10
	q3MaxLineSampleCount  = 1200
	q3BoundaryMaxGap      = 1.5
	q3BoundaryPad         = 0.35
	q3BoundaryNearestSnap = 1.5
)

type q3BestLap struct {
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

type q3SectorStats struct {
	overallLeaders [3]q3OverallSectorLeader
	driverBest     map[string][3]float64
	driverCount    map[string][3]int
}

func newQ3SectorStats(capHint int) q3SectorStats {
	stats := q3SectorStats{
		driverBest:  make(map[string][3]float64, capHint),
		driverCount: make(map[string][3]int, capHint),
	}
	for i := 0; i < 3; i++ {
		stats.overallLeaders[i] = q3OverallSectorLeader{
			Sector:     math.MaxFloat64,
			LapSeconds: math.MaxFloat64,
		}
	}
	return stats
}

func buildQ3LinesJSON(drivers []driverMeta, byNum map[string]driverMeta, laps []map[string]any, pos map[string][]posSample, timing map[string][]timingState, track map[string]any) map[string]any {
	abbrToMeta := make(map[string]driverMeta, len(drivers))
	abbrToNumber := make(map[string]string, len(drivers))
	for _, d := range drivers {
		abbrToMeta[d.Abbr] = d
		abbrToNumber[d.Abbr] = d.Number
	}

	bestByDriver, sectorStats := collectQ3BestLapsAndSectorStats(laps, len(drivers))
	if len(bestByDriver) == 0 {
		return nil
	}

	xMin, yMin, scale := q3TrackNorm(track)
	entries := make([]map[string]any, 0, len(bestByDriver))

	for abbr, best := range bestByDriver {
		number := abbrToNumber[abbr]
		if number == "" {
			continue
		}
		lapPoints := extractQ3LapPoints(pos[number], timing[number], best)
		if len(lapPoints) < q3MinLinePointCount {
			continue
		}

		samples := buildQ3LineSamples(lapPoints, best.Seconds, xMin, yMin, scale)
		if len(samples) < q3MinLinePointCount {
			continue
		}

		meta, ok := abbrToMeta[abbr]
		if !ok {
			meta = byNum[number]
		}
		lapStartTS := any(nil)
		lapEndTS := any(nil)
		if best.EndTs > 0 && best.Seconds > 0 {
			start := best.EndTs - best.Seconds
			if start >= 0 {
				lapStartTS = round3(start)
				lapEndTS = round3(best.EndTs)
			}
		}
		entries = append(entries, map[string]any{
			"abbr":             abbr,
			"driver_number":    number,
			"team":             meta.Team,
			"color":            meta.Color,
			"lap_number":       best.Lap,
			"lap_start_ts":     lapStartTS,
			"lap_end_ts":       lapEndTS,
			"lap_time":         best.TimeStr,
			"lap_time_seconds": round3(best.Seconds),
			"sector1":          nilIfEmptyString(best.Sector1),
			"sector2":          nilIfEmptyString(best.Sector2),
			"sector3":          nilIfEmptyString(best.Sector3),
			"sector_colors":    classifyQ3SectorColors(abbr, best, sectorStats),
			"phase":            q3ComparePhase,
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
	defaultPair = append(defaultPair, asString(entries[0]["abbr"]))
	if len(entries) > 1 {
		defaultPair = append(defaultPair, asString(entries[1]["abbr"]))
	}

	return map[string]any{
		"phase":        q3ComparePhase,
		"generated_at": time.Now().UTC().Format(time.RFC3339),
		"drivers":      entries,
		"default_pair": defaultPair,
	}
}

func collectQ3BestLapsAndSectorStats(laps []map[string]any, capHint int) (map[string]q3BestLap, q3SectorStats) {
	stats := newQ3SectorStats(capHint)
	bestByDriver := make(map[string]q3BestLap, len(laps))

	for _, lap := range laps {
		if strings.ToUpper(strings.TrimSpace(asString(lap["qualifying_phase"]))) != q3ComparePhase {
			continue
		}
		abbr := strings.ToUpper(strings.TrimSpace(asString(lap["driver"])))
		if abbr == "" {
			continue
		}

		lapNumber := asInt(lap["lap_number"])
		if lapNumber <= 0 {
			continue
		}

		lapTime := strings.TrimSpace(asString(lap["lap_time"]))
		lapSeconds, ok := parseLapTimeToSeconds(lapTime)
		if !ok {
			continue
		}

		bestLap := q3BestLap{
			Lap:          lapNumber,
			TimeStr:      lapTime,
			Seconds:      lapSeconds,
			EndTs:        asFloat(lap["time"], 0),
			Sector1:      strings.TrimSpace(asString(lap["sector1"])),
			Sector2:      strings.TrimSpace(asString(lap["sector2"])),
			Sector3:      strings.TrimSpace(asString(lap["sector3"])),
			Sector1Color: normalizeOfficialSectorColor(asString(lap["sector1_color"])),
			Sector2Color: normalizeOfficialSectorColor(asString(lap["sector2_color"])),
			Sector3Color: normalizeOfficialSectorColor(asString(lap["sector3_color"])),
		}

		updateQ3SectorStats(&stats, abbr, lapSeconds, [3]string{bestLap.Sector1, bestLap.Sector2, bestLap.Sector3})

		prev, exists := bestByDriver[abbr]
		if !exists || lapSeconds < prev.Seconds {
			bestByDriver[abbr] = bestLap
		}
	}

	return bestByDriver, stats
}

func updateQ3SectorStats(stats *q3SectorStats, abbr string, lapSeconds float64, sectors [3]string) {
	if _, exists := stats.driverBest[abbr]; !exists {
		stats.driverBest[abbr] = [3]float64{math.MaxFloat64, math.MaxFloat64, math.MaxFloat64}
	}
	driverBest := stats.driverBest[abbr]
	driverCount := stats.driverCount[abbr]

	for i := 0; i < len(sectors); i++ {
		sectorSeconds, ok := parseLapTimeToSeconds(sectors[i])
		if !ok {
			continue
		}

		driverCount[i]++
		if sectorSeconds < driverBest[i]-q3SectorTolerance {
			driverBest[i] = sectorSeconds
		}

		leader := stats.overallLeaders[i]
		isBetter := sectorSeconds < leader.Sector-q3SectorTolerance
		isTied := math.Abs(sectorSeconds-leader.Sector) <= q3SectorTolerance
		isTieBreakBetter := isTied && (lapSeconds < leader.LapSeconds-q3SectorTolerance ||
			(math.Abs(lapSeconds-leader.LapSeconds) <= q3SectorTolerance &&
				(leader.Abbr == "" || abbr < leader.Abbr)))
		if isBetter || isTieBreakBetter {
			stats.overallLeaders[i] = q3OverallSectorLeader{
				Abbr:       abbr,
				Sector:     sectorSeconds,
				LapSeconds: lapSeconds,
			}
		}
	}

	stats.driverBest[abbr] = driverBest
	stats.driverCount[abbr] = driverCount
}

func q3TrackNorm(track map[string]any) (float64, float64, float64) {
	norm, _ := track["norm"].(map[string]any)
	xMin := asFloat(norm["x_min"], 0)
	yMin := asFloat(norm["y_min"], 0)
	scale := asFloat(norm["scale"], 1)
	if scale <= 0 {
		scale = 1
	}
	return xMin, yMin, scale
}

func extractQ3LapPoints(driverPos []posSample, driverTiming []timingState, best q3BestLap) []posSample {
	if len(driverPos) == 0 || len(driverTiming) == 0 {
		return nil
	}

	var points []posSample
	if best.EndTs > 0 && best.Seconds > 0 {
		points = extractQ3LapPointsByTimestamp(driverPos, best)
	}
	if len(points) < q3MinLinePointCount {
		points = extractQ3LapPointsByLapNumber(driverPos, driverTiming, best.Lap)
	}
	if len(points) < q3MinLinePointCount {
		return nil
	}

	sort.Slice(points, func(i, j int) bool { return points[i].T < points[j].T })
	points = sanitizeTrackLap(points)
	if len(points) < q3MinLinePointCount {
		return nil
	}
	return points
}

func extractQ3LapPointsByTimestamp(driverPos []posSample, best q3BestLap) []posSample {
	lapStartTs := best.EndTs - best.Seconds
	startPoint := interpolatePosSampleAt(driverPos, lapStartTs, q3BoundaryMaxGap)
	endPoint := interpolatePosSampleAt(driverPos, best.EndTs, q3BoundaryMaxGap)
	if startPoint != nil && endPoint != nil {
		out := make([]posSample, 0, 1024)
		out = append(out, *startPoint)
		for _, p := range driverPos {
			if p.T <= lapStartTs || p.T >= best.EndTs {
				continue
			}
			out = append(out, p)
		}
		out = append(out, *endPoint)
		return out
	}

	// Fallback for timelines where lap completion timestamps are not on the same absolute axis
	// as position samples. This preserves compatibility for older cached artifacts/tests.
	out := make([]posSample, 0, 1024)
	if ps := nearestPosSampleWithin(driverPos, lapStartTs, q3BoundaryNearestSnap); ps != nil {
		out = append(out, *ps)
	}
	for _, p := range driverPos {
		if p.T < lapStartTs-q3BoundaryPad || p.T > best.EndTs+q3BoundaryPad {
			continue
		}
		out = append(out, p)
	}
	if pe := nearestPosSampleWithin(driverPos, best.EndTs, q3BoundaryNearestSnap); pe != nil {
		out = append(out, *pe)
	}
	return out
}

func extractQ3LapPointsByLapNumber(driverPos []posSample, driverTiming []timingState, lapNumber int) []posSample {
	if lapNumber <= 0 {
		return nil
	}
	out := make([]posSample, 0, 1024)
	for _, p := range driverPos {
		if lapAt(driverTiming, p.T) != lapNumber {
			continue
		}
		out = append(out, p)
	}
	return out
}

func buildQ3LineSamples(points []posSample, lapSeconds, xMin, yMin, scale float64) []map[string]any {
	if len(points) < 2 || lapSeconds <= 0 {
		return nil
	}
	stepDist := make([]float64, len(points))
	totalDist := 0.0
	for i := 1; i < len(points); i++ {
		totalDist += pointDistance(points[i-1], points[i])
		stepDist[i] = totalDist
	}
	if totalDist <= 0 {
		return nil
	}

	samples := make([]map[string]any, 0, len(points))
	for i, pt := range points {
		progress := stepDist[i] / totalDist
		if progress < 0 {
			progress = 0
		} else if progress > 1 {
			progress = 1
		}
		samples = append(samples, map[string]any{
			"x": normalizeCoord(pt.X, xMin, scale),
			"y": normalizeCoord(pt.Y, yMin, scale),
			"t": round3(progress * lapSeconds),
			"p": round6(progress),
		})
	}
	samples[0]["t"] = 0.0
	samples[len(samples)-1]["t"] = round3(lapSeconds)
	return decimateLineSamples(samples, q3MaxLineSampleCount)
}

func classifyQ3SectorColors(abbr string, best q3BestLap, stats q3SectorStats) map[string]any {
	colors := map[string]any{"s1": nil, "s2": nil, "s3": nil}
	sectorRaw := [3]string{best.Sector1, best.Sector2, best.Sector3}
	officialColors := [3]string{best.Sector1Color, best.Sector2Color, best.Sector3Color}
	driverBest := stats.driverBest[abbr]
	driverCount := stats.driverCount[abbr]

	for i, key := range []string{"s1", "s2", "s3"} {
		color := normalizeOfficialSectorColor(officialColors[i])
		if color == "" {
			sectorSeconds, ok := parseLapTimeToSeconds(sectorRaw[i])
			if ok {
				leader := stats.overallLeaders[i]
				switch {
				case leader.Abbr == abbr && leader.Sector < math.MaxFloat64 && math.Abs(sectorSeconds-leader.Sector) <= q3SectorTolerance:
					color = "purple"
				case driverCount[i] <= 1:
					color = "green"
				case driverBest[i] < math.MaxFloat64 && math.Abs(sectorSeconds-driverBest[i]) <= q3SectorTolerance:
					color = "green"
				default:
					color = "yellow"
				}
			}
		}
		colors[key] = nilIfEmptyString(color)
	}

	return colors
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
