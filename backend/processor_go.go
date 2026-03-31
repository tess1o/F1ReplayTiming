package main

import (
	"bufio"
	"bytes"
	"compress/flate"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"f1replaytiming/backend/storage"
)

const defaultF1StaticBase = "https://livetiming.formula1.com/static"

type GoSessionProcessor struct {
	dataDir               string
	store                 *storage.Store
	baseURL               string
	httpClient            *http.Client
	sampleEvery           float64
	fetchWorkers          int
	parseWorkers          int
	replayChunkFrames     int
	telemetryChunkSamples int
	rawMinDelta           float64
	replayChunkCodec      string
	telemetryChunkCodec   string
	circuitMeta           *circuitMetadataIndex
	metaLoadErr           error
}

type seasonIndex struct {
	Year     int             `json:"Year"`
	Meetings []seasonMeeting `json:"Meetings"`
}

type seasonMeeting struct {
	Number       int              `json:"Number"`
	Name         string           `json:"Name"`
	OfficialName string           `json:"OfficialName"`
	Location     string           `json:"Location"`
	Country      meetingCountry   `json:"Country"`
	Circuit      meetingCircuit   `json:"Circuit"`
	Sessions     []meetingSession `json:"Sessions"`
}

type meetingCountry struct {
	Name string `json:"Name"`
}

type meetingCircuit struct {
	ShortName string `json:"ShortName"`
}

type meetingSession struct {
	Name      string `json:"Name"`
	Type      string `json:"Type"`
	Number    *int   `json:"Number"`
	StartDate string `json:"StartDate"`
	EndDate   string `json:"EndDate"`
	GmtOffset string `json:"GmtOffset"`
	Path      string `json:"Path"`
}

type sessionIndex struct {
	Feeds map[string]sessionFeed `json:"Feeds"`
}

type sessionFeed struct {
	KeyFramePath string `json:"KeyFramePath"`
	StreamPath   string `json:"StreamPath"`
}

type driverMeta struct {
	Number string
	Abbr   string
	Name   string
	Team   string
	Color  string
}

type timingState struct {
	T        float64
	Lap      int
	Position int
	Gap      string
	Interval string
	InPit    bool
	PitOut   bool
	Retired  bool
}

type posSample struct {
	T float64
	X float64
	Y float64
}

type carSample struct {
	T        float64
	Speed    float64
	Throttle float64
	Brake    bool
	Gear     int
	RPM      float64
	DRS      int
}

type weatherPoint struct {
	T      float64
	Values map[string]any
}

type trackStatusPoint struct {
	T      float64
	Status string
}

type raceControlPoint struct {
	T       float64
	Message map[string]any
}

type sessionStatusPoint struct {
	T      float64
	Status string
}

type replayWindow struct {
	Start float64
	End   float64
}

type replayWriter struct {
	path       string
	tmpPath    string
	f          *os.File
	offset     int64
	count      int
	frames     []replayFrameMeta
	quali      []map[string]any
	seenPhases map[string]struct{}
	totalTime  float64
	totalLaps  int
}

func NewGoSessionProcessor(dataDir string, store *storage.Store, replayChunkFrames, telemetryChunkSamples int) *GoSessionProcessor {
	baseURL := strings.TrimSpace(os.Getenv("F1_STATIC_BASE_URL"))
	if baseURL == "" {
		baseURL = defaultF1StaticBase
	}
	sampleEvery := 0.5
	if raw := strings.TrimSpace(os.Getenv("REPLAY_SAMPLE_INTERVAL_SECONDS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			sampleEvery = v
		}
	}
	if sampleEvery < 0.25 {
		sampleEvery = 0.25
	}
	fetchWorkers := 3
	if raw := strings.TrimSpace(os.Getenv("PROCESS_FETCH_CONCURRENCY")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			fetchWorkers = v
		}
	}
	parseWorkers := 2
	if raw := strings.TrimSpace(os.Getenv("PROCESS_PARSE_CONCURRENCY")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			parseWorkers = v
		}
	}
	meta, metaErr := loadCircuitMetadata(embeddedCircuitMetadata)
	if replayChunkFrames <= 0 {
		replayChunkFrames = 256
	}
	if telemetryChunkSamples <= 0 {
		telemetryChunkSamples = 512
	}
	rawMinDelta := 0.10
	if raw := strings.TrimSpace(os.Getenv("PROCESS_RAW_MIN_DT_SECONDS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v >= 0 {
			rawMinDelta = v
		}
	}
	chunkCodec := strings.ToLower(strings.TrimSpace(os.Getenv("PROCESS_CHUNK_CODEC")))
	if chunkCodec == "" {
		chunkCodec = storage.CodecProtobuf
	}
	if chunkCodec != storage.CodecProtobuf && chunkCodec != storage.CodecProtobufZstd {
		chunkCodec = storage.CodecProtobuf
	}

	return &GoSessionProcessor{
		dataDir: dataDir,
		store:   store,
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		sampleEvery:           sampleEvery,
		fetchWorkers:          fetchWorkers,
		parseWorkers:          parseWorkers,
		replayChunkFrames:     replayChunkFrames,
		telemetryChunkSamples: telemetryChunkSamples,
		rawMinDelta:           rawMinDelta,
		replayChunkCodec:      chunkCodec,
		telemetryChunkCodec:   chunkCodec,
		circuitMeta:           meta,
		metaLoadErr:           metaErr,
	}
}

func (p *GoSessionProcessor) EnsureSchedule(ctx context.Context, year int) error {
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	idx, err := p.fetchSeasonIndex(ctx, year)
	if err != nil {
		return err
	}
	events := p.buildScheduleEvents(idx)
	if len(events) == 0 {
		return errors.New("season schedule is empty")
	}
	out := map[string]any{
		"year":   year,
		"events": events,
	}
	rel := filepath.Join("seasons", strconv.Itoa(year), "schedule.json")
	if err := p.writeJSONAtomic(rel, out); err != nil {
		return err
	}
	return p.store.UpsertSchedule(ctx, year, out)
}

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

	status("Parsing timing stream...")
	timingTimeline, laps, latestState, err := p.parseTimingDataStream(ctx, sessionPath, sidx.Feeds["TimingData"], driverByNum, timingAppRaw)
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

func (p *GoSessionProcessor) fetchSeasonIndex(ctx context.Context, year int) (*seasonIndex, error) {
	url := fmt.Sprintf("%s/%d/Index.json", p.baseURL, year)
	var out seasonIndex
	if err := p.fetchJSON(ctx, url, &out); err != nil {
		return nil, fmt.Errorf("fetch season index: %w", err)
	}
	return &out, nil
}

func (p *GoSessionProcessor) buildScheduleEvents(idx *seasonIndex) []map[string]any {
	now := time.Now().UTC()
	out := make([]map[string]any, 0, len(idx.Meetings))
	for _, m := range idx.Meetings {
		if isTestingMeeting(m.Name, m.OfficialName) {
			continue
		}
		sessions := make([]map[string]any, 0, len(m.Sessions))
		hasAnyAvailable := false
		for _, s := range m.Sessions {
			name := normalizeSessionName(s.Name)
			if name == "" {
				continue
			}
			startUTC, ok := sessionStartUTC(s.StartDate, s.GmtOffset)
			available := false
			dateUTC := ""
			if ok {
				dateUTC = startUTC.Format(time.RFC3339)
				available = now.After(startUTC.Add(2 * time.Hour))
			}
			if available {
				hasAnyAvailable = true
			}
			sessions = append(sessions, map[string]any{
				"name":      name,
				"date_utc":  dateUTC,
				"available": available,
			})
		}
		eventDate := ""
		if len(sessions) > 0 {
			if dtRaw := asString(sessions[len(sessions)-1]["date_utc"]); dtRaw != "" {
				if ts, ok := parseDateMaybe(dtRaw); ok {
					eventDate = ts.Format("2006-01-02")
				}
			}
		}
		status := "future"
		if hasAnyAvailable {
			status = "available"
		}
		out = append(out, map[string]any{
			"round_number": m.Number,
			"country":      m.Country.Name,
			"event_name":   m.Name,
			"location":     defaultString(m.Location, m.Circuit.ShortName),
			"event_date":   eventDate,
			"sessions":     sessions,
			"status":       status,
		})
	}
	for i := len(out) - 1; i >= 0; i-- {
		if asString(out[i]["status"]) == "available" {
			out[i]["status"] = "latest"
			break
		}
	}
	return out
}

func isTestingMeeting(name, official string) bool {
	n := strings.ToLower(name + " " + official)
	return strings.Contains(n, "testing")
}

func normalizeSessionName(name string) string {
	switch strings.TrimSpace(name) {
	case "Race":
		return "Race"
	case "Qualifying":
		return "Qualifying"
	case "Sprint":
		return "Sprint"
	case "Sprint Qualifying", "Sprint Shootout":
		return "Sprint Qualifying"
	case "Practice 1":
		return "Practice 1"
	case "Practice 2":
		return "Practice 2"
	case "Practice 3":
		return "Practice 3"
	default:
		return ""
	}
}

func sessionStartUTC(startDate, gmtOffset string) (time.Time, bool) {
	startDate = strings.TrimSpace(startDate)
	if startDate == "" {
		return time.Time{}, false
	}
	// startDate from feed is local meeting time without timezone.
	local, err := time.Parse("2006-01-02T15:04:05", startDate)
	if err != nil {
		return time.Time{}, false
	}
	off := parseHMSOffset(gmtOffset)
	loc := time.FixedZone("meeting", int(off.Seconds()))
	withTZ := time.Date(local.Year(), local.Month(), local.Day(), local.Hour(), local.Minute(), local.Second(), 0, loc)
	return withTZ.UTC(), true
}

func parseHMSOffset(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	sign := 1
	if strings.HasPrefix(raw, "-") {
		sign = -1
		raw = strings.TrimPrefix(raw, "-")
	} else {
		raw = strings.TrimPrefix(raw, "+")
	}
	parts := strings.Split(raw, ":")
	if len(parts) < 2 {
		return 0
	}
	h, _ := strconv.Atoi(parts[0])
	m, _ := strconv.Atoi(parts[1])
	s := 0
	if len(parts) > 2 {
		s, _ = strconv.Atoi(parts[2])
	}
	d := time.Duration(h)*time.Hour + time.Duration(m)*time.Minute + time.Duration(s)*time.Second
	if sign < 0 {
		d = -d
	}
	return d
}

func (p *GoSessionProcessor) findMeetingSession(idx *seasonIndex, round int, sessionType string) (*seasonMeeting, *meetingSession, error) {
	var meeting *seasonMeeting
	for i := range idx.Meetings {
		m := &idx.Meetings[i]
		if isTestingMeeting(m.Name, m.OfficialName) {
			continue
		}
		if m.Number == round {
			meeting = m
			break
		}
	}
	if meeting == nil {
		return nil, nil, fmt.Errorf("round %d not found in season index", round)
	}
	wantNames := desiredSessionNames(sessionType)
	for _, want := range wantNames {
		for i := range meeting.Sessions {
			s := &meeting.Sessions[i]
			if strings.EqualFold(strings.TrimSpace(s.Name), want) {
				return meeting, s, nil
			}
		}
	}
	return nil, nil, fmt.Errorf("session %s not found for round %d", sessionType, round)
}

func desiredSessionNames(sessionType string) []string {
	switch strings.ToUpper(strings.TrimSpace(sessionType)) {
	case "R":
		return []string{"Race"}
	case "Q":
		return []string{"Qualifying"}
	case "S":
		return []string{"Sprint"}
	case "SQ":
		return []string{"Sprint Qualifying", "Sprint Shootout"}
	case "FP1":
		return []string{"Practice 1"}
	case "FP2":
		return []string{"Practice 2"}
	case "FP3":
		return []string{"Practice 3"}
	default:
		return []string{sessionType}
	}
}

func (p *GoSessionProcessor) fetchSessionIndex(ctx context.Context, sessionPath string) (*sessionIndex, error) {
	url := fmt.Sprintf("%s/%s/Index.json", p.baseURL, strings.Trim(sessionPath, "/"))
	var out sessionIndex
	if err := p.fetchJSON(ctx, url, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *GoSessionProcessor) fetchFeedKeyframeJSON(ctx context.Context, sessionPath string, feed sessionFeed) (map[string]any, error) {
	path := strings.TrimSpace(feed.KeyFramePath)
	if path == "" {
		return map[string]any{}, nil
	}
	url := fmt.Sprintf("%s/%s/%s", p.baseURL, strings.Trim(sessionPath, "/"), strings.Trim(path, "/"))
	raw, err := p.fetchBytes(ctx, url)
	if err != nil {
		return nil, err
	}
	if strings.Contains(path, ".z.json") {
		var encoded string
		if err := json.Unmarshal(raw, &encoded); err != nil {
			return nil, err
		}
		dec, err := decodeZPayload(encoded)
		if err != nil {
			return nil, err
		}
		raw = dec
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

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

func lapAt(tl []timingState, t float64) int {
	s := latestTimingAt(tl, t)
	if s == nil {
		return 0
	}
	return s.Lap
}

func latestTimingAt(tl []timingState, t float64) *timingState {
	if len(tl) == 0 {
		return nil
	}
	i := sort.Search(len(tl), func(i int) bool { return tl[i].T > t }) - 1
	if i < 0 {
		return nil
	}
	return &tl[i]
}

func nearestPosSample(arr []posSample, t float64) *posSample {
	if len(arr) == 0 {
		return nil
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T >= t })
	if i == 0 {
		return &arr[0]
	}
	if i >= len(arr) {
		return &arr[len(arr)-1]
	}
	prev := arr[i-1]
	next := arr[i]
	if math.Abs(prev.T-t) <= math.Abs(next.T-t) {
		return &prev
	}
	return &next
}

func nearestPosSampleWithin(arr []posSample, t, maxDeltaSeconds float64) *posSample {
	ps := nearestPosSample(arr, t)
	if ps == nil {
		return nil
	}
	if maxDeltaSeconds <= 0 {
		return ps
	}
	if math.Abs(ps.T-t) > maxDeltaSeconds {
		return nil
	}
	return ps
}

func latestCarAt(arr []carSample, t float64) *carSample {
	if len(arr) == 0 {
		return nil
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
	if i < 0 {
		return nil
	}
	return &arr[i]
}

func trackStatusAt(arr []trackStatusPoint, t float64) string {
	if len(arr) == 0 {
		return "green"
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
	if i < 0 {
		return "green"
	}
	switch asString(arr[i].Status) {
	case "1", "7":
		return "green"
	case "2":
		return "yellow"
	case "4":
		return "sc"
	case "5":
		return "red"
	case "6":
		return "vsc"
	default:
		return "green"
	}
}

func weatherAt(arr []weatherPoint, t float64) map[string]any {
	if len(arr) == 0 {
		return nil
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
	if i < 0 {
		return nil
	}
	return arr[i].Values
}

func raceControlAt(arr []raceControlPoint, t float64) []map[string]any {
	if len(arr) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, 50)
	for i := len(arr) - 1; i >= 0 && len(out) < 50; i-- {
		msgTs := asFloat(arr[i].Message["timestamp"], 0)
		if msgTs <= 0 || msgTs <= t+1e-9 {
			out = append(out, arr[i].Message)
		}
	}
	return out
}

func newReplayWriter(absPath string) (*replayWriter, error) {
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return nil, err
	}
	tmp := absPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write([]byte("[")); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &replayWriter{
		path:       absPath,
		tmpPath:    tmp,
		f:          f,
		offset:     1,
		frames:     make([]replayFrameMeta, 0, 4096),
		quali:      make([]map[string]any, 0, 3),
		seenPhases: map[string]struct{}{},
	}, nil
}

func (w *replayWriter) writeFrame(frame map[string]any) error {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if w.count > 0 {
		if _, err := w.f.Write([]byte(",")); err != nil {
			return err
		}
		w.offset++
	}
	start := w.offset
	if _, err := w.f.Write(encoded); err != nil {
		return err
	}
	w.offset += int64(len(encoded))
	end := w.offset
	w.frames = append(w.frames, replayFrameMeta{
		Start:     start,
		End:       end,
		Timestamp: asFloat(frame["timestamp"], 0),
		Lap:       asInt(frame["lap"]),
	})
	w.totalTime = asFloat(frame["timestamp"], 0)
	w.totalLaps = asInt(frame["total_laps"])
	if qp, ok := frame["quali_phase"].(map[string]any); ok {
		phase := strings.TrimSpace(asString(qp["phase"]))
		if phase != "" {
			if _, seen := w.seenPhases[phase]; !seen {
				w.seenPhases[phase] = struct{}{}
				w.quali = append(w.quali, map[string]any{
					"phase":     phase,
					"timestamp": asFloat(frame["timestamp"], 0),
				})
			}
		}
	}
	w.count++
	return nil
}

func (w *replayWriter) commit() error {
	if _, err := w.f.Write([]byte("]")); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	return os.Rename(w.tmpPath, w.path)
}

func (w *replayWriter) closeNoReplace() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
}

func (p *GoSessionProcessor) writeJSONAtomic(rel string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	rel = filepath.ToSlash(rel)
	if err := p.store.PutJSONArtifact(context.Background(), rel, body); err != nil {
		return err
	}
	log.Printf("processor(go): saved %s (%d bytes)", rel, len(body))
	return nil
}

func (p *GoSessionProcessor) writeJSONObjectItems(rel string, items []struct {
	K string
	V map[string]any
}) error {
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if _, err := buf.Write([]byte("{")); err != nil {
		return err
	}
	for i, it := range items {
		if i > 0 {
			if _, err := buf.Write([]byte(",")); err != nil {
				return err
			}
		}
		kb, _ := json.Marshal(it.K)
		vb, _ := json.Marshal(it.V)
		if _, err := buf.Write(kb); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(":")); err != nil {
			return err
		}
		if _, err := buf.Write(vb); err != nil {
			return err
		}
	}
	if _, err := buf.Write([]byte("}")); err != nil {
		return err
	}
	return p.store.PutJSONArtifact(context.Background(), filepath.ToSlash(rel), buf.Bytes())
}

func (p *GoSessionProcessor) fetchJSON(ctx context.Context, url string, out any) error {
	body, err := p.fetchBytes(ctx, url)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

func (p *GoSessionProcessor) fetchBytes(ctx context.Context, url string) ([]byte, error) {
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return trimBOM(body), nil
}

func trimBOM(b []byte) []byte {
	return bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
}

func (p *GoSessionProcessor) doRequest(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("http %s -> %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func parseStreamTimestamp(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 12 {
		return 0, false
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return 0, false
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	sec, err3 := strconv.ParseFloat(parts[2], 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, false
	}
	return float64(h*3600+m*60) + sec, true
}

func decodeMaybeZStreamPayload(payload string) ([]byte, error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return nil, errors.New("empty payload")
	}
	// For .z streams payload is a JSON string, not an object.
	if strings.HasPrefix(payload, "{") || strings.HasPrefix(payload, "[") {
		return []byte(payload), nil
	}
	var encoded string
	if strings.HasPrefix(payload, "\"") {
		if err := json.Unmarshal([]byte(payload), &encoded); err != nil {
			return nil, err
		}
	} else {
		encoded = payload
	}
	return decodeZPayload(encoded)
}

func decodeZPayload(encoded string) ([]byte, error) {
	enc := strings.TrimSpace(encoded)
	if enc == "" {
		return nil, errors.New("empty encoded payload")
	}
	data, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, err
	}
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	return io.ReadAll(r)
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

func zeroToNil(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}
