package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	openF1ChunkMinutesDefault = 30
	openF1ChunkMinSeconds     = 300
)

type openF1Meeting struct {
	MeetingKey       int    `json:"meeting_key"`
	MeetingName      string `json:"meeting_name"`
	Location         string `json:"location"`
	CountryName      string `json:"country_name"`
	CircuitKey       int    `json:"circuit_key"`
	CircuitShortName string `json:"circuit_short_name"`
	DateStart        string `json:"date_start"`
	DateEnd          string `json:"date_end"`
	Year             int    `json:"year"`
	IsCancelled      bool   `json:"is_cancelled"`
}

type openF1Session struct {
	SessionKey       int    `json:"session_key"`
	SessionType      string `json:"session_type"`
	SessionName      string `json:"session_name"`
	DateStart        string `json:"date_start"`
	DateEnd          string `json:"date_end"`
	MeetingKey       int    `json:"meeting_key"`
	CircuitKey       int    `json:"circuit_key"`
	CircuitShortName string `json:"circuit_short_name"`
	CountryName      string `json:"country_name"`
	Location         string `json:"location"`
	Year             int    `json:"year"`
}

type openF1Driver struct {
	DriverNumber int    `json:"driver_number"`
	FullName     string `json:"full_name"`
	NameAcronym  string `json:"name_acronym"`
	TeamName     string `json:"team_name"`
	TeamColour   string `json:"team_colour"`
}

type openF1Lap struct {
	DriverNumber    int     `json:"driver_number"`
	LapNumber       int     `json:"lap_number"`
	DateStart       string  `json:"date_start"`
	LapDuration     float64 `json:"lap_duration"`
	DurationSector1 float64 `json:"duration_sector_1"`
	DurationSector2 float64 `json:"duration_sector_2"`
	DurationSector3 float64 `json:"duration_sector_3"`
	IsPitOutLap     bool    `json:"is_pit_out_lap"`
}

type openF1Position struct {
	Date         string `json:"date"`
	DriverNumber int    `json:"driver_number"`
	Position     int    `json:"position"`
}

type openF1Location struct {
	Date         string  `json:"date"`
	DriverNumber int     `json:"driver_number"`
	X            float64 `json:"x"`
	Y            float64 `json:"y"`
}

type openF1CarData struct {
	Date         string  `json:"date"`
	DriverNumber int     `json:"driver_number"`
	Speed        float64 `json:"speed"`
	Throttle     float64 `json:"throttle"`
	Brake        int     `json:"brake"`
	Gear         int     `json:"n_gear"`
	RPM          float64 `json:"rpm"`
	DRS          any     `json:"drs"`
}

type openF1Interval struct {
	Date         string `json:"date"`
	DriverNumber int    `json:"driver_number"`
	GapToLeader  any    `json:"gap_to_leader"`
	Interval     any    `json:"interval"`
}

type openF1Stint struct {
	DriverNumber   int    `json:"driver_number"`
	StintNumber    int    `json:"stint_number"`
	LapStart       int    `json:"lap_start"`
	LapEnd         int    `json:"lap_end"`
	Compound       string `json:"compound"`
	TyreAgeAtStart int    `json:"tyre_age_at_start"`
}

type openF1Pit struct {
	Date         string  `json:"date"`
	DriverNumber int     `json:"driver_number"`
	LapNumber    int     `json:"lap_number"`
	PitDuration  float64 `json:"pit_duration"`
}

type openF1Weather struct {
	Date             string  `json:"date"`
	AirTemperature   float64 `json:"air_temperature"`
	TrackTemperature float64 `json:"track_temperature"`
	WindSpeed        float64 `json:"wind_speed"`
	WindDirection    float64 `json:"wind_direction"`
	Humidity         float64 `json:"humidity"`
	Pressure         float64 `json:"pressure"`
	Rainfall         float64 `json:"rainfall"`
}

type openF1RaceControl struct {
	Date      string `json:"date"`
	Driver    *int   `json:"driver_number"`
	Category  string `json:"category"`
	Flag      any    `json:"flag"`
	Scope     any    `json:"scope"`
	Sector    any    `json:"sector"`
	LapNumber any    `json:"lap_number"`
	Message   string `json:"message"`
}

type openF1SessionResult struct {
	DriverNumber int  `json:"driver_number"`
	Position     int  `json:"position"`
	NumberOfLaps int  `json:"number_of_laps"`
	DNF          bool `json:"dnf"`
	DNS          bool `json:"dns"`
	DSQ          bool `json:"dsq"`
	Duration     any  `json:"duration"`
	GapToLeader  any  `json:"gap_to_leader"`
	Points       any  `json:"points"`
}

type openF1SessionContext struct {
	Meeting      openF1Meeting
	Session      openF1Session
	MeetingModel *seasonMeeting
	StartUTC     time.Time
	EndUTC       time.Time
}

type openF1HTTPError struct {
	URL    string
	Status int
	Body   string
}

func (e openF1HTTPError) Error() string {
	return fmt.Sprintf("http %s -> %d: %s", e.URL, e.Status, strings.TrimSpace(e.Body))
}

func (p *GoSessionProcessor) processSessionViaOpenF1(ctx context.Context, year, round int, sessionType string, onStatus func(string)) error {
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	status := func(msg string) {
		if onStatus != nil {
			onStatus(msg)
		}
		log.Printf("processor(openf1): %s", msg)
	}

	sessionType = strings.ToUpper(strings.TrimSpace(sessionType))
	isQualifying := sessionType == "Q" || sessionType == "SQ"
	baseOut := filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType)

	status("Resolving session metadata...")
	meta, err := p.resolveOpenF1SessionContext(ctx, year, round, sessionType)
	if err != nil {
		return err
	}

	status("Loading driver roster...")
	driversRaw := make([]openF1Driver, 0, 24)
	if err := p.fetchOpenF1JSON(ctx, "drivers", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &driversRaw); err != nil {
		return err
	}
	drivers, driverByNum := buildDriversFromOpenF1(driversRaw)
	if len(drivers) == 0 {
		return errors.New("openf1 driver list is empty")
	}

	sessionInfoRaw := buildOpenF1SessionInfo(meta)
	circuitMeta, circuitName, err := p.resolveCircuitMetadata(sessionInfoRaw)
	if err != nil {
		return err
	}

	status("Loading laps and stints...")
	lapsRaw := make([]openF1Lap, 0, 2048)
	if err := p.fetchOpenF1JSON(ctx, "laps", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &lapsRaw); err != nil {
		return err
	}
	stintsRaw := make([]openF1Stint, 0, 512)
	if err := p.fetchOpenF1JSON(ctx, "stints", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &stintsRaw); err != nil {
		return err
	}
	pitsRaw := make([]openF1Pit, 0, 512)
	_ = p.fetchOpenF1JSON(ctx, "pit", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &pitsRaw)

	status("Loading timing and positions...")
	positionsRaw := make([]openF1Position, 0, 4096)
	if err := p.fetchOpenF1JSON(ctx, "position", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &positionsRaw); err != nil {
		return err
	}
	intervalsRaw := make([]openF1Interval, 0, 4096)
	_ = p.fetchOpenF1JSON(ctx, "intervals", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &intervalsRaw)

	status("Loading car location and telemetry...")
	windowStart := meta.StartUTC.Add(-15 * time.Minute)
	windowEnd := meta.EndUTC.Add(15 * time.Minute)
	locationsRaw, err := fetchOpenF1Chunked[openF1Location](ctx, p, "location", meta.Session.SessionKey, windowStart, windowEnd)
	if err != nil {
		return err
	}
	carDataRaw, err := fetchOpenF1Chunked[openF1CarData](ctx, p, "car_data", meta.Session.SessionKey, windowStart, windowEnd)
	if err != nil {
		return err
	}

	status("Loading weather and race control...")
	weatherRaw := make([]openF1Weather, 0, 1024)
	_ = p.fetchOpenF1JSON(ctx, "weather", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &weatherRaw)
	rcRaw := make([]openF1RaceControl, 0, 1024)
	_ = p.fetchOpenF1JSON(ctx, "race_control", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &rcRaw)
	resultsRaw := make([]openF1SessionResult, 0, 64)
	_ = p.fetchOpenF1JSON(ctx, "session_result", map[string]string{
		"session_key": strconv.Itoa(meta.Session.SessionKey),
	}, &resultsRaw)

	lapState := buildOpenF1LapState(lapsRaw, driverByNum, meta.StartUTC, windowStart, windowEnd)
	stintByAbbr := buildStintsFromOpenF1(stintsRaw, driverByNum)
	timingTimeline := buildTimingFromOpenF1(positionsRaw, intervalsRaw, lapState, driverByNum, meta.StartUTC, windowStart, windowEnd)
	posTimeline := buildPosTimelineFromOpenF1(locationsRaw, meta.StartUTC, driverByNum, windowStart, windowEnd)
	if len(posTimeline) == 0 {
		return errors.New("openf1 location stream is empty")
	}
	carTimeline := buildCarTimelineFromOpenF1(carDataRaw, meta.StartUTC, driverByNum, windowStart, windowEnd)
	weatherPoints := buildWeatherFromOpenF1(weatherRaw, meta.StartUTC, windowStart, windowEnd)
	rcPoints := buildRaceControlFromOpenF1(rcRaw, meta.StartUTC, windowStart, windowEnd)
	trackStatuses := buildTrackStatusFromOpenF1(rcRaw, meta.StartUTC, windowStart, windowEnd)
	laps := buildLapsFromOpenF1(lapState, stintByAbbr, pitsRaw, meta.StartUTC, driverByNum)
	latestState := latestTimingByDriver(timingTimeline)
	results := buildResultsFromOpenF1(resultsRaw, driverByNum, latestState, sessionType)
	if len(results) == 0 {
		results = buildResultsJSON(latestState, driverByNum)
	}

	info := buildInfoJSON(year, round, sessionType, meta.MeetingModel, sessionInfoRaw, drivers)
	if err := p.writeJSONAtomic(filepath.Join(baseOut, "info.json"), info); err != nil {
		return err
	}
	sessionID, err := p.store.UpsertSessionInfo(ctx, year, round, sessionType, info)
	if err != nil {
		return fmt.Errorf("upsert session info: %w", err)
	}

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

	status("Writing replay frames...")
	replayIdx, err := p.writeReplayFromTimelines(ctx, sessionID, drivers, timingTimeline, posTimeline, carTimeline, trackStatuses, weatherPoints, rcPoints, sessionType, nil, stintByAbbr)
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
	status("Processing complete (openf1)")
	return nil
}

func (p *GoSessionProcessor) resolveOpenF1SessionContext(ctx context.Context, year, round int, sessionType string) (*openF1SessionContext, error) {
	meetings := make([]openF1Meeting, 0, 64)
	if err := p.fetchOpenF1JSON(ctx, "meetings", map[string]string{
		"year": strconv.Itoa(year),
	}, &meetings); err != nil {
		return nil, err
	}
	raceMeetings := make([]openF1Meeting, 0, len(meetings))
	for _, m := range meetings {
		if m.IsCancelled || isTestingMeeting(m.MeetingName, m.MeetingName) {
			continue
		}
		if m.Year != 0 && m.Year != year {
			continue
		}
		raceMeetings = append(raceMeetings, m)
	}
	sort.Slice(raceMeetings, func(i, j int) bool {
		ti, _ := parseOpenF1Time(raceMeetings[i].DateStart)
		tj, _ := parseOpenF1Time(raceMeetings[j].DateStart)
		return ti.Before(tj)
	})
	if round <= 0 || round > len(raceMeetings) {
		return nil, fmt.Errorf("openf1 meeting not found for year=%d round=%d", year, round)
	}
	meeting := raceMeetings[round-1]

	sessions := make([]openF1Session, 0, 16)
	if err := p.fetchOpenF1JSON(ctx, "sessions", map[string]string{
		"meeting_key": strconv.Itoa(meeting.MeetingKey),
	}, &sessions); err != nil {
		return nil, err
	}
	want := desiredSessionNames(sessionType)
	var selected *openF1Session
	for i := range sessions {
		name := strings.TrimSpace(sessions[i].SessionName)
		for _, w := range want {
			if strings.EqualFold(name, w) {
				selected = &sessions[i]
				break
			}
		}
		if selected != nil {
			break
		}
	}
	if selected == nil {
		return nil, fmt.Errorf("openf1 session %s not found for year=%d round=%d", sessionType, year, round)
	}

	startUTC, ok := parseOpenF1Time(selected.DateStart)
	if !ok {
		return nil, errors.New("openf1 session has invalid start time")
	}
	endUTC, ok := parseOpenF1Time(selected.DateEnd)
	if !ok || !endUTC.After(startUTC) {
		endUTC = startUTC.Add(3 * time.Hour)
	}

	meetingModel := &seasonMeeting{
		Number:   round,
		Name:     meeting.MeetingName,
		Location: meeting.Location,
		Country:  meetingCountry{Name: meeting.CountryName},
		Circuit:  meetingCircuit{ShortName: meeting.CircuitShortName},
	}

	return &openF1SessionContext{
		Meeting:      meeting,
		Session:      *selected,
		MeetingModel: meetingModel,
		StartUTC:     startUTC.UTC(),
		EndUTC:       endUTC.UTC(),
	}, nil
}

func buildDriversFromOpenF1(rows []openF1Driver) ([]driverMeta, map[string]driverMeta) {
	drivers := make([]driverMeta, 0, len(rows))
	byNum := make(map[string]driverMeta, len(rows))
	for _, r := range rows {
		num := strconv.Itoa(r.DriverNumber)
		abbr := strings.ToUpper(strings.TrimSpace(r.NameAcronym))
		if num == "0" || abbr == "" {
			continue
		}
		d := driverMeta{
			Number: num,
			Abbr:   abbr,
			Name:   strings.TrimSpace(r.FullName),
			Team:   strings.TrimSpace(r.TeamName),
			Color:  strings.ToUpper(strings.TrimSpace(r.TeamColour)),
		}
		drivers = append(drivers, d)
		byNum[num] = d
	}
	sort.Slice(drivers, func(i, j int) bool {
		ni, _ := strconv.Atoi(drivers[i].Number)
		nj, _ := strconv.Atoi(drivers[j].Number)
		return ni < nj
	})
	return drivers, byNum
}

func buildOpenF1SessionInfo(meta *openF1SessionContext) map[string]any {
	return map[string]any{
		"Meeting": map[string]any{
			"Name":     meta.Meeting.MeetingName,
			"Location": meta.Meeting.Location,
			"Country": map[string]any{
				"Name": meta.Meeting.CountryName,
			},
			"Circuit": map[string]any{
				"Key":       meta.Meeting.CircuitKey,
				"ShortName": meta.Meeting.CircuitShortName,
			},
		},
	}
}

type openF1LapEvent struct {
	DriverNumber string
	LapNumber    int
	Start        float64
	End          float64
	LapTime      float64
	S1           float64
	S2           float64
	S3           float64
	PitOut       bool
}

type openF1LapState struct {
	ByDriver map[string][]openF1LapEvent
}

func buildOpenF1LapState(rows []openF1Lap, byNum map[string]driverMeta, sessionStart, windowStart, windowEnd time.Time) openF1LapState {
	out := openF1LapState{
		ByDriver: make(map[string][]openF1LapEvent, len(byNum)),
	}
	for _, row := range rows {
		num := strconv.Itoa(row.DriverNumber)
		if _, ok := byNum[num]; !ok || row.LapNumber <= 0 {
			continue
		}
		startUTC, ok := parseOpenF1Time(row.DateStart)
		if !ok {
			continue
		}
		endUTC := startUTC.Add(time.Duration(row.LapDuration * float64(time.Second)))
		if endUTC.Before(windowStart) || startUTC.After(windowEnd) {
			continue
		}
		out.ByDriver[num] = append(out.ByDriver[num], openF1LapEvent{
			DriverNumber: num,
			LapNumber:    row.LapNumber,
			Start:        startUTC.Sub(sessionStart).Seconds(),
			End:          endUTC.Sub(sessionStart).Seconds(),
			LapTime:      row.LapDuration,
			S1:           row.DurationSector1,
			S2:           row.DurationSector2,
			S3:           row.DurationSector3,
			PitOut:       row.IsPitOutLap,
		})
	}
	for num := range out.ByDriver {
		sort.Slice(out.ByDriver[num], func(i, j int) bool {
			return out.ByDriver[num][i].LapNumber < out.ByDriver[num][j].LapNumber
		})
	}
	return out
}

func buildStintsFromOpenF1(rows []openF1Stint, byNum map[string]driverMeta) map[string][]map[string]any {
	out := make(map[string][]map[string]any, len(byNum))
	for _, row := range rows {
		num := strconv.Itoa(row.DriverNumber)
		drv := byNum[num]
		if drv.Abbr == "" || row.LapStart <= 0 {
			continue
		}
		total := 0
		if row.LapEnd >= row.LapStart {
			total = row.LapEnd - row.LapStart + 1
		}
		out[drv.Abbr] = append(out[drv.Abbr], map[string]any{
			"Compound":  strings.ToUpper(strings.TrimSpace(row.Compound)),
			"LapNumber": row.LapStart,
			"TotalLaps": total,
			"StartLaps": max(0, row.TyreAgeAtStart),
		})
	}
	for abbr := range out {
		sort.Slice(out[abbr], func(i, j int) bool {
			return asInt(out[abbr][i]["LapNumber"]) < asInt(out[abbr][j]["LapNumber"])
		})
	}
	return out
}

func buildTimingFromOpenF1(posRows []openF1Position, intervalRows []openF1Interval, laps openF1LapState, byNum map[string]driverMeta, sessionStart, windowStart, windowEnd time.Time) map[string][]timingState {
	type intervalPoint struct {
		T        float64
		Gap      string
		Interval string
	}
	intervalByNum := make(map[string][]intervalPoint, len(byNum))
	for _, row := range intervalRows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		num := strconv.Itoa(row.DriverNumber)
		if _, ok := byNum[num]; !ok {
			continue
		}
		intervalByNum[num] = append(intervalByNum[num], intervalPoint{
			T:        ts.Sub(sessionStart).Seconds(),
			Gap:      formatOpenF1Gap(row.GapToLeader),
			Interval: formatOpenF1Gap(row.Interval),
		})
	}
	for num := range intervalByNum {
		sort.Slice(intervalByNum[num], func(i, j int) bool {
			return intervalByNum[num][i].T < intervalByNum[num][j].T
		})
	}

	out := make(map[string][]timingState, len(byNum))
	for _, row := range posRows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		num := strconv.Itoa(row.DriverNumber)
		if _, ok := byNum[num]; !ok {
			continue
		}
		t := ts.Sub(sessionStart).Seconds()
		completedLap := completedLapsAtOpenF1(laps.ByDriver[num], t)
		gap, interval := "", ""
		arr := intervalByNum[num]
		if len(arr) > 0 {
			i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
			if i >= 0 {
				gap = arr[i].Gap
				interval = arr[i].Interval
			}
		}
		out[num] = append(out[num], timingState{
			T:        t,
			Lap:      completedLap,
			Position: row.Position,
			Gap:      gap,
			Interval: interval,
			InPit:    false,
			PitOut:   false,
			Retired:  false,
		})
	}
	for num := range out {
		sort.Slice(out[num], func(i, j int) bool {
			return out[num][i].T < out[num][j].T
		})
	}
	return out
}

func buildPosTimelineFromOpenF1(rows []openF1Location, sessionStart time.Time, byNum map[string]driverMeta, windowStart, windowEnd time.Time) map[string][]posSample {
	out := make(map[string][]posSample, len(byNum))
	for _, row := range rows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		num := strconv.Itoa(row.DriverNumber)
		if _, ok := byNum[num]; !ok {
			continue
		}
		out[num] = append(out[num], posSample{
			T: ts.Sub(sessionStart).Seconds(),
			X: row.X,
			Y: row.Y,
		})
	}
	for num := range out {
		sort.Slice(out[num], func(i, j int) bool { return out[num][i].T < out[num][j].T })
	}
	return out
}

func buildCarTimelineFromOpenF1(rows []openF1CarData, sessionStart time.Time, byNum map[string]driverMeta, windowStart, windowEnd time.Time) map[string][]carSample {
	out := make(map[string][]carSample, len(byNum))
	for _, row := range rows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		num := strconv.Itoa(row.DriverNumber)
		if _, ok := byNum[num]; !ok {
			continue
		}
		out[num] = append(out[num], carSample{
			T:        ts.Sub(sessionStart).Seconds(),
			Speed:    row.Speed,
			Throttle: row.Throttle,
			Brake:    row.Brake > 0,
			Gear:     row.Gear,
			RPM:      row.RPM,
			DRS:      asInt(row.DRS),
		})
	}
	for num := range out {
		sort.Slice(out[num], func(i, j int) bool { return out[num][i].T < out[num][j].T })
	}
	return out
}

func buildWeatherFromOpenF1(rows []openF1Weather, sessionStart time.Time, windowStart, windowEnd time.Time) []weatherPoint {
	out := make([]weatherPoint, 0, len(rows))
	for _, row := range rows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		out = append(out, weatherPoint{
			T: ts.Sub(sessionStart).Seconds(),
			Values: map[string]any{
				"air_temp":       row.AirTemperature,
				"track_temp":     row.TrackTemperature,
				"wind_speed":     row.WindSpeed,
				"wind_direction": row.WindDirection,
				"humidity":       row.Humidity,
				"pressure":       row.Pressure,
				"rainfall":       row.Rainfall,
			},
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

func buildRaceControlFromOpenF1(rows []openF1RaceControl, sessionStart time.Time, windowStart, windowEnd time.Time) []raceControlPoint {
	out := make([]raceControlPoint, 0, len(rows))
	for _, row := range rows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		entry := map[string]any{
			"message":   row.Message,
			"category":  row.Category,
			"timestamp": ts.Sub(sessionStart).Seconds(),
		}
		if row.Driver != nil && *row.Driver > 0 {
			entry["racing_number"] = strconv.Itoa(*row.Driver)
		}
		if f := asString(row.Flag); f != "" {
			entry["flag"] = f
		}
		if v := asString(row.Scope); v != "" {
			entry["scope"] = v
		}
		if v := asInt(row.Sector); v > 0 {
			entry["sector"] = v
		}
		if v := asInt(row.LapNumber); v > 0 {
			entry["lap"] = v
		}
		out = append(out, raceControlPoint{
			T:       ts.Sub(sessionStart).Seconds(),
			Message: entry,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

func buildTrackStatusFromOpenF1(rows []openF1RaceControl, sessionStart time.Time, windowStart, windowEnd time.Time) []trackStatusPoint {
	out := make([]trackStatusPoint, 0, len(rows))
	for _, row := range rows {
		ts, ok := parseOpenF1Time(row.Date)
		if !ok || ts.Before(windowStart) || ts.After(windowEnd) {
			continue
		}
		status := mapOpenF1TrackStatus(row)
		if status == "" {
			continue
		}
		out = append(out, trackStatusPoint{
			T:      ts.Sub(sessionStart).Seconds(),
			Status: status,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].T < out[j].T })
	return out
}

func mapOpenF1TrackStatus(row openF1RaceControl) string {
	flag := strings.ToUpper(strings.TrimSpace(asString(row.Flag)))
	msg := strings.ToUpper(strings.TrimSpace(row.Message))
	switch {
	case strings.Contains(msg, "VIRTUAL SAFETY CAR"), strings.Contains(msg, "VSC"), strings.Contains(flag, "VSC"):
		return "6"
	case strings.Contains(msg, "SAFETY CAR"), strings.Contains(msg, "SC"), strings.Contains(flag, "SC"):
		return "4"
	case strings.Contains(flag, "RED"), strings.Contains(msg, "RED FLAG"):
		return "5"
	case strings.Contains(flag, "YELLOW"), strings.Contains(msg, "YELLOW FLAG"):
		return "2"
	case strings.Contains(flag, "GREEN"), strings.Contains(msg, "TRACK CLEAR"):
		return "1"
	default:
		return ""
	}
}

func buildLapsFromOpenF1(state openF1LapState, stintsByAbbr map[string][]map[string]any, pits []openF1Pit, sessionStart time.Time, byNum map[string]driverMeta) []map[string]any {
	pitByDriverLap := make(map[string]map[int]bool, 32)
	for _, p := range pits {
		if p.DriverNumber <= 0 || p.LapNumber <= 0 {
			continue
		}
		num := strconv.Itoa(p.DriverNumber)
		if pitByDriverLap[num] == nil {
			pitByDriverLap[num] = make(map[int]bool)
		}
		pitByDriverLap[num][p.LapNumber] = true
	}
	out := make([]map[string]any, 0, 4096)
	for num, laps := range state.ByDriver {
		abbr := byNum[num].Abbr
		if abbr == "" {
			abbr = num
		}
		stints := stintsByAbbr[abbr]
		for _, lap := range laps {
			compound, tyreLife := compoundForLap(stints, lap.LapNumber)
			entry := map[string]any{
				"driver":           abbr,
				"lap_number":       lap.LapNumber,
				"position":         0,
				"lap_time":         formatLapTime(lap.LapTime),
				"time_seconds":     lap.LapTime,
				"time":             round3(lap.End),
				"sector1":          formatSector(lap.S1),
				"sector2":          formatSector(lap.S2),
				"sector3":          formatSector(lap.S3),
				"compound":         compound,
				"tyre_life":        tyreLife,
				"pit_in":           pitByDriverLap[num][lap.LapNumber],
				"pit_out":          lap.PitOut,
				"lap_start_utc":    sessionStart.Add(time.Duration(lap.Start * float64(time.Second))).Format(time.RFC3339Nano),
				"lap_finish_utc":   sessionStart.Add(time.Duration(lap.End * float64(time.Second))).Format(time.RFC3339Nano),
				"lap_duration_raw": lap.LapTime,
			}
			out = append(out, entry)
		}
	}
	sort.Slice(out, func(i, j int) bool {
		di := asString(out[i]["driver"])
		dj := asString(out[j]["driver"])
		if di == dj {
			return asInt(out[i]["lap_number"]) < asInt(out[j]["lap_number"])
		}
		return di < dj
	})
	return out
}

func buildResultsFromOpenF1(rows []openF1SessionResult, byNum map[string]driverMeta, latest map[string]timingState, sessionType string) []map[string]any {
	if len(rows) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		num := strconv.Itoa(row.DriverNumber)
		drv := byNum[num]
		if drv.Abbr == "" {
			continue
		}
		status := "Finished"
		if row.DSQ {
			status = "DSQ"
		} else if row.DNS {
			status = "DNS"
		} else if row.DNF {
			status = "DNF"
		}
		gap := formatOpenF1Gap(row.GapToLeader)
		fastest := ""
		if strings.EqualFold(sessionType, "Q") || strings.EqualFold(sessionType, "SQ") {
			if secs, ok := bestOpenF1DurationSeconds(row.Duration); ok && secs > 0 {
				fastest = formatLapTime(secs)
			}
		}
		out = append(out, map[string]any{
			"position":       row.Position,
			"driver":         drv.Name,
			"abbreviation":   drv.Abbr,
			"team":           drv.Team,
			"team_color":     drv.Color,
			"grid_position":  0,
			"status":         status,
			"points":         asFloat(row.Points, 0),
			"fastest_lap":    fastest,
			"gap_to_leader":  gap,
			"number_of_laps": row.NumberOfLaps,
		})
		if st, ok := latest[num]; ok && st.Position == 0 {
			st.Position = row.Position
			latest[num] = st
		}
	}
	sort.Slice(out, func(i, j int) bool {
		return asInt(out[i]["position"]) < asInt(out[j]["position"])
	})
	return out
}

func latestTimingByDriver(timing map[string][]timingState) map[string]timingState {
	out := make(map[string]timingState, len(timing))
	for num, arr := range timing {
		if len(arr) == 0 {
			continue
		}
		out[num] = arr[len(arr)-1]
	}
	return out
}

func completedLapsAtOpenF1(laps []openF1LapEvent, t float64) int {
	completed := 0
	for _, l := range laps {
		if l.End <= t+1e-9 {
			if l.LapNumber > completed {
				completed = l.LapNumber
			}
		}
	}
	return completed
}

func formatOpenF1Gap(v any) string {
	switch x := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(x)
	case float64:
		if x <= 0 {
			return ""
		}
		return fmt.Sprintf("+%.3f", x)
	case json.Number:
		f, err := x.Float64()
		if err != nil || f <= 0 {
			return ""
		}
		return fmt.Sprintf("+%.3f", f)
	default:
		f := asFloat(v, math.NaN())
		if math.IsNaN(f) || f <= 0 {
			return ""
		}
		return fmt.Sprintf("+%.3f", f)
	}
}

func bestOpenF1DurationSeconds(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, x > 0
	case json.Number:
		f, err := x.Float64()
		return f, err == nil && f > 0
	case []any:
		best := math.MaxFloat64
		ok := false
		for _, item := range x {
			f := asFloat(item, math.NaN())
			if math.IsNaN(f) || f <= 0 {
				continue
			}
			if f < best {
				best = f
			}
			ok = true
		}
		if !ok {
			return 0, false
		}
		return best, true
	default:
		return 0, false
	}
}

func formatSector(v float64) string {
	if v <= 0 {
		return ""
	}
	return fmt.Sprintf("%.3f", v)
}

func compoundForLap(stints []map[string]any, lap int) (any, any) {
	compound, life, _, _ := tyreStateForLap(stints, lap)
	return compound, life
}

func parseOpenF1Time(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}
	if t, err := time.Parse(time.RFC3339Nano, raw); err == nil {
		return t.UTC(), true
	}
	if t, err := time.Parse(time.RFC3339, raw); err == nil {
		return t.UTC(), true
	}
	return time.Time{}, false
}

func (p *GoSessionProcessor) fetchOpenF1JSON(ctx context.Context, endpoint string, params map[string]string, out any) error {
	base := strings.TrimRight(strings.TrimSpace(p.openF1BaseURL), "/")
	if base == "" {
		base = defaultOpenF1Base
	}
	u := base + "/" + strings.TrimLeft(strings.TrimSpace(endpoint), "/")
	q := url.Values{}
	for k, v := range params {
		if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			continue
		}
		q.Set(k, v)
	}
	if encoded := q.Encode(); encoded != "" {
		u += "?" + encoded
	}
	maxRetries := p.openF1MaxRetries
	if maxRetries <= 0 {
		maxRetries = 5
	}
	var lastErr error
	for attempt := 1; attempt <= maxRetries; attempt++ {
		if err := p.waitOpenF1Turn(ctx); err != nil {
			return err
		}
		resp, err := doRequestWithClient(ctx, u, nil, p.httpClient)
		if err != nil {
			lastErr = err
			if attempt < maxRetries {
				if waitErr := p.sleepWithContext(ctx, p.openF1RetryDelay(attempt, 0)); waitErr != nil {
					return waitErr
				}
				continue
			}
			return err
		}
		body, readErr := readResponseBody(resp)
		if readErr != nil {
			lastErr = readErr
			if attempt < maxRetries {
				if waitErr := p.sleepWithContext(ctx, p.openF1RetryDelay(attempt, 0)); waitErr != nil {
					return waitErr
				}
				continue
			}
			return readErr
		}
		if resp.StatusCode >= 200 && resp.StatusCode < 300 {
			return json.Unmarshal(trimBOM(body), out)
		}
		lastErr = openF1HTTPError{
			URL:    u,
			Status: resp.StatusCode,
			Body:   string(body),
		}
		if (resp.StatusCode != 429 && (resp.StatusCode < 500 || resp.StatusCode > 599)) || attempt >= maxRetries {
			return lastErr
		}
		retryAfter := time.Duration(0)
		if resp.StatusCode == 429 {
			retryAfter = parseRetryAfter(resp.Header.Get("Retry-After"))
			log.Printf("processor(openf1): rate limited endpoint=%s attempt=%d/%d retry_after=%s", endpoint, attempt, maxRetries, retryAfter)
		} else {
			log.Printf("processor(openf1): server error endpoint=%s status=%d attempt=%d/%d", endpoint, resp.StatusCode, attempt, maxRetries)
		}
		if waitErr := p.sleepWithContext(ctx, p.openF1RetryDelay(attempt, retryAfter)); waitErr != nil {
			return waitErr
		}
	}
	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("http %s failed after retries", u)
}

func fetchOpenF1Chunked[T any](ctx context.Context, p *GoSessionProcessor, endpoint string, sessionKey int, startUTC, endUTC time.Time) ([]T, error) {
	if !endUTC.After(startUTC) {
		return nil, errors.New("invalid openf1 chunk window")
	}
	chunkMin := readPositiveIntEnv("OPENF1_CHUNK_MINUTES", openF1ChunkMinutesDefault)
	chunkDur := time.Duration(chunkMin) * time.Minute
	if chunkDur <= 0 {
		chunkDur = 30 * time.Minute
	}
	minChunkSeconds := readPositiveIntEnv("OPENF1_CHUNK_MIN_SECONDS", openF1ChunkMinSeconds)
	minChunkDur := time.Duration(minChunkSeconds) * time.Second
	if minChunkDur <= 0 {
		minChunkDur = 5 * time.Minute
	}
	out := make([]T, 0, 8192)
	for chunkStart := startUTC; chunkStart.Before(endUTC); chunkStart = chunkStart.Add(chunkDur) {
		chunkEnd := chunkStart.Add(chunkDur)
		if chunkEnd.After(endUTC) {
			chunkEnd = endUTC
		}
		part, err := fetchOpenF1ChunkAdaptive[T](ctx, p, endpoint, sessionKey, chunkStart, chunkEnd, minChunkDur)
		if err != nil {
			return nil, fmt.Errorf("openf1 %s chunk %s..%s failed: %w", endpoint, chunkStart.UTC().Format(time.RFC3339), chunkEnd.UTC().Format(time.RFC3339), err)
		}
		out = append(out, part...)
	}
	return out, nil
}

func fetchOpenF1ChunkAdaptive[T any](ctx context.Context, p *GoSessionProcessor, endpoint string, sessionKey int, startUTC, endUTC time.Time, minChunkDur time.Duration) ([]T, error) {
	params := map[string]string{
		"session_key": strconv.Itoa(sessionKey),
		"date>=":      startUTC.UTC().Format(time.RFC3339),
		"date<":       endUTC.UTC().Format(time.RFC3339),
	}
	part := make([]T, 0, 1024)
	err := p.fetchOpenF1JSON(ctx, endpoint, params, &part)
	if err == nil {
		return part, nil
	}

	var httpErr openF1HTTPError
	if errors.As(err, &httpErr) && httpErr.Status >= 500 && httpErr.Status <= 599 && endUTC.Sub(startUTC) > minChunkDur {
		mid := startUTC.Add(endUTC.Sub(startUTC) / 2)
		if !mid.After(startUTC) || !endUTC.After(mid) {
			return nil, err
		}
		log.Printf("processor(openf1): splitting chunk endpoint=%s window=%s..%s due to status=%d", endpoint, startUTC.UTC().Format(time.RFC3339), endUTC.UTC().Format(time.RFC3339), httpErr.Status)
		left, leftErr := fetchOpenF1ChunkAdaptive[T](ctx, p, endpoint, sessionKey, startUTC, mid, minChunkDur)
		if leftErr != nil {
			return nil, leftErr
		}
		right, rightErr := fetchOpenF1ChunkAdaptive[T](ctx, p, endpoint, sessionKey, mid, endUTC, minChunkDur)
		if rightErr != nil {
			return nil, rightErr
		}
		return append(left, right...), nil
	}
	return nil, err
}

func (p *GoSessionProcessor) waitOpenF1Turn(ctx context.Context) error {
	minInterval := p.openF1MinInterval
	if minInterval <= 0 {
		minInterval = 450 * time.Millisecond
	}
	for {
		p.openF1ReqMu.Lock()
		wait := minInterval - time.Since(p.openF1LastReqAt)
		if wait <= 0 {
			p.openF1LastReqAt = time.Now()
			p.openF1ReqMu.Unlock()
			return nil
		}
		p.openF1ReqMu.Unlock()
		if err := p.sleepWithContext(ctx, wait); err != nil {
			return err
		}
	}
}

func (p *GoSessionProcessor) openF1RetryDelay(attempt int, retryAfter time.Duration) time.Duration {
	if retryAfter > 0 {
		return retryAfter
	}
	delay := time.Duration(attempt) * time.Second
	if delay > 15*time.Second {
		delay = 15 * time.Second
	}
	return delay
}

func (p *GoSessionProcessor) sleepWithContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func readResponseBody(resp *http.Response) ([]byte, error) {
	if resp == nil {
		return nil, errors.New("empty http response")
	}
	defer resp.Body.Close()
	return io.ReadAll(resp.Body)
}

func parseRetryAfter(raw string) time.Duration {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0
	}
	if sec, err := strconv.Atoi(raw); err == nil && sec > 0 {
		return time.Duration(sec) * time.Second
	}
	if ts, err := time.Parse(time.RFC1123, raw); err == nil {
		d := time.Until(ts)
		if d > 0 {
			return d
		}
	}
	if ts, err := time.Parse(time.RFC1123Z, raw); err == nil {
		d := time.Until(ts)
		if d > 0 {
			return d
		}
	}
	return 0
}
