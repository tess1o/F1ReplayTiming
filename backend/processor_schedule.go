package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

func (p *GoSessionProcessor) EnsureSchedule(ctx context.Context, year int) error {
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	idx, err := p.fetchSeasonIndex(ctx, year)
	if err != nil {
		return err
	}
	events := p.buildScheduleEvents(idx)
	events = p.mergeEditorialSchedule(ctx, year, events)
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
	p.recomputeScheduleStatuses(out, now)
	return out
}

type editorialEventListing struct {
	Events []editorialEvent `json:"events"`
}

type editorialEvent struct {
	MeetingKey         string `json:"meetingKey"`
	MeetingName        string `json:"meetingName"`
	MeetingOfficial    string `json:"meetingOfficialName"`
	MeetingLocation    string `json:"meetingLocation"`
	MeetingCountryName string `json:"meetingCountryName"`
	RoundText          string `json:"roundText"`
	IsTestEvent        bool   `json:"isTestEvent"`
}

type editorialRaceEnvelope struct {
	Race editorialRace `json:"race"`
}

type editorialRace struct {
	MeetingNumber     string                    `json:"meetingNumber"`
	MeetingName       string                    `json:"meetingName"`
	MeetingOfficial   string                    `json:"meetingOfficialName"`
	MeetingLocation   string                    `json:"meetingLocation"`
	MeetingCountryISO string                    `json:"meetingIsoCountryName"`
	MeetingSessions   []editorialMeetingSession `json:"meetingSessions"`
}

type editorialMeetingSession struct {
	Description string `json:"description"`
	StartTime   string `json:"startTime"`
	GmtOffset   string `json:"gmtOffset"`
}

func (p *GoSessionProcessor) mergeEditorialSchedule(ctx context.Context, year int, events []map[string]any) []map[string]any {
	if strings.TrimSpace(p.editorialAPIKey) == "" {
		return events
	}
	if strings.TrimSpace(p.editorialBaseURL) == "" {
		return events
	}

	listing, err := p.fetchEditorialEventListing(ctx, year)
	if err != nil {
		log.Printf("processor(go): editorial schedule fallback unavailable: %v", err)
		return events
	}

	existingRounds := make(map[int]struct{}, len(events))
	for _, evt := range events {
		rnd := asInt(evt["round_number"])
		if rnd > 0 {
			existingRounds[rnd] = struct{}{}
		}
	}

	out := append([]map[string]any{}, events...)
	now := time.Now().UTC()
	for _, item := range listing.Events {
		if item.IsTestEvent || isTestingMeeting(item.MeetingName, item.MeetingOfficial) {
			continue
		}
		round := parseRoundText(item.RoundText)
		if round <= 0 {
			continue
		}
		if _, ok := existingRounds[round]; ok {
			continue
		}

		meetingID, err := strconv.Atoi(strings.TrimSpace(item.MeetingKey))
		if err != nil || meetingID <= 0 {
			continue
		}

		evt, err := p.fetchEditorialRaceEvent(ctx, meetingID)
		if err != nil {
			log.Printf("processor(go): failed to fetch editorial race details for round %d (meeting=%d): %v", round, meetingID, err)
			continue
		}
		if editorialFallbackEventEnded(evt, now) {
			continue
		}

		if asInt(evt["round_number"]) <= 0 {
			evt["round_number"] = round
		}
		out = append(out, evt)
		existingRounds[round] = struct{}{}
	}

	sort.Slice(out, func(i, j int) bool {
		return asInt(out[i]["round_number"]) < asInt(out[j]["round_number"])
	})
	p.recomputeScheduleStatuses(out, now)
	return out
}

func (p *GoSessionProcessor) fetchEditorialEventListing(ctx context.Context, year int) (*editorialEventListing, error) {
	q := url.Values{}
	q.Set("season", strconv.Itoa(year))
	endpoint := fmt.Sprintf("%s/v1/editorial-eventlisting/events?%s", p.editorialBaseURL, q.Encode())
	headers := map[string]string{
		"apikey": p.editorialAPIKey,
		"locale": defaultString(p.editorialLocale, defaultF1EditorialLocale),
	}
	var out editorialEventListing
	if err := p.fetchJSONWithHeaders(ctx, endpoint, headers, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (p *GoSessionProcessor) fetchEditorialRaceEvent(ctx context.Context, meetingID int) (map[string]any, error) {
	q := url.Values{}
	q.Set("meeting", strconv.Itoa(meetingID))
	endpoint := fmt.Sprintf("%s/v1/editorial-assemblies/races?%s", p.editorialBaseURL, q.Encode())
	headers := map[string]string{
		"apikey": p.editorialAPIKey,
		"locale": defaultString(p.editorialLocale, defaultF1EditorialLocale),
	}
	var envelope editorialRaceEnvelope
	if err := p.fetchJSONWithHeaders(ctx, endpoint, headers, &envelope); err != nil {
		return nil, err
	}

	round := asInt(envelope.Race.MeetingNumber)
	sessions := make([]map[string]any, 0, len(envelope.Race.MeetingSessions))
	for _, s := range envelope.Race.MeetingSessions {
		name := normalizeSessionName(s.Description)
		if name == "" {
			continue
		}
		startUTC, ok := sessionStartUTC(strings.TrimSpace(s.StartTime), strings.TrimSpace(s.GmtOffset))
		dateUTC := ""
		available := false
		if ok {
			dateUTC = startUTC.Format(time.RFC3339)
			available = time.Now().UTC().After(startUTC.Add(2 * time.Hour))
		}
		sessions = append(sessions, map[string]any{
			"name":         name,
			"session_type": normalizeSessionType(sessionNameToType[name]),
			"date_utc":     dateUTC,
			"available":    available,
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

	return map[string]any{
		"round_number": round,
		"country":      defaultString(envelope.Race.MeetingCountryISO, itemFallbackCountry(envelope.Race.MeetingLocation)),
		"event_name":   envelope.Race.MeetingName,
		"location":     envelope.Race.MeetingLocation,
		"event_date":   eventDate,
		"sessions":     sessions,
		"status":       "future",
	}, nil
}

func editorialFallbackEventEnded(evt map[string]any, now time.Time) bool {
	sessions := sessionMapsFromEvent(evt)
	if len(sessions) == 0 {
		return false
	}
	last := sessions[len(sessions)-1]
	dateStr := asString(last["date_utc"])
	ts, ok := parseDateMaybe(dateStr)
	if !ok {
		return false
	}
	return now.After(ts.Add(6 * time.Hour))
}

func parseRoundText(raw string) int {
	raw = strings.TrimSpace(strings.ToUpper(raw))
	raw = strings.TrimPrefix(raw, "ROUND")
	raw = strings.TrimSpace(strings.TrimPrefix(raw, "-"))
	if raw == "" {
		return 0
	}
	n, _ := strconv.Atoi(strings.Fields(raw)[0])
	return n
}

func (p *GoSessionProcessor) recomputeScheduleStatuses(events []map[string]any, now time.Time) {
	latestIdx := -1
	for i := range events {
		hasAnyAvailable := false
		for _, s := range sessionMapsFromEvent(events[i]) {
			dateStr := asString(s["date_utc"])
			available := false
			if ts, ok := parseDateMaybe(dateStr); ok {
				available = now.After(ts.Add(2 * time.Hour))
			}
			s["available"] = available
			if available {
				hasAnyAvailable = true
			}
		}
		if hasAnyAvailable {
			events[i]["status"] = "available"
			latestIdx = i
		} else {
			events[i]["status"] = "future"
		}
	}
	if latestIdx >= 0 {
		events[latestIdx]["status"] = "latest"
	}
}

func sessionMapsFromEvent(evt map[string]any) []map[string]any {
	if sess, ok := evt["sessions"].([]map[string]any); ok {
		return sess
	}
	raw, ok := evt["sessions"].([]any)
	if !ok {
		return nil
	}
	out := make([]map[string]any, 0, len(raw))
	for _, sAny := range raw {
		s, ok := sAny.(map[string]any)
		if ok {
			out = append(out, s)
		}
	}
	return out
}

func itemFallbackCountry(location string) string {
	if strings.TrimSpace(location) == "" {
		return "Unknown"
	}
	return location
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
