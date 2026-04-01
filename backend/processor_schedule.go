package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
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
