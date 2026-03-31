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
	"net/http/cookiejar"
	"net/url"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	liveSignalRBaseURL  = "https://livetiming.formula1.com/signalrcore"
	liveSignalRWSURL    = "wss://livetiming.formula1.com/signalrcore"
	liveSignalRRecordRS = '\x1e'
)

var liveSignalRTopics = []string{
	"TimingData",
	"TimingAppData",
	"TimingStats",
	"DriverList",
	"RaceControlMessages",
	"TrackStatus",
	"WeatherData",
	"LapCount",
	"ExtrapolatedClock",
	"SessionInfo",
	"SessionStatus",
	"SessionData",
	"Position.z",
}

type liveSignalRClient struct {
	httpClient *http.Client
	dialer     websocket.Dialer
}

func newLiveSignalRClient() *liveSignalRClient {
	jar, _ := cookiejar.New(nil)
	return &liveSignalRClient{
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Jar:     jar,
		},
		dialer: websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 15 * time.Second,
		},
	}
}

func (c *liveSignalRClient) stream(ctx context.Context, onMessage func(topic string, data map[string]any, ts float64)) error {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := c.runSession(ctx, onMessage)
		if err == nil || errors.Is(err, io.EOF) {
			backoff = time.Second
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		wait := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			wait.Stop()
			return nil
		case <-wait.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (c *liveSignalRClient) runSession(ctx context.Context, onMessage func(topic string, data map[string]any, ts float64)) error {
	token, cookie, err := c.negotiate(ctx)
	if err != nil {
		return err
	}

	wsURL := liveSignalRWSURL + "?id=" + url.QueryEscape(token)
	header := http.Header{}
	if cookie != "" {
		header.Set("Cookie", cookie)
	}
	ws, _, err := c.dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return err
	}
	defer ws.Close()
	closeOnCancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ws.Close()
		case <-closeOnCancelDone:
		}
	}()
	defer close(closeOnCancelDone)

	if err := c.writeSignalR(ws, map[string]any{"protocol": "json", "version": 1}); err != nil {
		return err
	}
	_, hsRaw, err := ws.ReadMessage()
	if err != nil {
		return err
	}
	if err := validateSignalRHandshake(hsRaw); err != nil {
		return err
	}
	if err := c.writeSignalR(ws, map[string]any{
		"type":         1,
		"invocationId": "1",
		"target":       "Subscribe",
		"arguments":    []any{liveSignalRTopics},
	}); err != nil {
		return err
	}

	buf := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, payload, err := ws.ReadMessage()
		if err != nil {
			var netErr interface{ Timeout() bool }
			if errors.As(err, &netErr) && netErr.Timeout() {
				_ = c.writeSignalR(ws, map[string]any{"type": 6})
				continue
			}
			return err
		}
		recvTS := float64(time.Now().UnixNano()) / 1e9
		buf += string(payload)
		for {
			idx := strings.IndexByte(buf, liveSignalRRecordRS)
			if idx < 0 {
				break
			}
			msgRaw := strings.TrimSpace(buf[:idx])
			buf = buf[idx+1:]
			if msgRaw == "" {
				continue
			}
			var msg map[string]any
			if err := json.Unmarshal([]byte(msgRaw), &msg); err != nil {
				continue
			}
			if err := c.handleSignalRMessage(ctx, ws, msg, recvTS, onMessage); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}
}

func (c *liveSignalRClient) negotiate(ctx context.Context) (connectionToken string, cookie string, err error) {
	negotiateURL := liveSignalRBaseURL + "/negotiate?negotiateVersion=1"

	reqOptions, _ := http.NewRequestWithContext(ctx, http.MethodOptions, negotiateURL, nil)
	reqOptions.Header.Set("Accept", "*/*")
	_, _ = c.httpClient.Do(reqOptions)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, negotiateURL, strings.NewReader(""))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", "", fmt.Errorf("signalr negotiate failed: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		ConnectionToken string `json:"connectionToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", err
	}
	if strings.TrimSpace(out.ConnectionToken) == "" {
		return "", "", errors.New("signalr negotiate returned empty connection token")
	}

	baseURL, _ := url.Parse(liveSignalRBaseURL)
	if c.httpClient.Jar != nil && baseURL != nil {
		for _, ck := range c.httpClient.Jar.Cookies(baseURL) {
			if ck.Name == "AWSALBCORS" {
				cookie = ck.Name + "=" + ck.Value
				break
			}
		}
	}
	return out.ConnectionToken, cookie, nil
}

func validateSignalRHandshake(raw []byte) error {
	parts := strings.Split(string(raw), string(liveSignalRRecordRS))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var hs map[string]any
		if err := json.Unmarshal([]byte(p), &hs); err != nil {
			return err
		}
		if errMsg := strings.TrimSpace(asString(hs["error"])); errMsg != "" {
			return fmt.Errorf("signalr handshake error: %s", errMsg)
		}
	}
	return nil
}

func (c *liveSignalRClient) writeSignalR(ws *websocket.Conn, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	raw = append(raw, byte(liveSignalRRecordRS))
	return ws.WriteMessage(websocket.TextMessage, raw)
}

func (c *liveSignalRClient) handleSignalRMessage(
	ctx context.Context,
	ws *websocket.Conn,
	msg map[string]any,
	recvTS float64,
	onMessage func(topic string, data map[string]any, ts float64),
) error {
	_ = ctx
	msgType := asInt(msg["type"])
	switch msgType {
	case 6: // ping
		_ = c.writeSignalR(ws, map[string]any{"type": 6})
		return nil
	case 7: // close
		return io.EOF
	case 1: // invocation
		target := asString(msg["target"])
		args, _ := msg["arguments"].([]any)
		if target == "" {
			return nil
		}
		if target == "feed" {
			if len(args) >= 2 {
				if topic, ok := args[0].(string); ok {
					c.dispatchLiveTopic(topic, args[1], recvTS, onMessage)
					return nil
				}
			}
			if len(args) >= 1 {
				if root, ok := args[0].(map[string]any); ok {
					for topic, payload := range root {
						c.dispatchLiveTopic(topic, payload, recvTS, onMessage)
					}
				}
			}
			return nil
		}
		if len(args) == 0 {
			return nil
		}
		c.dispatchLiveTopic(target, args[0], recvTS, onMessage)
		return nil
	case 2: // stream item (can include Position.z)
		item := msg["item"]
		switch v := item.(type) {
		case map[string]any:
			onMessage("Position", v, recvTS)
		case string:
			dec, err := decodeZPayload(v)
			if err != nil {
				return nil
			}
			var out map[string]any
			if err := json.Unmarshal(dec, &out); err == nil {
				onMessage("Position", out, recvTS)
			}
		}
		return nil
	case 3: // completion (initial snapshot)
		result, _ := msg["result"].(map[string]any)
		for topic, payload := range result {
			c.dispatchLiveTopic(topic, payload, recvTS, onMessage)
		}
		return nil
	default:
		return nil
	}
}

func (c *liveSignalRClient) dispatchLiveTopic(topic string, payload any, ts float64, onMessage func(topic string, data map[string]any, ts float64)) {
	decodedTopic, decodedData, ok := decodeLiveTopicPayload(topic, payload)
	if !ok {
		return
	}
	onMessage(decodedTopic, decodedData, ts)
}

func decodeLiveTopicPayload(topic string, payload any) (string, map[string]any, bool) {
	effectiveTopic := topic
	switch v := payload.(type) {
	case map[string]any:
		return stripTopicZ(effectiveTopic), v, true
	case string:
		if strings.HasSuffix(effectiveTopic, ".z") {
			dec, err := decodeZPayload(v)
			if err != nil {
				return "", nil, false
			}
			var out map[string]any
			if err := json.Unmarshal(dec, &out); err != nil {
				return "", nil, false
			}
			return stripTopicZ(effectiveTopic), out, true
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(v), &out); err != nil {
			return "", nil, false
		}
		return stripTopicZ(effectiveTopic), out, true
	default:
		return "", nil, false
	}
}

func stripTopicZ(topic string) string {
	if strings.HasSuffix(topic, ".z") {
		return strings.TrimSuffix(topic, ".z")
	}
	return topic
}

type liveTrackNorm struct {
	XMin  float64
	YMin  float64
	Scale float64
}

type liveTrackPoint struct {
	X float64
	Y float64
}

type liveDriverState struct {
	RacingNumber string
	Abbr         string
	Team         string
	Color        string

	Position int
	Gap      string
	Interval string

	Compound    string
	TyreLife    int
	TyreHistory []string
	lastStint   int
	PitStops    int
	InPit       bool

	HasFastestLap bool
	Flag          string
	Retired       bool
	NoTiming      bool
	GridPosition  int
	Sectors       []map[string]any
	BestLapTime   string
	PitStart      bool

	X                float64
	Y                float64
	RelativeDistance float64
	OnTrack          bool

	sectorBestPersonal map[int]float64
	sectorTimes        map[int]float64
	s3CompletedAt      time.Time
}

func newLiveDriverState(number string) *liveDriverState {
	return &liveDriverState{
		RacingNumber:       number,
		Color:              "#FFFFFF",
		lastStint:          -1,
		sectorBestPersonal: make(map[int]float64),
		sectorTimes:        make(map[int]float64),
	}
}

type liveStateManager struct {
	mu sync.RWMutex

	sessionType string
	pitLoss     pitLossValues
	trackNorm   *liveTrackNorm
	trackPoints []liveTrackPoint

	drivers map[string]*liveDriverState

	status            string
	weather           map[string]any
	currentLap        int
	totalLaps         int
	sessionStatus     string
	sessionWasStarted bool
	qualiPart         int

	clockRemaining    float64
	clockExtrapolates bool
	clockUpdatedAt    time.Time

	lastTimestamp float64
	rcMessages    []map[string]any

	overallSectorBest map[int]float64

	rawXMin         float64
	rawXMax         float64
	rawYMin         float64
	rawYMax         float64
	positionSamples int
}

func newLiveStateManager(sessionType string, pit pitLossValues, trackNorm *liveTrackNorm, trackPoints []liveTrackPoint) *liveStateManager {
	return &liveStateManager{
		sessionType:       strings.ToUpper(strings.TrimSpace(sessionType)),
		pitLoss:           pit,
		trackNorm:         trackNorm,
		trackPoints:       trackPoints,
		drivers:           make(map[string]*liveDriverState),
		status:            "green",
		sessionStatus:     "Inactive",
		overallSectorBest: make(map[int]float64),
		rawXMin:           math.Inf(1),
		rawXMax:           math.Inf(-1),
		rawYMin:           math.Inf(1),
		rawYMax:           math.Inf(-1),
	}
}

func (m *liveStateManager) Process(topic string, data map[string]any, ts float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastTimestamp = ts
	switch topic {
	case "DriverList":
		m.handleDriverList(data)
	case "TimingData":
		m.handleTimingData(data)
	case "TimingAppData":
		m.handleTimingAppData(data)
	case "TimingStats":
		m.handleTimingStats(data)
	case "RaceControlMessages":
		m.handleRaceControl(data, ts)
	case "TrackStatus":
		m.handleTrackStatus(data)
	case "WeatherData":
		m.handleWeather(data)
	case "LapCount":
		m.handleLapCount(data)
	case "ExtrapolatedClock":
		m.handleClock(data)
	case "SessionStatus":
		m.handleSessionStatus(data)
	case "SessionData":
		m.handleSessionData(data)
	case "Position":
		m.handlePosition(data)
	}
}

func (m *liveStateManager) getDriver(number string) *liveDriverState {
	d := m.drivers[number]
	if d == nil {
		d = newLiveDriverState(number)
		m.drivers[number] = d
	}
	return d
}

func (m *liveStateManager) handleDriverList(data map[string]any) {
	for number, raw := range data {
		info, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		d := m.getDriver(number)
		if abbr := strings.TrimSpace(asString(info["Tla"])); abbr != "" {
			d.Abbr = abbr
		}
		if team := strings.TrimSpace(asString(info["TeamName"])); team != "" {
			d.Team = team
		}
		if color := strings.TrimSpace(asString(info["TeamColour"])); color != "" {
			if !strings.HasPrefix(color, "#") {
				color = "#" + color
			}
			d.Color = color
		}
	}
}

func (m *liveStateManager) handleTimingData(data map[string]any) {
	lines, _ := data["Lines"].(map[string]any)
	if len(lines) == 0 {
		return
	}
	for number, raw := range lines {
		updates, ok := raw.(map[string]any)
		if !ok {
			continue
		}
		d := m.getDriver(number)

		if pos := asInt(updates["Position"]); pos > 0 {
			d.Position = pos
		}
		if gap, exists := updates["GapToLeader"]; exists {
			switch g := gap.(type) {
			case string:
				if strings.TrimSpace(g) != "" || d.Position == 1 {
					d.Gap = g
				}
			case map[string]any:
				v := asString(g["Value"])
				if strings.TrimSpace(v) != "" || d.Position == 1 {
					d.Gap = v
				}
			}
		}
		if intervalRaw, exists := updates["IntervalToPositionAhead"]; exists {
			switch iv := intervalRaw.(type) {
			case string:
				if strings.TrimSpace(iv) != "" {
					d.Interval = iv
				}
			case map[string]any:
				v := asString(iv["Value"])
				if strings.TrimSpace(v) != "" {
					d.Interval = v
				}
			}
		}
		if blt, exists := updates["BestLapTime"]; exists {
			switch v := blt.(type) {
			case string:
				if strings.TrimSpace(v) != "" {
					d.BestLapTime = strings.TrimSpace(v)
				}
			case map[string]any:
				val := strings.TrimSpace(asString(v["Value"]))
				if val != "" {
					d.BestLapTime = val
				}
			}
		}
		if inPit, ok := updates["InPit"].(bool); ok {
			d.InPit = inPit
		}
		if retired, ok := updates["Retired"].(bool); ok && retired {
			d.Retired = true
		}
		if knockedOut, ok := updates["KnockedOut"].(bool); ok && knockedOut {
			d.Retired = true
		}
		if sectorsRaw, exists := updates["Sectors"]; exists {
			m.processSectors(d, sectorsRaw)
		}

		if statusRaw, exists := updates["Status"]; exists {
			switch status := statusRaw.(type) {
			case string:
				d.NoTiming = strings.TrimSpace(status) == ""
			case map[string]any:
				d.NoTiming = len(status) == 0
			}
		}
		if d.Position == 0 && strings.TrimSpace(d.Gap) == "" {
			d.NoTiming = true
		} else {
			d.NoTiming = false
		}
	}
}

func (m *liveStateManager) processSectors(d *liveDriverState, sectorsRaw any) {
	sectors := make(map[int]map[string]any)
	if len(d.Sectors) > 0 {
		for _, s := range d.Sectors {
			num := asInt(s["num"])
			if num > 0 {
				sectors[num] = map[string]any{"num": num, "color": asString(s["color"])}
			}
		}
	}

	in := make(map[string]any)
	switch v := sectorsRaw.(type) {
	case []any:
		for i, item := range v {
			if m, ok := item.(map[string]any); ok {
				in[strconv.Itoa(i)] = m
			}
		}
	case map[string]any:
		in = v
	default:
		return
	}

	keys := make([]int, 0, len(in))
	for k := range in {
		if idx, err := strconv.Atoi(k); err == nil {
			keys = append(keys, idx)
		}
	}
	sort.Ints(keys)
	for _, idx := range keys {
		payload, _ := in[strconv.Itoa(idx)].(map[string]any)
		if len(payload) == 0 {
			continue
		}
		valRaw := strings.TrimSpace(asString(payload["Value"]))
		if valRaw == "" {
			continue
		}
		secTime, err := strconv.ParseFloat(valRaw, 64)
		if err != nil {
			continue
		}
		secIdx := idx
		secNum := secIdx + 1

		d.sectorTimes[secIdx] = secTime
		pb, hasPB := d.sectorBestPersonal[secIdx]
		isPB := !hasPB || secTime <= pb+0.0005
		if !hasPB || secTime < pb {
			d.sectorBestPersonal[secIdx] = secTime
		}

		ob, hasOB := m.overallSectorBest[secIdx]
		isOB := !hasOB || secTime <= ob+0.0005
		if !hasOB || secTime < ob {
			m.overallSectorBest[secIdx] = secTime
			m.recomputeSectorColors(secIdx, d.RacingNumber)
		}

		color := "yellow"
		if isOB {
			color = "purple"
		} else if isPB {
			color = "green"
		}
		sectors[secNum] = map[string]any{"num": secNum, "color": color}

		for n := range sectors {
			if n > secNum {
				delete(sectors, n)
				delete(d.sectorTimes, n-1)
			}
		}
		if secNum == 3 {
			d.s3CompletedAt = time.Now()
		} else if secNum == 1 {
			d.s3CompletedAt = time.Time{}
		}
	}

	if len(sectors) == 0 {
		d.Sectors = nil
		return
	}
	ordered := make([]int, 0, len(sectors))
	for n := range sectors {
		ordered = append(ordered, n)
	}
	sort.Ints(ordered)
	out := make([]map[string]any, 0, len(ordered))
	for _, n := range ordered {
		out = append(out, sectors[n])
	}
	d.Sectors = out
}

func (m *liveStateManager) recomputeSectorColors(sectorIdx int, excludedNumber string) {
	sectorNum := sectorIdx + 1
	for num, d := range m.drivers {
		if num == excludedNumber || len(d.Sectors) == 0 {
			continue
		}
		for _, s := range d.Sectors {
			if asInt(s["num"]) != sectorNum || asString(s["color"]) != "purple" {
				continue
			}
			drvTime, okTime := d.sectorTimes[sectorIdx]
			drvPB, okPB := d.sectorBestPersonal[sectorIdx]
			if okTime && okPB && drvTime <= drvPB+0.0005 {
				s["color"] = "green"
			} else {
				s["color"] = "yellow"
			}
		}
	}
}

func (m *liveStateManager) handlePosition(data map[string]any) {
	positionList, _ := data["Position"].([]any)
	if len(positionList) == 0 {
		return
	}
	type sample struct {
		number string
		x      float64
		y      float64
		status string
	}
	batch := make([]sample, 0, 32)
	for _, item := range positionList {
		sampleObj, _ := item.(map[string]any)
		entries, _ := sampleObj["Entries"].(map[string]any)
		for number, entryRaw := range entries {
			entry, _ := entryRaw.(map[string]any)
			x, okX := floatFromAny(entry["X"])
			y, okY := floatFromAny(entry["Y"])
			if !okX || !okY {
				continue
			}
			batch = append(batch, sample{
				number: number,
				x:      x,
				y:      y,
				status: asString(entry["Status"]),
			})
		}
	}
	if len(batch) == 0 {
		return
	}

	var xMin, yMin, scale float64
	if m.trackNorm == nil {
		for _, s := range batch {
			if s.x < m.rawXMin {
				m.rawXMin = s.x
			}
			if s.x > m.rawXMax {
				m.rawXMax = s.x
			}
			if s.y < m.rawYMin {
				m.rawYMin = s.y
			}
			if s.y > m.rawYMax {
				m.rawYMax = s.y
			}
			m.positionSamples++
		}
		xRange := m.rawXMax - m.rawXMin
		yRange := m.rawYMax - m.rawYMin
		scale = math.Max(xRange, yRange)
		if scale < 1 || m.positionSamples < 5 {
			return
		}
		padding := scale * 0.05
		xMin = m.rawXMin - padding
		yMin = m.rawYMin - padding
		scale += 2 * padding
	} else {
		xMin = m.trackNorm.XMin
		yMin = m.trackNorm.YMin
		scale = m.trackNorm.Scale
	}
	if scale <= 0 {
		return
	}

	for _, s := range batch {
		d := m.getDriver(s.number)
		d.OnTrack = strings.EqualFold(strings.TrimSpace(s.status), "OnTrack")
		nx := (s.x - xMin) / scale
		ny := (s.y - yMin) / scale
		if len(m.trackPoints) == 0 {
			d.X = nx
			d.Y = ny
			d.RelativeDistance = 0
			continue
		}
		nearestIdx := 0
		bestDist := math.Inf(1)
		for i := range m.trackPoints {
			dx := m.trackPoints[i].X - nx
			dy := m.trackPoints[i].Y - ny
			dist := dx*dx + dy*dy
			if dist < bestDist {
				bestDist = dist
				nearestIdx = i
			}
		}
		d.X = m.trackPoints[nearestIdx].X
		d.Y = m.trackPoints[nearestIdx].Y
		d.RelativeDistance = float64(nearestIdx) / float64(len(m.trackPoints))
	}
}

func (m *liveStateManager) handleTimingAppData(data map[string]any) {
	lines, _ := data["Lines"].(map[string]any)
	if len(lines) == 0 {
		return
	}
	for number, raw := range lines {
		updates, _ := raw.(map[string]any)
		if len(updates) == 0 {
			continue
		}
		d := m.getDriver(number)
		if gp := asInt(updates["GridPos"]); gp > 0 {
			d.GridPosition = gp
		}
		if stintsRaw, exists := updates["Stints"]; exists {
			m.processStints(d, stintsRaw)
		}
	}
}

func (m *liveStateManager) processStints(d *liveDriverState, stintsRaw any) {
	stints := make(map[string]any)
	switch v := stintsRaw.(type) {
	case []any:
		for i, item := range v {
			if stint, ok := item.(map[string]any); ok {
				stints[strconv.Itoa(i)] = stint
			}
		}
	case map[string]any:
		stints = v
	default:
		return
	}
	maxIdx := -1
	var latest map[string]any
	for idxStr, raw := range stints {
		idx, err := strconv.Atoi(idxStr)
		if err != nil {
			continue
		}
		stint, _ := raw.(map[string]any)
		if stint == nil {
			continue
		}
		if idx > maxIdx {
			maxIdx = idx
			latest = stint
		}
	}
	if latest == nil {
		return
	}

	if compound := strings.ToUpper(strings.TrimSpace(asString(latest["Compound"]))); compound != "" && compound != "UNKNOWN" {
		if maxIdx > d.lastStint && d.lastStint >= 0 && d.Compound != "" {
			if len(d.TyreHistory) == 0 || d.TyreHistory[len(d.TyreHistory)-1] != d.Compound {
				d.TyreHistory = append(d.TyreHistory, d.Compound)
			}
		}
		d.Compound = compound
		d.lastStint = maxIdx
	}
	if tyreLife := asInt(latest["TotalLaps"]); tyreLife >= 0 {
		d.TyreLife = tyreLife
	}
	if maxIdx >= 0 {
		d.PitStops = maxIdx
	}
}

func (m *liveStateManager) handleTimingStats(data map[string]any) {
	lines, _ := data["Lines"].(map[string]any)
	if len(lines) == 0 {
		return
	}
	fastestNumber := ""
	for number, raw := range lines {
		stats, _ := raw.(map[string]any)
		pb, _ := stats["PersonalBestLapTime"].(map[string]any)
		if asInt(pb["Position"]) == 1 {
			fastestNumber = number
			break
		}
	}
	if fastestNumber == "" {
		return
	}
	for num, d := range m.drivers {
		d.HasFastestLap = num == fastestNumber
	}
}

func (m *liveStateManager) handleRaceControl(data map[string]any, ts float64) {
	msgsRaw := data["Messages"]
	if msgsRaw == nil {
		return
	}
	iter := make([]map[string]any, 0, 8)
	switch v := msgsRaw.(type) {
	case []any:
		for _, item := range v {
			if msg, ok := item.(map[string]any); ok {
				iter = append(iter, msg)
			}
		}
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if msg, ok := v[k].(map[string]any); ok {
				iter = append(iter, msg)
			}
		}
	}
	for _, msg := range iter {
		message := strings.TrimSpace(asString(msg["Message"]))
		category := strings.TrimSpace(asString(msg["Category"]))
		racingNumber := strings.TrimSpace(asString(msg["RacingNumber"]))
		lap := asInt(msg["Lap"])

		if message != "" {
			entry := map[string]any{
				"message":   message,
				"category":  category,
				"timestamp": ts,
			}
			if lap > 0 {
				entry["lap"] = lap
			}
			if racingNumber != "" {
				entry["racing_number"] = racingNumber
			}
			m.rcMessages = append(m.rcMessages, entry)
			if len(m.rcMessages) > 50 {
				m.rcMessages = m.rcMessages[len(m.rcMessages)-50:]
			}
		}

		if racingNumber == "" {
			racingNumber = extractCarNumberFromRC(message)
		}
		if racingNumber == "" {
			continue
		}
		d := m.getDriver(racingNumber)
		upper := strings.ToUpper(message)
		switch {
		case strings.Contains(upper, "NO FURTHER ACTION"), strings.Contains(upper, "NO INVESTIGATION"), strings.Contains(upper, "PENALTY SERVED"):
			d.Flag = ""
		case strings.Contains(upper, "DECISION") && !strings.Contains(upper, "PENALTY"):
			d.Flag = ""
		case strings.Contains(upper, "UNDER INVESTIGATION"), strings.Contains(upper, "IS NOTED"):
			d.Flag = "investigation"
		case strings.Contains(upper, "TIME PENALTY"), strings.Contains(upper, "PENALTY"):
			if !strings.Contains(upper, "NO FURTHER") {
				d.Flag = "penalty"
			}
		}
	}
}

func extractCarNumberFromRC(message string) string {
	upper := strings.ToUpper(message)
	idx := strings.Index(upper, "CAR ")
	if idx < 0 {
		return ""
	}
	s := upper[idx+4:]
	s = strings.TrimLeft(s, " ")
	end := 0
	for end < len(s) && s[end] >= '0' && s[end] <= '9' {
		end++
	}
	if end == 0 {
		return ""
	}
	return s[:end]
}

func (m *liveStateManager) handleTrackStatus(data map[string]any) {
	code := strings.TrimSpace(asString(data["Status"]))
	switch code {
	case "1":
		m.status = "green"
	case "2":
		m.status = "yellow"
	case "4":
		m.status = "sc"
	case "5":
		m.status = "red"
	case "6", "7":
		m.status = "vsc"
	}
}

func (m *liveStateManager) handleWeather(data map[string]any) {
	weather := map[string]any{
		"air_temp":       nil,
		"track_temp":     nil,
		"humidity":       nil,
		"rainfall":       false,
		"wind_speed":     nil,
		"wind_direction": nil,
	}
	if m.weather != nil {
		for k, v := range m.weather {
			weather[k] = v
		}
	}
	if v, ok := floatFromAny(data["AirTemp"]); ok {
		weather["air_temp"] = v
	}
	if v, ok := floatFromAny(data["TrackTemp"]); ok {
		weather["track_temp"] = v
	}
	if v, ok := floatFromAny(data["Humidity"]); ok {
		weather["humidity"] = v
	}
	if raw, ok := data["Rainfall"]; ok {
		weather["rainfall"] = strings.TrimSpace(asString(raw)) != "0"
	}
	if v, ok := floatFromAny(data["WindSpeed"]); ok {
		weather["wind_speed"] = v
	}
	if v, ok := floatFromAny(data["WindDirection"]); ok {
		weather["wind_direction"] = v
	}
	m.weather = weather
}

func (m *liveStateManager) handleLapCount(data map[string]any) {
	if v := asInt(data["CurrentLap"]); v >= 0 {
		m.currentLap = v
	}
	if v := asInt(data["TotalLaps"]); v >= 0 {
		m.totalLaps = v
	}
}

func (m *liveStateManager) handleClock(data map[string]any) {
	if remaining := strings.TrimSpace(asString(data["Remaining"])); remaining != "" {
		m.clockRemaining = parseRemainingSeconds(remaining)
		m.clockUpdatedAt = time.Now()
	}
	if extrapolating, ok := data["Extrapolating"].(bool); ok {
		m.clockExtrapolates = extrapolating
	}
}

func parseRemainingSeconds(raw string) float64 {
	parts := strings.Split(raw, ":")
	switch len(parts) {
	case 3:
		h, _ := strconv.Atoi(parts[0])
		m, _ := strconv.Atoi(parts[1])
		s, _ := strconv.ParseFloat(parts[2], 64)
		return float64(h*3600+m*60) + s
	case 2:
		m, _ := strconv.Atoi(parts[0])
		s, _ := strconv.ParseFloat(parts[1], 64)
		return float64(m*60) + s
	default:
		v, _ := strconv.ParseFloat(raw, 64)
		return v
	}
}

func (m *liveStateManager) handleSessionStatus(data map[string]any) {
	status := strings.ToLower(strings.TrimSpace(asString(data["Status"])))
	if status == "" {
		return
	}
	if status == "started" {
		m.sessionWasStarted = true
	}
	m.sessionStatus = status
}

func (m *liveStateManager) handleSessionData(data map[string]any) {
	seriesRaw := data["Series"]
	if seriesRaw == nil {
		return
	}
	entries := make([]map[string]any, 0, 4)
	switch v := seriesRaw.(type) {
	case []any:
		for _, item := range v {
			if e, ok := item.(map[string]any); ok {
				entries = append(entries, e)
			}
		}
	case map[string]any:
		keys := make([]string, 0, len(v))
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			if e, ok := v[k].(map[string]any); ok {
				entries = append(entries, e)
			}
		}
	}
	for i := len(entries) - 1; i >= 0; i-- {
		if qp := asInt(entries[i]["QualifyingPart"]); qp > 0 {
			m.qualiPart = qp
			return
		}
	}
}

func (m *liveStateManager) SessionEnded() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.sessionWasStarted && isTerminalSessionStatus(m.sessionStatus)
}

func (m *liveStateManager) Frame() map[string]any {
	m.mu.Lock()
	defer m.mu.Unlock()

	const sectorLinger = 5 * time.Second
	now := time.Now()

	drivers := make([]map[string]any, 0, len(m.drivers))
	for _, d := range m.drivers {
		if strings.TrimSpace(d.Abbr) == "" {
			continue
		}
		if !d.s3CompletedAt.IsZero() && now.Sub(d.s3CompletedAt) > sectorLinger {
			d.Sectors = nil
			d.s3CompletedAt = time.Time{}
			d.sectorTimes = make(map[int]float64)
		}
		item := map[string]any{
			"abbr":                    d.Abbr,
			"position":                zeroToNilInt(d.Position),
			"gap":                     emptyToNil(d.Gap),
			"interval":                emptyToNil(d.Interval),
			"color":                   d.Color,
			"team":                    d.Team,
			"compound":                emptyToNil(d.Compound),
			"tyre_life":               zeroToNilInt(d.TyreLife),
			"tyre_history":            latestTyreHistory(d.TyreHistory),
			"pit_stops":               d.PitStops,
			"in_pit":                  d.InPit,
			"has_fastest_lap":         d.HasFastestLap,
			"flag":                    emptyToNil(d.Flag),
			"retired":                 d.Retired,
			"no_timing":               d.NoTiming,
			"grid_position":           zeroToNilInt(d.GridPosition),
			"sectors":                 d.Sectors,
			"best_lap_time":           emptyToNil(d.BestLapTime),
			"pit_prediction":          nil,
			"pit_prediction_margin":   nil,
			"pit_prediction_free_air": nil,
			"pit_start":               d.PitStart,
			"x":                       sanitizeNumber(d.X),
			"y":                       sanitizeNumber(d.Y),
			"relative_distance":       sanitizeNumber(d.RelativeDistance),
			"speed":                   nil,
			"throttle":                nil,
			"brake":                   false,
			"gear":                    nil,
			"rpm":                     nil,
			"drs":                     nil,
		}
		drivers = append(drivers, item)
	}

	sort.Slice(drivers, func(i, j int) bool {
		pi := asInt(drivers[i]["position"])
		pj := asInt(drivers[j]["position"])
		if pi == 0 {
			pi = 9999
		}
		if pj == 0 {
			pj = 9999
		}
		return pi < pj
	})

	isRace := m.sessionType == "R" || m.sessionType == "S"
	if isRace && len(drivers) > 0 {
		for _, d := range drivers {
			if asInt(d["position"]) == 1 && m.currentLap > 0 {
				d["gap"] = fmt.Sprintf("LAP %d", m.currentLap)
				break
			}
		}
	} else if len(drivers) > 0 {
		applyPracticeGaps(drivers)
	}

	var qualiPhase any
	if (m.sessionType == "Q" || m.sessionType == "SQ") && m.qualiPart > 0 {
		remaining := m.clockRemaining
		if m.clockExtrapolates && !m.clockUpdatedAt.IsZero() {
			remaining = math.Max(0, remaining-time.Since(m.clockUpdatedAt).Seconds())
		}
		qualiPhase = map[string]any{
			"phase":     fmt.Sprintf("Q%d", m.qualiPart),
			"elapsed":   0,
			"remaining": roundTo(remaining, 1),
		}
	}

	rc := make([]map[string]any, 0, len(m.rcMessages))
	for i := len(m.rcMessages) - 1; i >= 0; i-- {
		rc = append(rc, m.rcMessages[i])
	}

	frame := map[string]any{
		"timestamp":    sanitizeNumber(m.lastTimestamp),
		"lap":          m.currentLap,
		"total_laps":   m.totalLaps,
		"session_type": m.sessionType,
		"status":       m.status,
		"weather":      m.weather,
		"quali_phase":  qualiPhase,
		"drivers":      drivers,
		"rc_messages":  rc,
	}
	if isRace && m.pitLoss.Green > 0 {
		addPitPredictions(frame, m.pitLoss)
	}
	return frame
}

func zeroToNilInt(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}

func emptyToNil(v string) any {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	return v
}

func latestTyreHistory(history []string) []string {
	if len(history) <= 2 {
		out := make([]string, len(history))
		copy(out, history)
		return out
	}
	return append([]string{}, history[len(history)-2:]...)
}

func sanitizeNumber(v float64) any {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return nil
	}
	return v
}

func applyPracticeGaps(drivers []map[string]any) {
	type timedDriver struct {
		index int
		secs  float64
	}
	timed := make([]timedDriver, 0, len(drivers))
	for i := range drivers {
		secs, ok := parseLapSeconds(asString(drivers[i]["best_lap_time"]))
		if ok {
			timed = append(timed, timedDriver{index: i, secs: secs})
		}
	}
	if len(timed) == 0 {
		return
	}
	sort.Slice(timed, func(i, j int) bool { return timed[i].secs < timed[j].secs })
	leader := timed[0].secs
	for rank, t := range timed {
		d := drivers[t.index]
		d["position"] = rank + 1
		if rank == 0 {
			d["gap"] = formatLapTime(t.secs)
		} else {
			d["gap"] = fmt.Sprintf("+%.3f", t.secs-leader)
		}
	}
	sort.Slice(drivers, func(i, j int) bool {
		pi := asInt(drivers[i]["position"])
		pj := asInt(drivers[j]["position"])
		if pi == 0 {
			pi = 9999
		}
		if pj == 0 {
			pj = 9999
		}
		return pi < pj
	})
}

func parseLapSeconds(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	if strings.Count(raw, ":") == 1 {
		parts := strings.Split(raw, ":")
		m, errM := strconv.Atoi(parts[0])
		s, errS := strconv.ParseFloat(parts[1], 64)
		if errM != nil || errS != nil {
			return 0, false
		}
		return float64(m*60) + s, true
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func formatLapTime(secs float64) string {
	m := int(secs) / 60
	s := secs - float64(m*60)
	return fmt.Sprintf("%d:%06.3f", m, s)
}

func floatFromAny(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	case int:
		return float64(x), true
	case int64:
		return float64(x), true
	case json.Number:
		f, err := x.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		return f, err == nil
	default:
		return 0, false
	}
}

func (a *app) runLiveWebSocket(ctx context.Context, conn *websocket.Conn, year, round int, sessionType string, speed float64) error {
	_ = speed
	pit := a.resolvePitLoss(year, round, sessionType)
	trackNorm, trackPoints := a.findLiveTrackData(year, round, sessionType)
	state := newLiveStateManager(sessionType, pit, trackNorm, trackPoints)

	runID := int64(0)
	if a.store != nil {
		if id, err := a.store.StartLiveStreamRun(ctx, year, round, sessionType); err != nil {
			log.Printf("live run start warning: %v", err)
		} else {
			runID = id
		}
	}

	finalStatus := "stale"
	finalError := ""
	defer func() {
		if a.store != nil && runID > 0 {
			if err := a.store.FinishLiveStreamRun(context.Background(), runID, finalStatus, finalError); err != nil {
				log.Printf("live run finalize warning: %v", err)
			}
		}
	}()

	if err := conn.WriteJSON(map[string]any{
		"type":         "ready",
		"mode":         "live",
		"total_frames": 0,
		"total_time":   0,
		"total_laps":   0,
		"quali_phases": nil,
	}); err != nil {
		return err
	}

	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}()

	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	client := newLiveSignalRClient()
	streamErrCh := make(chan error, 1)
	go func() {
		streamErrCh <- client.stream(streamCtx, func(topic string, data map[string]any, ts float64) {
			state.Process(topic, data, ts)
		})
	}()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-readDone:
			return nil
		case err := <-streamErrCh:
			if err != nil {
				finalStatus = "failed"
				finalError = err.Error()
				return err
			}
			return nil
		case <-ticker.C:
			frame := state.Frame()
			payload := mapWithType("frame", frame)
			if err := conn.WriteJSON(payload); err != nil {
				return err
			}
			if state.SessionEnded() {
				finalStatus = "finished"
				_ = conn.WriteJSON(map[string]any{
					"type":    "finished",
					"message": "Session ended. Full replay with track positions and telemetry will be available shortly.",
				})
				return nil
			}
		}
	}
}

func mapWithType(typ string, payload map[string]any) map[string]any {
	out := make(map[string]any, len(payload)+1)
	out["type"] = typ
	for k, v := range payload {
		out[k] = v
	}
	return out
}

func (a *app) findLiveTrackData(year, round int, sessionType string) (*liveTrackNorm, []liveTrackPoint) {
	tryRead := func(y, r int, typ string) (*liveTrackNorm, []liveTrackPoint, bool) {
		raw, err := a.readJSONAny(filepath.Join("sessions", strconv.Itoa(y), strconv.Itoa(r), typ, "track.json"))
		if err != nil {
			return nil, nil, false
		}
		root, ok := raw.(map[string]any)
		if !ok {
			return nil, nil, false
		}
		normRaw, _ := root["norm"].(map[string]any)
		scale := asFloat(normRaw["scale"], 0)
		if scale <= 0 {
			return nil, nil, false
		}
		norm := &liveTrackNorm{
			XMin:  asFloat(normRaw["x_min"], 0),
			YMin:  asFloat(normRaw["y_min"], 0),
			Scale: scale,
		}
		pointsAny, _ := root["track_points"].([]any)
		points := make([]liveTrackPoint, 0, len(pointsAny))
		for _, item := range pointsAny {
			p, _ := item.(map[string]any)
			if p == nil {
				continue
			}
			points = append(points, liveTrackPoint{
				X: asFloat(p["x"], 0),
				Y: asFloat(p["y"], 0),
			})
		}
		return norm, points, true
	}

	if norm, points, ok := tryRead(year, round, sessionType); ok {
		return norm, points
	}
	for _, alt := range sessionTypePriority {
		if alt == sessionType {
			continue
		}
		if norm, points, ok := tryRead(year, round, alt); ok {
			return norm, points
		}
	}
	for prev := year - 1; prev >= year-3; prev-- {
		for _, alt := range []string{"R", "Q"} {
			if norm, points, ok := tryRead(prev, round, alt); ok {
				return norm, points
			}
		}
	}
	return nil, nil
}
