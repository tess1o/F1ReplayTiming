package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	mvCircuitAPIBase = "https://api.multiviewer.app/api/v1/circuits"
)

type staticDataRefreshRequest struct {
	Target     string `json:"target"`
	PitLossURL string `json:"pit_loss_url,omitempty"`
	Reason     string
}

type staticDataSourceStatus struct {
	Enabled      bool   `json:"enabled"`
	Configured   bool   `json:"configured"`
	LastAttempt  string `json:"last_attempt,omitempty"`
	LastSuccess  string `json:"last_success,omitempty"`
	LastError    string `json:"last_error,omitempty"`
	UpdatedCount int    `json:"updated_count,omitempty"`
}

type staticDataStatus struct {
	Enabled           bool                   `json:"enabled"`
	Running           bool                   `json:"running"`
	RefreshIntervalHr int                    `json:"refresh_interval_hours"`
	LastRun           string                 `json:"last_run,omitempty"`
	CircuitMetadata   staticDataSourceStatus `json:"circuit_metadata"`
	PitLoss           staticDataSourceStatus `json:"pit_loss"`
}

type staticDataRefresher struct {
	app            *app
	client         *http.Client
	enabled        bool
	onStart        bool
	interval       time.Duration
	timeout        time.Duration
	refreshYears   []int
	circuitEnabled bool
	pitLossEnabled bool
	pitLossURL     string
	triggerCh      chan staticDataRefreshRequest

	mu          sync.Mutex
	running     bool
	lastRun     time.Time
	circuitStat staticDataSourceStatus
	pitLossStat staticDataSourceStatus
}

type mvCircuitResponse struct {
	CircuitKey     int        `json:"circuitKey"`
	CircuitName    string     `json:"circuitName"`
	Rotation       float64    `json:"rotation"`
	Corners        []mvMarker `json:"corners"`
	MarshalSectors []mvMarker `json:"marshalSectors"`
}

type mvMarker struct {
	Number        int             `json:"number"`
	Letter        string          `json:"letter"`
	Angle         float64         `json:"angle"`
	TrackPosition mvTrackPosition `json:"trackPosition"`
}

type mvTrackPosition struct {
	X float64 `json:"x"`
	Y float64 `json:"y"`
}

type refreshSeasonIndex struct {
	Meetings []refreshSeasonMeeting `json:"Meetings"`
}

type refreshSeasonMeeting struct {
	Name         string                `json:"Name"`
	OfficialName string                `json:"OfficialName"`
	Circuit      refreshMeetingCircuit `json:"Circuit"`
}

type refreshMeetingCircuit struct {
	Key int `json:"Key"`
}

func newStaticDataRefresher(a *app) *staticDataRefresher {
	enabled := envBoolDefault("STATIC_DATA_REFRESH_ENABLED", true)
	onStart := envBoolDefault("STATIC_DATA_REFRESH_ON_START", true)
	intervalHours := readPositiveIntEnv("STATIC_DATA_REFRESH_INTERVAL_HOURS", 24*7)
	timeoutSec := readPositiveIntEnv("STATIC_DATA_REFRESH_TIMEOUT_SECONDS", 60)

	pitLossURL := strings.TrimSpace(os.Getenv("PIT_LOSS_REFRESH_URL"))
	pitEnabled := envBoolDefault("PIT_LOSS_AUTO_REFRESH_ENABLED", pitLossURL != "")
	circuitEnabled := envBoolDefault("CIRCUIT_METADATA_AUTO_REFRESH_ENABLED", true)

	refreshYears := parseRefreshYears(os.Getenv("CIRCUIT_METADATA_YEARS"))

	r := &staticDataRefresher{
		app:            a,
		client:         &http.Client{Timeout: time.Duration(timeoutSec) * time.Second},
		enabled:        enabled,
		onStart:        onStart,
		interval:       time.Duration(intervalHours) * time.Hour,
		timeout:        time.Duration(timeoutSec) * time.Second,
		refreshYears:   refreshYears,
		circuitEnabled: circuitEnabled,
		pitLossEnabled: pitEnabled,
		pitLossURL:     pitLossURL,
		triggerCh:      make(chan staticDataRefreshRequest, 8),
		circuitStat: staticDataSourceStatus{
			Enabled:    circuitEnabled,
			Configured: true,
		},
		pitLossStat: staticDataSourceStatus{
			Enabled:    pitEnabled,
			Configured: pitLossURL != "",
		},
	}
	return r
}

func envBoolDefault(name string, def bool) bool {
	raw := strings.TrimSpace(os.Getenv(name))
	if raw == "" {
		return def
	}
	return isTrue(raw)
}

func parseRefreshYears(raw string) []int {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return append([]int{}, availableSeasons...)
	}
	parts := strings.Split(raw, ",")
	seen := make(map[int]struct{})
	out := make([]int, 0, len(parts))
	for _, p := range parts {
		y, err := strconv.Atoi(strings.TrimSpace(p))
		if err != nil || y <= 0 {
			continue
		}
		if _, ok := seen[y]; ok {
			continue
		}
		seen[y] = struct{}{}
		out = append(out, y)
	}
	sort.Ints(out)
	if len(out) == 0 {
		return append([]int{}, availableSeasons...)
	}
	return out
}

func (r *staticDataRefresher) start() {
	if r == nil || !r.enabled {
		return
	}
	go r.loop()
	if r.onStart {
		_ = r.enqueue(staticDataRefreshRequest{Target: "all", Reason: "startup"})
	}
}

func (r *staticDataRefresher) loop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()
	for {
		select {
		case req := <-r.triggerCh:
			r.run(req)
		case <-ticker.C:
			r.run(staticDataRefreshRequest{Target: "all", Reason: "scheduled"})
		}
	}
}

func (r *staticDataRefresher) enqueue(req staticDataRefreshRequest) error {
	if r == nil || !r.enabled {
		return errors.New("static data refresher is disabled")
	}
	target := strings.ToLower(strings.TrimSpace(req.Target))
	if target == "" {
		target = "all"
	}
	switch target {
	case "all", "circuit", "pit_loss":
		req.Target = target
	default:
		return fmt.Errorf("invalid target %q", req.Target)
	}
	select {
	case r.triggerCh <- req:
		return nil
	default:
		return errors.New("refresh queue is full")
	}
}

func (r *staticDataRefresher) run(req staticDataRefreshRequest) {
	r.mu.Lock()
	if r.running {
		r.mu.Unlock()
		return
	}
	r.running = true
	r.lastRun = time.Now().UTC()
	r.mu.Unlock()

	defer func() {
		r.mu.Lock()
		r.running = false
		r.mu.Unlock()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), r.timeout)
	defer cancel()

	target := req.Target
	if target == "" {
		target = "all"
	}
	if target == "all" || target == "circuit" {
		r.runCircuitRefresh(ctx)
	}
	if target == "all" || target == "pit_loss" {
		r.runPitLossRefresh(ctx, strings.TrimSpace(req.PitLossURL))
	}
}

func (r *staticDataRefresher) runCircuitRefresh(ctx context.Context) {
	r.mu.Lock()
	r.circuitStat.LastAttempt = time.Now().UTC().Format(time.RFC3339)
	r.mu.Unlock()
	if !r.circuitEnabled {
		return
	}

	updated, err := r.refreshCircuitMetadata(ctx)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.circuitStat.LastError = err.Error()
		return
	}
	r.circuitStat.LastError = ""
	r.circuitStat.LastSuccess = time.Now().UTC().Format(time.RFC3339)
	r.circuitStat.UpdatedCount = updated
}

func (r *staticDataRefresher) runPitLossRefresh(ctx context.Context, overrideURL string) {
	r.mu.Lock()
	r.pitLossStat.LastAttempt = time.Now().UTC().Format(time.RFC3339)
	r.mu.Unlock()
	if !r.pitLossEnabled {
		return
	}

	url := strings.TrimSpace(overrideURL)
	if url == "" {
		url = r.pitLossURL
	}
	if url == "" {
		r.mu.Lock()
		r.pitLossStat.LastError = "PIT_LOSS_REFRESH_URL is not configured"
		r.mu.Unlock()
		return
	}

	changed, err := r.refreshPitLoss(ctx, url)
	r.mu.Lock()
	defer r.mu.Unlock()
	if err != nil {
		r.pitLossStat.LastError = err.Error()
		return
	}
	r.pitLossStat.LastError = ""
	r.pitLossStat.LastSuccess = time.Now().UTC().Format(time.RFC3339)
	if changed {
		r.pitLossStat.UpdatedCount++
	}
}

func (r *staticDataRefresher) refreshCircuitMetadata(ctx context.Context) (int, error) {
	if r.app.store == nil {
		return 0, errors.New("sqlite store is not configured")
	}
	baseURL := strings.TrimSpace(os.Getenv("F1_STATIC_BASE_URL"))
	if baseURL == "" {
		baseURL = defaultF1StaticBase
	}
	baseURL = strings.TrimRight(baseURL, "/")

	circuitPreferredYear := make(map[int]int)
	for _, year := range r.refreshYears {
		idx, err := r.fetchSeasonIndex(ctx, baseURL, year)
		if err != nil {
			continue
		}
		for _, m := range idx.Meetings {
			if isTestingMeeting(m.Name, m.OfficialName) || m.Circuit.Key <= 0 {
				continue
			}
			if _, ok := circuitPreferredYear[m.Circuit.Key]; !ok {
				circuitPreferredYear[m.Circuit.Key] = year
			}
		}
	}
	if len(circuitPreferredYear) == 0 {
		return 0, errors.New("no circuits discovered from season indexes")
	}

	existing := make(map[int]circuitMetadataEntry)
	if raw, err := r.app.store.GetJSONArtifact(ctx, "circuit_metadata.json"); err == nil {
		if parsed, parseErr := loadCircuitMetadata(raw); parseErr == nil {
			for k, v := range parsed.Circuits {
				existing[k] = v
			}
		}
	}
	if len(existing) == 0 {
		if parsed, parseErr := loadCircuitMetadata(embeddedCircuitMetadata); parseErr == nil {
			for k, v := range parsed.Circuits {
				existing[k] = v
			}
		}
	}

	updated := 0
	for key, preferredYear := range circuitPreferredYear {
		meta, err := r.fetchCircuitMetadata(ctx, key, preferredYear)
		if err != nil {
			continue
		}
		existing[key] = meta
		updated++
	}
	if len(existing) == 0 {
		return 0, errors.New("no usable circuit metadata entries")
	}

	body, err := marshalCircuitMetadata(existing)
	if err != nil {
		return 0, err
	}
	if _, err := loadCircuitMetadata(body); err != nil {
		return 0, fmt.Errorf("refreshed metadata validation failed: %w", err)
	}
	if err := r.app.store.PutJSONArtifact(ctx, "circuit_metadata.json", body); err != nil {
		return 0, err
	}
	if gp, ok := r.app.processor.(*GoSessionProcessor); ok {
		if err := gp.SetCircuitMetadata(body); err != nil {
			return 0, err
		}
	}
	return updated, nil
}

func marshalCircuitMetadata(entries map[int]circuitMetadataEntry) ([]byte, error) {
	keys := make([]int, 0, len(entries))
	for k := range entries {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	circuits := make(map[string]circuitMetadataFileEntry, len(keys))
	for _, key := range keys {
		e := entries[key]
		circuits[strconv.Itoa(key)] = circuitMetadataFileEntry{
			CircuitKey:     e.CircuitKey,
			CircuitName:    e.CircuitName,
			Rotation:       e.Rotation,
			Corners:        append([]circuitCornerMeta(nil), e.Corners...),
			MarshalSectors: append([]circuitMarshalSectorMeta(nil), e.MarshalSectors...),
		}
	}
	out := circuitMetadataFile{
		Version:     circuitMetadataVersion,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Source:      "auto_refresh_multiviewer+f1_static",
		Circuits:    circuits,
	}
	return json.Marshal(out)
}

func (r *staticDataRefresher) fetchSeasonIndex(ctx context.Context, baseURL string, year int) (*refreshSeasonIndex, error) {
	url := fmt.Sprintf("%s/%d/Index.json", baseURL, year)
	body, err := r.fetchBytes(ctx, url)
	if err != nil {
		return nil, err
	}
	var idx refreshSeasonIndex
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, err
	}
	return &idx, nil
}

func orderedCandidateYears(years []int, preferred int) []int {
	seen := make(map[int]struct{}, len(years)+1)
	out := make([]int, 0, len(years)+1)
	if preferred > 0 {
		out = append(out, preferred)
		seen[preferred] = struct{}{}
	}
	for i := len(years) - 1; i >= 0; i-- {
		y := years[i]
		if y <= 0 {
			continue
		}
		if _, ok := seen[y]; ok {
			continue
		}
		seen[y] = struct{}{}
		out = append(out, y)
	}
	return out
}

func (r *staticDataRefresher) fetchCircuitMetadata(ctx context.Context, circuitKey int, preferredYear int) (circuitMetadataEntry, error) {
	for _, year := range orderedCandidateYears(r.refreshYears, preferredYear) {
		url := fmt.Sprintf("%s/%d/%d", mvCircuitAPIBase, circuitKey, year)
		body, err := r.fetchBytes(ctx, url)
		if err != nil {
			continue
		}
		var raw mvCircuitResponse
		if err := json.Unmarshal(body, &raw); err != nil {
			continue
		}
		meta, err := convertMVCircuitMetadata(raw, circuitKey)
		if err != nil {
			continue
		}
		return meta, nil
	}
	return circuitMetadataEntry{}, fmt.Errorf("no valid metadata for circuit %d", circuitKey)
}

func convertMVCircuitMetadata(raw mvCircuitResponse, fallbackKey int) (circuitMetadataEntry, error) {
	key := raw.CircuitKey
	if key <= 0 {
		key = fallbackKey
	}
	if key <= 0 {
		return circuitMetadataEntry{}, errors.New("missing circuit key")
	}
	corners := make([]circuitCornerMeta, 0, len(raw.Corners))
	for _, c := range raw.Corners {
		if c.Number <= 0 {
			continue
		}
		corners = append(corners, circuitCornerMeta{
			X:      c.TrackPosition.X,
			Y:      c.TrackPosition.Y,
			Number: c.Number,
			Letter: strings.TrimSpace(c.Letter),
			Angle:  c.Angle,
		})
	}
	marshal := make([]circuitMarshalSectorMeta, 0, len(raw.MarshalSectors))
	for _, s := range raw.MarshalSectors {
		if s.Number <= 0 {
			continue
		}
		marshal = append(marshal, circuitMarshalSectorMeta{
			X:      s.TrackPosition.X,
			Y:      s.TrackPosition.Y,
			Number: s.Number,
		})
	}
	if len(corners) == 0 || len(marshal) == 0 {
		return circuitMetadataEntry{}, errors.New("missing corners or marshal sectors")
	}
	return circuitMetadataEntry{
		CircuitKey:     key,
		CircuitName:    strings.TrimSpace(raw.CircuitName),
		Rotation:       raw.Rotation,
		Corners:        corners,
		MarshalSectors: marshal,
	}, nil
}

func (r *staticDataRefresher) refreshPitLoss(ctx context.Context, sourceURL string) (bool, error) {
	if r.app.store == nil {
		return false, errors.New("sqlite store is not configured")
	}
	body, err := r.fetchBytes(ctx, sourceURL)
	if err != nil {
		return false, err
	}
	var parsed map[string]any
	if err := json.Unmarshal(body, &parsed); err != nil {
		return false, fmt.Errorf("invalid pit_loss json: %w", err)
	}
	if len(parsed) == 0 {
		return false, errors.New("pit_loss payload is empty")
	}
	if _, ok := parsed["circuits"]; !ok {
		return false, errors.New("pit_loss payload missing circuits")
	}
	if err := r.app.store.PutJSONArtifact(ctx, "pit_loss.json", body); err != nil {
		return false, err
	}

	r.app.pitLossMu.Lock()
	r.app.pitLossRaw = nil
	r.app.pitLossLoadedAt = time.Time{}
	r.app.pitLossMu.Unlock()

	return true, nil
}

func (r *staticDataRefresher) fetchBytes(ctx context.Context, url string) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("http %s -> %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return bytes.TrimSpace(body), nil
}

func (r *staticDataRefresher) status() staticDataStatus {
	if r == nil {
		return staticDataStatus{Enabled: false}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	out := staticDataStatus{
		Enabled:           r.enabled,
		Running:           r.running,
		RefreshIntervalHr: int(r.interval / time.Hour),
		CircuitMetadata:   r.circuitStat,
		PitLoss:           r.pitLossStat,
	}
	if !r.lastRun.IsZero() {
		out.LastRun = r.lastRun.Format(time.RFC3339)
	}
	return out
}

func (a *app) handleStaticDataStatus(w http.ResponseWriter, _ *http.Request) {
	if a.staticData == nil {
		writeJSON(w, http.StatusOK, staticDataStatus{Enabled: false})
		return
	}
	writeJSON(w, http.StatusOK, a.staticData.status())
}

func (a *app) handleStaticDataRefresh(w http.ResponseWriter, r *http.Request) {
	if a.staticData == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]any{"detail": "static data refresher is not available"})
		return
	}
	var req staticDataRefreshRequest
	if r.ContentLength > 0 {
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid JSON"})
			return
		}
	}
	if err := a.staticData.enqueue(req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": err.Error()})
		return
	}
	writeJSON(w, http.StatusAccepted, map[string]any{
		"queued": true,
		"target": defaultString(strings.TrimSpace(req.Target), "all"),
	})
}
