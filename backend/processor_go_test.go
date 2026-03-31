package main

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestParseStreamTimestamp(t *testing.T) {
	v, ok := parseStreamTimestamp("01:02:03.500")
	if !ok {
		t.Fatalf("expected parse success")
	}
	if v != 3723.5 {
		t.Fatalf("unexpected timestamp: got %.3f want 3723.5", v)
	}
	if _, ok := parseStreamTimestamp("bad"); ok {
		t.Fatalf("expected parse failure for malformed timestamp")
	}
}

func TestDecodeZPayload(t *testing.T) {
	src := []byte(`{"hello":"world","n":123}`)
	var b bytes.Buffer
	w, err := flate.NewWriter(&b, flate.DefaultCompression)
	if err != nil {
		t.Fatalf("flate writer: %v", err)
	}
	if _, err := w.Write(src); err != nil {
		t.Fatalf("flate write: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("flate close: %v", err)
	}
	encoded := base64.StdEncoding.EncodeToString(b.Bytes())
	dec, err := decodeZPayload(encoded)
	if err != nil {
		t.Fatalf("decodeZPayload: %v", err)
	}
	if string(dec) != string(src) {
		t.Fatalf("decoded mismatch: got %s want %s", string(dec), string(src))
	}
}

func TestReplayWriterAndIndexOffsets(t *testing.T) {
	dir := t.TempDir()
	replayPath := filepath.Join(dir, "replay.json")
	w, err := newReplayWriter(replayPath)
	if err != nil {
		t.Fatalf("newReplayWriter: %v", err)
	}
	frame1 := map[string]any{"timestamp": 0.5, "lap": 1, "total_laps": 10, "drivers": []any{}}
	frame2 := map[string]any{"timestamp": 1.0, "lap": 1, "total_laps": 10, "drivers": []any{}}
	if err := w.writeFrame(frame1); err != nil {
		t.Fatalf("write frame1: %v", err)
	}
	if err := w.writeFrame(frame2); err != nil {
		t.Fatalf("write frame2: %v", err)
	}
	if err := w.commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	f, err := os.Open(replayPath)
	if err != nil {
		t.Fatalf("open replay: %v", err)
	}
	defer f.Close()

	if len(w.frames) != 2 {
		t.Fatalf("expected 2 frame offsets, got %d", len(w.frames))
	}
	raw, err := readFrameAt(f, w.frames[0])
	if err != nil {
		t.Fatalf("readFrameAt: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("unmarshal frame raw: %v", err)
	}
	if asFloat(decoded["timestamp"], 0) != 0.5 {
		t.Fatalf("unexpected first frame timestamp")
	}
}

func TestTimelineLookups(t *testing.T) {
	tl := []timingState{
		{T: 1.0, Lap: 1},
		{T: 2.0, Lap: 2},
		{T: 3.0, Lap: 3},
	}
	if lap := lapAt(tl, 0.5); lap != 0 {
		t.Fatalf("lapAt before first expected 0, got %d", lap)
	}
	if lap := lapAt(tl, 2.4); lap != 2 {
		t.Fatalf("lapAt mid expected 2, got %d", lap)
	}
	pos := []posSample{
		{T: 1.0, X: 10, Y: 20},
		{T: 2.0, X: 12, Y: 22},
		{T: 3.0, X: 14, Y: 24},
	}
	n := nearestPosSample(pos, 2.6)
	if n == nil || n.X != 14 {
		t.Fatalf("nearestPosSample expected third point")
	}
}

func TestDeriveReplayWindow(t *testing.T) {
	points := []sessionStatusPoint{
		{T: 100, Status: "Inactive"},
		{T: 3600, Status: "Started"},
		{T: 9000, Status: "Finished"},
		{T: 9050, Status: "Finalised"},
	}
	w := deriveReplayWindow(points, "R")
	if w == nil {
		t.Fatalf("expected replay window")
	}
	if w.Start != 3600 {
		t.Fatalf("unexpected start: got %.1f want 3600", w.Start)
	}
	if w.End != 9000 {
		t.Fatalf("unexpected end: got %.1f want 9000", w.End)
	}
}

func TestDeriveReplayWindowNoStart(t *testing.T) {
	points := []sessionStatusPoint{
		{T: 100, Status: "Inactive"},
		{T: 9000, Status: "Finished"},
	}
	if w := deriveReplayWindow(points, "R"); w != nil {
		t.Fatalf("expected nil replay window when no start status")
	}
}

func TestDeriveReplayWindowNonRaceUsesLastTerminal(t *testing.T) {
	points := []sessionStatusPoint{
		{T: 100, Status: "Started"},
		{T: 1000, Status: "Finished"},
		{T: 1100, Status: "Started"},
		{T: 2000, Status: "Finished"},
	}
	w := deriveReplayWindow(points, "Q")
	if w == nil {
		t.Fatalf("expected replay window")
	}
	if w.Start != 100 {
		t.Fatalf("unexpected start: got %.1f want 100", w.Start)
	}
	if w.End != 2000 {
		t.Fatalf("unexpected end: got %.1f want 2000", w.End)
	}
}

func TestPickRepresentativeTrackLap(t *testing.T) {
	timing := map[string][]timingState{
		"1": {
			{T: 10, Lap: 1},
			{T: 20, Lap: 2},
			{T: 30, Lap: 3},
		},
	}
	pos := map[string][]posSample{
		"1": {},
	}
	// Lap 1 (ignored)
	for i := 0; i < 130; i++ {
		pos["1"] = append(pos["1"], posSample{T: 10.1 + float64(i)*0.05, X: float64(i), Y: 0})
	}
	// Lap 2 (should be selected)
	for i := 0; i < 150; i++ {
		pos["1"] = append(pos["1"], posSample{T: 20.1 + float64(i)*0.05, X: float64(i), Y: 1})
	}
	// Lap 3 (ignored as final lap)
	for i := 0; i < 140; i++ {
		pos["1"] = append(pos["1"], posSample{T: 30.1 + float64(i)*0.05, X: float64(i), Y: 2})
	}

	pts := pickRepresentativeTrackLap(pos, timing)
	if len(pts) != 150 {
		t.Fatalf("unexpected selected lap size: got %d want 150", len(pts))
	}
	if pts[0].Y != 1 {
		t.Fatalf("selected wrong lap, expected lap-2 points")
	}
}

func TestBoundsForTrackAndNormalizeCoord(t *testing.T) {
	points := []posSample{
		{X: 10, Y: 20},
		{X: 30, Y: 40},
		{X: 20, Y: 35},
	}
	xMin, yMin, scale := boundsForTrack(points)
	if xMin != 10 || yMin != 20 {
		t.Fatalf("unexpected mins: xMin=%v yMin=%v", xMin, yMin)
	}
	if scale != 20 {
		t.Fatalf("unexpected scale: got %v want 20", scale)
	}
	x := normalizeCoord(30, xMin, scale)
	y := normalizeCoord(40, yMin, scale)
	if x != 1 || y != 1 {
		t.Fatalf("unexpected normalized coords: x=%v y=%v", x, y)
	}
}

func TestGoProcessorEnsureSchedule(t *testing.T) {
	if os.Getenv("RUN_NET_TESTS") != "1" {
		t.Skip("set RUN_NET_TESTS=1 to run network-backed processor tests")
	}
	dir := t.TempDir()
	p := NewGoSessionProcessor(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.EnsureSchedule(ctx, 2026); err != nil {
		t.Fatalf("EnsureSchedule failed: %v", err)
	}
	rel := filepath.Join(dir, "seasons", "2026", "schedule.json")
	if _, err := os.Stat(rel); err != nil {
		t.Fatalf("schedule output missing: %v", err)
	}
}

func TestGoProcessorProcessSessionSmoke(t *testing.T) {
	if os.Getenv("RUN_PROCESS_TEST") != "1" {
		t.Skip("set RUN_PROCESS_TEST=1 to run full process-session smoke test")
	}
	year := 2026
	round := 1
	sessionType := "R"
	if raw := os.Getenv("PROCESS_TEST_YEAR"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			year = v
		}
	}
	if raw := os.Getenv("PROCESS_TEST_ROUND"); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			round = v
		}
	}
	if raw := strings.TrimSpace(os.Getenv("PROCESS_TEST_TYPE")); raw != "" {
		sessionType = strings.ToUpper(raw)
	}

	dir := t.TempDir()
	p := NewGoSessionProcessor(dir)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := p.ProcessSession(ctx, year, round, sessionType, nil); err != nil {
		t.Fatalf("ProcessSession failed: %v", err)
	}
	required := []string{
		filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "info.json"),
		filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "track.json"),
		filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "laps.json"),
		filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "results.json"),
		filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "replay.json"),
		filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "replay.index.json"),
	}
	for _, pth := range required {
		if _, err := os.Stat(pth); err != nil {
			t.Fatalf("required artifact missing %s: %v", pth, err)
		}
	}
}
