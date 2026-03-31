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

func TestParseLapFromGapString(t *testing.T) {
	if got := parseLapFromGapString("LAP 17"); got != 17 {
		t.Fatalf("expected 17, got %d", got)
	}
	if got := parseLapFromGapString("lap 3"); got != 3 {
		t.Fatalf("expected 3, got %d", got)
	}
	if got := parseLapFromGapString("+2.331"); got != 0 {
		t.Fatalf("expected 0, got %d", got)
	}
}

func TestCurrentLapFromLeaderRaceSemantics(t *testing.T) {
	abbrMap := map[string]string{"PER": "11"}
	timing := map[string][]timingState{
		"11": {
			{T: 100, Lap: 1, Gap: ""},
			{T: 200, Lap: 2, Gap: ""},
		},
	}
	if lap := currentLapFromLeader("R", 53, "PER", abbrMap, timing, 100); lap != 2 {
		t.Fatalf("expected race lap 2 after first completed lap, got %d", lap)
	}
	if lap := currentLapFromLeader("R", 53, "PER", abbrMap, timing, 200); lap != 3 {
		t.Fatalf("expected race lap 3 after second completed lap, got %d", lap)
	}
}

func TestCurrentLapFromLeaderUsesGapString(t *testing.T) {
	abbrMap := map[string]string{"PER": "11"}
	timing := map[string][]timingState{
		"11": {
			{T: 100, Lap: 1, Gap: "LAP 7"},
		},
	}
	if lap := currentLapFromLeader("R", 53, "PER", abbrMap, timing, 100); lap != 7 {
		t.Fatalf("expected lap from gap string, got %d", lap)
	}
}

func TestLapForTyreState(t *testing.T) {
	if got := lapForTyreState("R", 0); got != 1 {
		t.Fatalf("race tyre lap expected 1, got %d", got)
	}
	if got := lapForTyreState("R", 2); got != 3 {
		t.Fatalf("race tyre lap expected 3, got %d", got)
	}
	if got := lapForTyreState("Q", 2); got != 2 {
		t.Fatalf("quali tyre lap expected 2, got %d", got)
	}
}

func TestTyreStateForLap(t *testing.T) {
	stints := []map[string]any{
		{"LapNumber": 1.0, "Compound": "SOFT"},
		{"LapNumber": 12.0, "Compound": "MEDIUM"},
		{"LapNumber": 35.0, "Compound": "HARD"},
	}
	compound, life, history, pitStops := tyreStateForLap(stints, 18)
	if asString(compound) != "MEDIUM" {
		t.Fatalf("expected MEDIUM, got %v", compound)
	}
	if asInt(life) != 7 {
		t.Fatalf("expected tyre life 7, got %v", life)
	}
	if pitStops != 1 {
		t.Fatalf("expected 1 pit stop, got %d", pitStops)
	}
	if len(history) != 1 || asString(history[0]) != "SOFT" {
		t.Fatalf("unexpected history: %#v", history)
	}
}

func TestExtractTimingAppStintsKeysByAbbreviation(t *testing.T) {
	timingApp := map[string]any{
		"Lines": map[string]any{
			"63": map[string]any{
				"RacingNumber": "63",
				"Stints": []any{
					map[string]any{"LapNumber": 1.0, "Compound": "SOFT"},
				},
			},
		},
	}
	byNum := map[string]driverMeta{
		"63": {Abbr: "RUS"},
	}
	stints := extractTimingAppStints(timingApp, byNum)
	if _, ok := stints["RUS"]; !ok {
		t.Fatalf("expected stints keyed by abbreviation")
	}
	if _, ok := stints["63"]; ok {
		t.Fatalf("did not expect racing number key in stints map")
	}
}

func TestLoadCircuitMetadataValidation(t *testing.T) {
	valid := []byte(`{
  "version": 1,
  "circuits": {
    "49": {
      "rotation": 237,
      "corners": [{"x": 1, "y": 2, "number": 1, "angle": 90}],
      "marshal_sectors": [{"x": 3, "y": 4, "number": 1}]
    }
  }
}`)
	idx, err := loadCircuitMetadata(valid)
	if err != nil {
		t.Fatalf("expected valid metadata, got err=%v", err)
	}
	if idx == nil || len(idx.Circuits) != 1 {
		t.Fatalf("unexpected metadata index")
	}

	badJSON := []byte(`{"version":1,"circuits":`)
	if _, err := loadCircuitMetadata(badJSON); err == nil {
		t.Fatalf("expected json decode error")
	}

	missingMarkers := []byte(`{
  "version": 1,
  "circuits": {
    "49": {
      "rotation": 237,
      "corners": [],
      "marshal_sectors": [{"x": 3, "y": 4, "number": 1}]
    }
  }
}`)
	if _, err := loadCircuitMetadata(missingMarkers); err == nil {
		t.Fatalf("expected validation error for missing corners")
	}
}

func TestExtractCircuitKeyFromSessionInfo(t *testing.T) {
	sessionInfo := map[string]any{
		"Meeting": map[string]any{
			"Name":     "Chinese Grand Prix",
			"Location": "Shanghai",
			"Circuit": map[string]any{
				"Key":       49.0,
				"ShortName": "Shanghai",
			},
		},
	}
	key, name, err := extractCircuitKeyFromSessionInfo(sessionInfo)
	if err != nil {
		t.Fatalf("extractCircuitKeyFromSessionInfo failed: %v", err)
	}
	if key != 49 || name != "Shanghai" {
		t.Fatalf("unexpected key/name: %d %s", key, name)
	}
}

func TestResolveCircuitMetadataMissing(t *testing.T) {
	p := &GoSessionProcessor{
		circuitMeta: &circuitMetadataIndex{
			Version:  circuitMetadataVersion,
			Circuits: map[int]circuitMetadataEntry{},
		},
	}
	sessionInfo := map[string]any{
		"Meeting": map[string]any{
			"Location": "Shanghai",
			"Circuit":  map[string]any{"Key": 49.0, "ShortName": "Shanghai"},
		},
	}
	if _, _, err := p.resolveCircuitMetadata(sessionInfo); err == nil {
		t.Fatalf("expected missing metadata error")
	}
}

func TestBuildTrackJSONWithMetadata(t *testing.T) {
	pos := map[string][]posSample{
		"1": {
			{T: 10.1, X: 0, Y: 0},
			{T: 10.2, X: 1, Y: 0},
			{T: 10.3, X: 2, Y: 0},
			{T: 20.1, X: 0, Y: 0},
			{T: 20.2, X: 0, Y: 1},
			{T: 20.3, X: 0, Y: 2},
			{T: 30.1, X: 0, Y: 0},
			{T: 30.2, X: -1, Y: 0},
			{T: 30.3, X: -2, Y: 0},
		},
	}
	timing := map[string][]timingState{
		"1": {
			{T: 10, Lap: 1},
			{T: 20, Lap: 2},
			{T: 30, Lap: 3},
		},
	}
	meta := circuitMetadataEntry{
		CircuitKey:  49,
		CircuitName: "Shanghai",
		Rotation:    237,
		Corners: []circuitCornerMeta{
			{X: 0, Y: 0, Number: 1, Angle: 10},
		},
		MarshalSectors: []circuitMarshalSectorMeta{
			{X: 0, Y: 0, Number: 1},
		},
	}
	track := buildTrackJSON(pos, timing, meta, "Shanghai")
	if asFloat(track["rotation"], -1) != 237 {
		t.Fatalf("unexpected rotation in track json")
	}
	corners, _ := track["corners"].([]map[string]any)
	if len(corners) == 0 {
		t.Fatalf("expected non-empty corners")
	}
	ms, _ := track["marshal_sectors"].([]map[string]any)
	if len(ms) == 0 {
		t.Fatalf("expected non-empty marshal sectors")
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
	trackPath := filepath.Join(dir, "sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "track.json")
	raw, err := os.ReadFile(trackPath)
	if err != nil {
		t.Fatalf("read track json: %v", err)
	}
	var track map[string]any
	if err := json.Unmarshal(raw, &track); err != nil {
		t.Fatalf("decode track json: %v", err)
	}
	if _, ok := track["rotation"]; !ok {
		t.Fatalf("track json missing rotation")
	}
	if year == 2026 && round == 2 && strings.EqualFold(sessionType, "R") {
		if rot := asFloat(track["rotation"], -1); rot != 237 {
			t.Fatalf("unexpected Shanghai rotation: got %.1f want 237.0", rot)
		}
	}
	if arr, ok := track["corners"].([]any); !ok || len(arr) == 0 {
		t.Fatalf("track json corners missing/empty")
	}
	if arr, ok := track["marshal_sectors"].([]any); !ok || len(arr) == 0 {
		t.Fatalf("track json marshal_sectors missing/empty")
	}
}
