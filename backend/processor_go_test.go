package main

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"f1replaytiming/backend/storage"
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

func TestMergeSectorTimesFromMapAndArray(t *testing.T) {
	empty := [3]string{}
	mapped := mergeSectorTimes(empty, map[string]any{
		"0": map[string]any{"Value": "30.111"},
		"1": map[string]any{"Value": "28.222"},
		"2": map[string]any{"Value": "27.333"},
	})
	if mapped[0] != "30.111" || mapped[1] != "28.222" || mapped[2] != "27.333" {
		t.Fatalf("unexpected mapped sectors: %#v", mapped)
	}

	updated := mergeSectorTimes(mapped, []any{
		map[string]any{"Value": "30.000"},
		nil,
		map[string]any{"Value": "27.100"},
	})
	if updated[0] != "30.000" || updated[1] != "28.222" || updated[2] != "27.100" {
		t.Fatalf("unexpected updated sectors: %#v", updated)
	}
}

func TestMergeSectorStatesPurpleDowngradesToGreenWhenOverallClearsWithoutPersonalFlag(t *testing.T) {
	base := [3]lapSectorState{}

	next, changed := mergeSectorStates(base, map[string]any{
		"0": map[string]any{"Value": "31.855", "OverallFastest": true},
	})
	if !changed {
		t.Fatalf("expected sector state change on first update")
	}
	if next[0].Color != "purple" {
		t.Fatalf("expected purple after overall fastest, got %q", next[0].Color)
	}

	next, changed = mergeSectorStates(next, map[string]any{
		"0": map[string]any{"OverallFastest": false},
	})
	if !changed {
		t.Fatalf("expected sector state change when overall fastest clears")
	}
	if next[0].Color != "green" {
		t.Fatalf("expected green after purple is taken without personal=false, got %q", next[0].Color)
	}

	next, changed = mergeSectorStates(next, map[string]any{
		"0": map[string]any{"PersonalFastest": false},
	})
	if !changed {
		t.Fatalf("expected sector state change when personal fastest becomes false")
	}
	if next[0].Color != "yellow" {
		t.Fatalf("expected yellow after explicit personal=false, got %q", next[0].Color)
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

func TestParsePositionStreamSkipsZeroPlaceholderPoints(t *testing.T) {
	encodeLine := func(payload map[string]any) string {
		raw, err := json.Marshal(payload)
		if err != nil {
			t.Fatalf("marshal payload: %v", err)
		}
		var b bytes.Buffer
		w, err := flate.NewWriter(&b, flate.DefaultCompression)
		if err != nil {
			t.Fatalf("flate writer: %v", err)
		}
		if _, err := w.Write(raw); err != nil {
			t.Fatalf("flate write: %v", err)
		}
		if err := w.Close(); err != nil {
			t.Fatalf("flate close: %v", err)
		}
		enc := base64.StdEncoding.EncodeToString(b.Bytes())
		quoted, err := json.Marshal(enc)
		if err != nil {
			t.Fatalf("marshal quoted encoded payload: %v", err)
		}
		return string(quoted)
	}

	stream := strings.Join([]string{
		"00:00:01.000" + encodeLine(map[string]any{
			"Position": []any{
				map[string]any{
					"Entries": map[string]any{
						"81": map[string]any{"X": 0.0, "Y": 0.0},
						"1":  map[string]any{"X": 10.0, "Y": 5.0},
					},
				},
			},
		}),
		"00:00:01.200" + encodeLine(map[string]any{
			"Position": []any{
				map[string]any{
					"Entries": map[string]any{
						"81": map[string]any{"X": 12.0, "Y": 8.0},
					},
				},
			},
		}),
		"00:00:01.400" + encodeLine(map[string]any{
			"Position": []any{
				map[string]any{
					"Entries": map[string]any{
						"81": map[string]any{"X": 0.0, "Y": 0.0},
					},
				},
			},
		}),
	}, "\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/session/Position.z.jsonStream" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, stream)
	}))
	defer srv.Close()

	p := &GoSessionProcessor{
		baseURL:     srv.URL,
		httpClient:  srv.Client(),
		rawMinDelta: 0,
	}
	out, err := p.parsePositionStream(context.Background(), "session", sessionFeed{
		StreamPath: "Position.z.jsonStream",
	})
	if err != nil {
		t.Fatalf("parsePositionStream failed: %v", err)
	}

	if got := len(out["81"]); got != 1 {
		t.Fatalf("driver 81 samples: got %d want 1", got)
	}
	if out["81"][0].X != 12.0 || out["81"][0].Y != 8.0 {
		t.Fatalf("unexpected 81 sample: %+v", out["81"][0])
	}
	if got := len(out["1"]); got != 1 {
		t.Fatalf("driver 1 samples: got %d want 1", got)
	}
}

func TestParseTimingDataStreamCapturesOfficialSectorTimes(t *testing.T) {
	stream := strings.Join([]string{
		`00:00:10.000{"Lines":{"44":{"RacingNumber":"44","NumberOfLaps":1,"Sectors":{"0":{"Value":"30.111","PersonalFastest":true}}}}}`,
		`00:00:20.000{"Lines":{"44":{"RacingNumber":"44","NumberOfLaps":1,"Sectors":{"1":{"Value":"28.222","OverallFastest":true,"PersonalFastest":true}}}}}`,
		`00:00:30.000{"Lines":{"44":{"RacingNumber":"44","NumberOfLaps":1,"Sectors":{"2":{"Value":"27.333","OverallFastest":false,"PersonalFastest":false}}}}}`,
		`00:00:31.000{"Lines":{"44":{"RacingNumber":"44","NumberOfLaps":1,"LastLapTime":{"Value":"1:25.666"}}}}`,
	}, "\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/session/TimingData.jsonStream" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, stream)
	}))
	defer srv.Close()

	p := &GoSessionProcessor{
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}

	driverByNum := map[string]driverMeta{
		"44": {Number: "44", Abbr: "HAM"},
	}
	_, laps, _, err := p.parseTimingDataStream(
		context.Background(),
		"session",
		sessionFeed{StreamPath: "TimingData.jsonStream"},
		driverByNum,
		map[string]any{},
		nil,
	)
	if err != nil {
		t.Fatalf("parseTimingDataStream failed: %v", err)
	}
	if len(laps) != 1 {
		t.Fatalf("expected 1 lap, got %d", len(laps))
	}
	if asString(laps[0]["driver"]) != "HAM" {
		t.Fatalf("unexpected driver: %v", laps[0]["driver"])
	}
	if asString(laps[0]["lap_time"]) != "1:25.666" {
		t.Fatalf("unexpected lap_time: %v", laps[0]["lap_time"])
	}
	if asString(laps[0]["sector1"]) != "30.111" || asString(laps[0]["sector2"]) != "28.222" || asString(laps[0]["sector3"]) != "27.333" {
		t.Fatalf("unexpected sectors: s1=%v s2=%v s3=%v", laps[0]["sector1"], laps[0]["sector2"], laps[0]["sector3"])
	}
	if asString(laps[0]["sector1_color"]) != "green" || asString(laps[0]["sector2_color"]) != "purple" || asString(laps[0]["sector3_color"]) != "yellow" {
		t.Fatalf("unexpected sector colors: s1=%v s2=%v s3=%v", laps[0]["sector1_color"], laps[0]["sector2_color"], laps[0]["sector3_color"])
	}
}

func TestParseTimingDataStreamAppliesLateSectorColorUpdateToCompletedLap(t *testing.T) {
	stream := strings.Join([]string{
		`00:00:10.000{"Lines":{"12":{"RacingNumber":"12","NumberOfLaps":12,"Sectors":{"0":{"Value":"31.855","PersonalFastest":true},"1":{"Value":"39.439","OverallFastest":true,"PersonalFastest":true},"2":{"Value":"17.484","PersonalFastest":false}}}}}`,
		`00:00:11.000{"Lines":{"12":{"RacingNumber":"12","NumberOfLaps":12,"LastLapTime":{"Value":"1:28.778"}}}}`,
		`00:00:11.300{"Lines":{"12":{"RacingNumber":"12","Sectors":{"2":{"OverallFastest":true,"PersonalFastest":true}}}}}`,
	}, "\n")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/session/TimingData.jsonStream" {
			http.NotFound(w, r)
			return
		}
		_, _ = io.WriteString(w, stream)
	}))
	defer srv.Close()

	p := &GoSessionProcessor{
		baseURL:    srv.URL,
		httpClient: srv.Client(),
	}
	driverByNum := map[string]driverMeta{
		"12": {Number: "12", Abbr: "ANT"},
	}

	_, laps, _, err := p.parseTimingDataStream(
		context.Background(),
		"session",
		sessionFeed{StreamPath: "TimingData.jsonStream"},
		driverByNum,
		map[string]any{},
		nil,
	)
	if err != nil {
		t.Fatalf("parseTimingDataStream failed: %v", err)
	}
	if len(laps) != 1 {
		t.Fatalf("expected 1 lap, got %d", len(laps))
	}

	lap := laps[0]
	if asString(lap["lap_time"]) != "1:28.778" {
		t.Fatalf("unexpected lap_time: %v", lap["lap_time"])
	}
	if asString(lap["sector1_color"]) != "green" {
		t.Fatalf("unexpected sector1 color: %v", lap["sector1_color"])
	}
	if asString(lap["sector2_color"]) != "purple" {
		t.Fatalf("unexpected sector2 color: %v", lap["sector2_color"])
	}
	if asString(lap["sector3_color"]) != "purple" {
		t.Fatalf("expected late update to mark sector3 purple, got %v", lap["sector3_color"])
	}
}

func TestBuildQ3LinesJSONUsesOfficialSectorColors(t *testing.T) {
	drivers := []driverMeta{
		{Number: "1", Abbr: "ANT", Team: "Mercedes", Color: "00D2BE"},
		{Number: "2", Abbr: "RUS", Team: "Mercedes", Color: "00D2BE"},
	}
	byNum := map[string]driverMeta{
		"1": drivers[0],
		"2": drivers[1],
	}

	laps := []map[string]any{
		{
			"driver":           "ANT",
			"lap_number":       1,
			"lap_time":         "1:00.000",
			"time":             60.0,
			"qualifying_phase": "Q3",
			"sector1":          "20.000",
			"sector2":          "20.500",
			"sector3":          "19.500",
			"sector1_color":    "yellow",
			"sector2_color":    "green",
			"sector3_color":    "yellow",
		},
		{
			"driver":           "ANT",
			"lap_number":       2,
			"lap_time":         "0:59.500",
			"time":             120.0,
			"qualifying_phase": "Q3",
			"sector1":          "20.000",
			"sector2":          "20.000",
			"sector3":          "19.500",
			"sector1_color":    "green",
			"sector2_color":    "purple",
			"sector3_color":    "purple",
		},
		{
			"driver":           "RUS",
			"lap_number":       1,
			"lap_time":         "1:00.200",
			"time":             60.0,
			"qualifying_phase": "Q3",
			"sector1":          "20.000",
			"sector2":          "20.200",
			"sector3":          "20.000",
			"sector1_color":    "purple",
			"sector2_color":    "green",
			"sector3_color":    "green",
		},
	}

	buildLap := func(startT, endT, xOff, yOff float64) []posSample {
		out := make([]posSample, 0, 180)
		total := 180
		for i := 0; i < total; i++ {
			ratio := float64(i) / float64(total-1)
			theta := ratio * 2 * 3.141592653589793
			out = append(out, posSample{
				T: startT + ratio*(endT-startT),
				X: xOff + math.Cos(theta),
				Y: yOff + math.Sin(theta),
			})
		}
		return out
	}

	pos := map[string][]posSample{
		"1": buildLap(60.5, 120.0, 0, 0), // ANT best lap (lap 2)
		"2": buildLap(0.0, 60.0, 2, 2),   // RUS best lap (lap 1)
	}
	timing := map[string][]timingState{
		"1": {{T: 60.0, Lap: 1}, {T: 120.0, Lap: 2}},
		"2": {{T: 60.0, Lap: 1}},
	}
	track := map[string]any{
		"norm": map[string]any{
			"x_min": 0.0,
			"y_min": 0.0,
			"scale": 1.0,
		},
	}

	out := buildQ3LinesJSON(drivers, byNum, laps, pos, timing, track)
	if out == nil {
		t.Fatalf("expected q3 lines output")
	}
	rows, ok := out["drivers"].([]map[string]any)
	if !ok || len(rows) != 2 {
		t.Fatalf("expected 2 drivers in q3 output, got %#v", out["drivers"])
	}

	var antColors, rusColors map[string]any
	for _, row := range rows {
		abbr := asString(row["abbr"])
		colors, _ := row["sector_colors"].(map[string]any)
		if abbr == "ANT" {
			antColors = colors
		}
		if abbr == "RUS" {
			rusColors = colors
		}
	}
	if antColors == nil || rusColors == nil {
		t.Fatalf("missing sector colors for ANT/RUS")
	}

	if asString(antColors["s1"]) != "green" || asString(antColors["s2"]) != "purple" || asString(antColors["s3"]) != "purple" {
		t.Fatalf("ANT colors should come from official lap payload, got %#v", antColors)
	}
	if asString(rusColors["s1"]) != "purple" || asString(rusColors["s2"]) != "green" || asString(rusColors["s3"]) != "green" {
		t.Fatalf("RUS colors should come from official lap payload, got %#v", rusColors)
	}
}

func TestBuildQ3LinesJSONFallbacksMissingSectorColorsWithinQ3(t *testing.T) {
	drivers := []driverMeta{
		{Number: "1", Abbr: "ANT", Team: "Mercedes", Color: "00D2BE"},
		{Number: "2", Abbr: "RUS", Team: "Mercedes", Color: "00D2BE"},
		{Number: "3", Abbr: "PIA", Team: "McLaren", Color: "FF8000"},
	}
	byNum := map[string]driverMeta{
		"1": drivers[0],
		"2": drivers[1],
		"3": drivers[2],
	}

	laps := []map[string]any{
		// ANT best lap (fallback colors should apply on s1/s2; s3 is official and must be preserved)
		{
			"driver":           "ANT",
			"lap_number":       1,
			"lap_time":         "1:28.700",
			"time":             60.0,
			"qualifying_phase": "Q3",
			"sector1":          "30.000",
			"sector2":          "40.000",
			"sector3":          "18.000",
			"sector3_color":    "yellow",
		},
		{
			"driver":           "ANT",
			"lap_number":       2,
			"lap_time":         "1:29.000",
			"time":             120.0,
			"qualifying_phase": "Q3",
			"sector1":          "30.200",
			"sector2":          "39.500",
			"sector3":          "18.200",
		},
		// RUS ties ANT on sector1 but slower lap, so RUS should be green (single purple owner rule).
		{
			"driver":           "RUS",
			"lap_number":       1,
			"lap_time":         "1:29.000",
			"time":             60.0,
			"qualifying_phase": "Q3",
			"sector1":          "30.000",
			"sector2":          "39.900",
			"sector3":          "18.100",
		},
		{
			"driver":           "RUS",
			"lap_number":       2,
			"lap_time":         "1:29.500",
			"time":             120.0,
			"qualifying_phase": "Q3",
			"sector1":          "30.300",
			"sector2":          "39.800",
			"sector3":          "18.300",
		},
		// PIA has one valid Q3 lap; missing colors should fallback to green.
		{
			"driver":           "PIA",
			"lap_number":       1,
			"lap_time":         "1:29.200",
			"time":             60.0,
			"qualifying_phase": "Q3",
			"sector1":          "30.400",
			"sector2":          "40.100",
			"sector3":          "18.400",
		},
	}

	buildLap := func(startT, endT, xOff, yOff float64) []posSample {
		out := make([]posSample, 0, 180)
		total := 180
		for i := 0; i < total; i++ {
			ratio := float64(i) / float64(total-1)
			theta := ratio * 2 * 3.141592653589793
			out = append(out, posSample{
				T: startT + ratio*(endT-startT),
				X: xOff + math.Cos(theta),
				Y: yOff + math.Sin(theta),
			})
		}
		return out
	}

	pos := map[string][]posSample{
		"1": buildLap(0.0, 60.0, 0, 0),
		"2": buildLap(0.0, 60.0, 2, 2),
		"3": buildLap(0.0, 60.0, 4, 4),
	}
	timing := map[string][]timingState{
		"1": {{T: 60.0, Lap: 1}, {T: 120.0, Lap: 2}},
		"2": {{T: 60.0, Lap: 1}, {T: 120.0, Lap: 2}},
		"3": {{T: 60.0, Lap: 1}},
	}
	track := map[string]any{
		"norm": map[string]any{
			"x_min": 0.0,
			"y_min": 0.0,
			"scale": 1.0,
		},
	}

	out := buildQ3LinesJSON(drivers, byNum, laps, pos, timing, track)
	if out == nil {
		t.Fatalf("expected q3 lines output")
	}
	rows, ok := out["drivers"].([]map[string]any)
	if !ok || len(rows) != 3 {
		t.Fatalf("expected 3 drivers in q3 output, got %#v", out["drivers"])
	}

	colorsByAbbr := map[string]map[string]any{}
	for _, row := range rows {
		abbr := asString(row["abbr"])
		colors, _ := row["sector_colors"].(map[string]any)
		colorsByAbbr[abbr] = colors
	}

	ant := colorsByAbbr["ANT"]
	rus := colorsByAbbr["RUS"]
	pia := colorsByAbbr["PIA"]
	if ant == nil || rus == nil || pia == nil {
		t.Fatalf("missing sector colors for ANT/RUS/PIA: %#v", colorsByAbbr)
	}

	if asString(ant["s1"]) != "purple" {
		t.Fatalf("ANT S1 expected purple via fallback tie-break, got %v", ant["s1"])
	}
	if asString(rus["s1"]) != "green" {
		t.Fatalf("RUS S1 expected green (tie loser), got %v", rus["s1"])
	}
	if asString(ant["s2"]) != "yellow" {
		t.Fatalf("ANT S2 expected yellow (not driver PB), got %v", ant["s2"])
	}
	if asString(pia["s1"]) != "green" || asString(pia["s2"]) != "green" || asString(pia["s3"]) != "green" {
		t.Fatalf("PIA sectors expected green as single-lap fallback, got %#v", pia)
	}
	if asString(ant["s3"]) != "yellow" {
		t.Fatalf("ANT S3 official color should be preserved as yellow, got %v", ant["s3"])
	}
}

func TestWriteTelemetryFilesPrefersLapWindowsOverLapAt(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	defer st.Close()
	ctx := context.Background()

	const year, round = 2026, 77
	const sessionType = "Q"
	sessionID, err := st.UpsertSessionInfo(ctx, year, round, sessionType, map[string]any{
		"event_name": "Window Preference GP",
		"circuit":    "Test Circuit",
		"country":    "Test",
		"drivers": []any{
			map[string]any{
				"abbreviation":  "AAA",
				"driver_number": "1",
				"full_name":     "Driver A",
				"team_name":     "Team A",
				"team_color":    "#FFFFFF",
			},
		},
	})
	if err != nil {
		t.Fatalf("upsert session info: %v", err)
	}

	p := &GoSessionProcessor{
		store:                 st,
		telemetryChunkSamples: 512,
		telemetryChunkCodec:   storage.CodecProtobuf,
	}
	drivers := []driverMeta{{Number: "1", Abbr: "AAA", Team: "Team A", Color: "#FFFFFF"}}
	timing := map[string][]timingState{
		"1": {
			{T: 0, Lap: 1},
			{T: 60, Lap: 2},
			{T: 120, Lap: 3},
		},
	}
	cars := map[string][]carSample{
		"1": {
			// Intended best-lap window [10,20]
			{T: 12, Speed: 298, Throttle: 99, Brake: false, Gear: 8, RPM: 11800, DRS: 12},
			{T: 15, Speed: 305, Throttle: 100, Brake: false, Gear: 8, RPM: 12100, DRS: 12},
			{T: 18, Speed: 292, Throttle: 97, Brake: false, Gear: 7, RPM: 11600, DRS: 10},
			// lapAt-only lap 2 samples (should be ignored when window is usable)
			{T: 75, Speed: 45, Throttle: 18, Brake: true, Gear: 1, RPM: 6200, DRS: 0},
			{T: 78, Speed: 52, Throttle: 20, Brake: true, Gear: 1, RPM: 6400, DRS: 0},
		},
	}
	laps := []map[string]any{
		{
			"driver":     "AAA",
			"lap_number": 2,
			"lap_time":   "0:10.000",
			"time":       20.0,
		},
	}

	if err := p.writeTelemetryFiles(ctx, sessionID, drivers, cars, map[string][]posSample{}, timing, laps); err != nil {
		t.Fatalf("writeTelemetryFiles: %v", err)
	}

	row := mustLoadTelemetryRow(t, ctx, st, year, round, sessionType, "AAA", 2)
	speed := toFloatSlice(row["speed"])
	gear := toIntSlice(row["gear"])
	if len(speed) == 0 || len(gear) == 0 {
		t.Fatalf("expected speed and gear telemetry arrays, got speed=%d gear=%d", len(speed), len(gear))
	}
	maxSpeed := 0.0
	for _, v := range speed {
		if v > maxSpeed {
			maxSpeed = v
		}
	}
	if maxSpeed < 250 {
		t.Fatalf("expected high-speed best-lap telemetry from window, got max speed %.1f", maxSpeed)
	}
	minGear := math.MaxInt32
	for _, g := range gear {
		if g < minGear {
			minGear = g
		}
	}
	if minGear <= 1 {
		t.Fatalf("expected window telemetry gears, got suspicious minimum gear %d", minGear)
	}
}

func TestWriteTelemetryFilesFallsBackToLapAtWhenLapWindowsUnusable(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	defer st.Close()
	ctx := context.Background()

	const year, round = 2026, 78
	const sessionType = "Q"
	sessionID, err := st.UpsertSessionInfo(ctx, year, round, sessionType, map[string]any{
		"event_name": "Fallback GP",
		"circuit":    "Test Circuit",
		"country":    "Test",
		"drivers": []any{
			map[string]any{
				"abbreviation":  "AAA",
				"driver_number": "1",
				"full_name":     "Driver A",
				"team_name":     "Team A",
				"team_color":    "#FFFFFF",
			},
		},
	})
	if err != nil {
		t.Fatalf("upsert session info: %v", err)
	}

	p := &GoSessionProcessor{
		store:                 st,
		telemetryChunkSamples: 512,
		telemetryChunkCodec:   storage.CodecProtobuf,
	}
	drivers := []driverMeta{{Number: "1", Abbr: "AAA", Team: "Team A", Color: "#FFFFFF"}}
	timing := map[string][]timingState{
		"1": {
			{T: 0, Lap: 1},
			{T: 10, Lap: 2},
			{T: 20, Lap: 3},
		},
	}
	cars := map[string][]carSample{
		"1": {
			{T: 11, Speed: 210, Throttle: 95, Brake: false, Gear: 7, RPM: 11200, DRS: 10},
			{T: 13, Speed: 220, Throttle: 98, Brake: false, Gear: 8, RPM: 11800, DRS: 12},
		},
	}
	// Unusable window (no overlap with sample timestamps) should trigger lapAt fallback.
	laps := []map[string]any{
		{
			"driver":     "AAA",
			"lap_number": 2,
			"lap_time":   "0:10.000",
			"time":       200.0,
		},
	}

	if err := p.writeTelemetryFiles(ctx, sessionID, drivers, cars, map[string][]posSample{}, timing, laps); err != nil {
		t.Fatalf("writeTelemetryFiles: %v", err)
	}

	row := mustLoadTelemetryRow(t, ctx, st, year, round, sessionType, "AAA", 2)
	speed := toFloatSlice(row["speed"])
	gear := toIntSlice(row["gear"])
	if len(speed) == 0 || len(gear) == 0 {
		t.Fatalf("expected fallback telemetry arrays, got speed=%d gear=%d", len(speed), len(gear))
	}
	if speed[0] < 200 {
		t.Fatalf("expected lapAt fallback speed >= 200, got %.1f", speed[0])
	}
	if gear[0] < 7 {
		t.Fatalf("expected lapAt fallback high gear, got %d", gear[0])
	}
}

func TestQ3BestLapWindowIsExposedAndTelemetryAligns(t *testing.T) {
	drivers := []driverMeta{
		{Number: "4", Abbr: "NOR", Team: "McLaren", Color: "FF8000"},
	}
	byNum := map[string]driverMeta{
		"4": drivers[0],
	}
	laps := []map[string]any{
		{
			"driver":           "NOR",
			"lap_number":       1,
			"lap_time":         "0:12.000",
			"time":             12.0,
			"qualifying_phase": "Q3",
			"sector1":          "4.000",
			"sector2":          "4.000",
			"sector3":          "4.000",
		},
		{
			"driver":           "NOR",
			"lap_number":       2,
			"lap_time":         "0:10.000",
			"time":             22.0,
			"qualifying_phase": "Q3",
			"sector1":          "3.400",
			"sector2":          "3.300",
			"sector3":          "3.300",
		},
	}

	buildLap := func(startT, endT float64) []posSample {
		out := make([]posSample, 0, 180)
		total := 180
		for i := 0; i < total; i++ {
			ratio := float64(i) / float64(total-1)
			theta := ratio * 2 * math.Pi
			out = append(out, posSample{
				T: startT + ratio*(endT-startT),
				X: math.Cos(theta),
				Y: math.Sin(theta),
			})
		}
		return out
	}

	pos := map[string][]posSample{
		"4": buildLap(12.0, 22.0),
	}
	timingForQ3 := map[string][]timingState{
		"4": {
			{T: 12.0, Lap: 1},
			{T: 22.0, Lap: 2},
		},
	}
	track := map[string]any{
		"norm": map[string]any{
			"x_min": 0.0,
			"y_min": 0.0,
			"scale": 1.0,
		},
	}

	out := buildQ3LinesJSON(drivers, byNum, laps, pos, timingForQ3, track)
	if out == nil {
		t.Fatalf("expected q3 lines output")
	}
	rows, ok := out["drivers"].([]map[string]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("expected one q3 line row, got %#v", out["drivers"])
	}
	row := rows[0]
	if asInt(row["lap_number"]) != 2 {
		t.Fatalf("expected Q3 best lap number 2, got %v", row["lap_number"])
	}
	if math.Abs(asFloat(row["lap_start_ts"], -1)-12.0) > 1e-6 {
		t.Fatalf("expected lap_start_ts=12.0, got %v", row["lap_start_ts"])
	}
	if math.Abs(asFloat(row["lap_end_ts"], -1)-22.0) > 1e-6 {
		t.Fatalf("expected lap_end_ts=22.0, got %v", row["lap_end_ts"])
	}

	dir := t.TempDir()
	st := newTestStore(t, dir)
	defer st.Close()
	ctx := context.Background()
	const year, round = 2026, 79
	const sessionType = "Q"
	sessionID, err := st.UpsertSessionInfo(ctx, year, round, sessionType, map[string]any{
		"event_name": "Q3 Alignment GP",
		"circuit":    "Test Circuit",
		"country":    "Test",
		"drivers": []any{
			map[string]any{
				"abbreviation":  "NOR",
				"driver_number": "4",
				"full_name":     "Lando Norris",
				"team_name":     "McLaren",
				"team_color":    "#FF8000",
			},
		},
	})
	if err != nil {
		t.Fatalf("upsert session info: %v", err)
	}

	p := &GoSessionProcessor{
		store:                 st,
		telemetryChunkSamples: 512,
		telemetryChunkCodec:   storage.CodecProtobuf,
	}
	// Deliberately misaligned timeline where lapAt would classify the true best-lap window as lap 1.
	timingForTelemetry := map[string][]timingState{
		"4": {
			{T: 0, Lap: 1},
			{T: 40, Lap: 2},
			{T: 80, Lap: 3},
		},
	}
	cars := map[string][]carSample{
		"4": {
			// Best-lap window [12,22]
			{T: 13, Speed: 286, Throttle: 99, Brake: false, Gear: 8, RPM: 11800, DRS: 12},
			{T: 16, Speed: 294, Throttle: 100, Brake: false, Gear: 8, RPM: 12100, DRS: 12},
			{T: 20, Speed: 281, Throttle: 97, Brake: false, Gear: 7, RPM: 11600, DRS: 10},
			// Adjacent lap sample that should not replace best-lap telemetry
			{T: 46, Speed: 88, Throttle: 28, Brake: true, Gear: 0, RPM: 6200, DRS: 0},
		},
	}

	if err := p.writeTelemetryFiles(ctx, sessionID, drivers, cars, map[string][]posSample{}, timingForTelemetry, laps); err != nil {
		t.Fatalf("writeTelemetryFiles: %v", err)
	}
	telemetry := mustLoadTelemetryRow(t, ctx, st, year, round, sessionType, "NOR", asInt(row["lap_number"]))
	speed := toFloatSlice(telemetry["speed"])
	gear := toIntSlice(telemetry["gear"])
	if len(speed) == 0 || len(gear) == 0 {
		t.Fatalf("expected telemetry arrays for Q3 best lap")
	}
	maxSpeed := 0.0
	for _, v := range speed {
		if v > maxSpeed {
			maxSpeed = v
		}
	}
	if maxSpeed < 250 {
		t.Fatalf("expected high-speed Q3 best-lap telemetry, got max speed %.1f", maxSpeed)
	}
	minGear := math.MaxInt32
	for _, g := range gear {
		if g < minGear {
			minGear = g
		}
	}
	if minGear <= 1 {
		t.Fatalf("expected no pit/neutral-heavy best-lap telemetry, got minimum gear %d", minGear)
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
	fresh := nearestPosSampleWithin(pos, 2.6, 0.5)
	if fresh == nil || fresh.X != 14 {
		t.Fatalf("nearestPosSampleWithin expected third point")
	}
	stale := nearestPosSampleWithin(pos, 100.0, 10.0)
	if stale != nil {
		t.Fatalf("nearestPosSampleWithin expected nil for stale sample")
	}

	interp := interpolatePosSampleAt(pos, 2.5, 1.0)
	if interp == nil {
		t.Fatalf("interpolatePosSampleAt expected interpolated sample")
	}
	if math.Abs(interp.T-2.5) > 1e-9 || math.Abs(interp.X-13.0) > 1e-9 || math.Abs(interp.Y-23.0) > 1e-9 {
		t.Fatalf("unexpected interpolated sample: %+v", interp)
	}

	tooFar := interpolatePosSampleAt(pos, 2.5, 0.4)
	if tooFar != nil {
		t.Fatalf("interpolatePosSampleAt expected nil when boundary gap exceeds limit")
	}

	car := []carSample{
		{T: 1.0, Speed: 100, Throttle: 20, Brake: false, Gear: 3, RPM: 9000, DRS: 8},
		{T: 3.0, Speed: 200, Throttle: 60, Brake: true, Gear: 6, RPM: 12000, DRS: 10},
	}
	carInterp := interpolateCarSampleAt(car, 2.0, 1.5)
	if carInterp == nil {
		t.Fatalf("interpolateCarSampleAt expected interpolated sample")
	}
	if math.Abs(carInterp.Speed-150) > 1e-9 || math.Abs(carInterp.Throttle-40) > 1e-9 || math.Abs(carInterp.RPM-10500) > 1e-9 {
		t.Fatalf("unexpected interpolated numeric fields: %+v", carInterp)
	}
	// Discrete channels use the previous sample to avoid flicker.
	if carInterp.Brake != false || carInterp.Gear != 3 || carInterp.DRS != 8 {
		t.Fatalf("unexpected interpolated discrete fields: %+v", carInterp)
	}
	carTooFar := interpolateCarSampleAt(car, 2.0, 0.9)
	if carTooFar != nil {
		t.Fatalf("interpolateCarSampleAt expected nil when boundary gap exceeds limit")
	}
}

func TestNewGoSessionProcessorReplayInterpMaxGapConfig(t *testing.T) {
	t.Setenv("REPLAY_INTERP_MAX_GAP_SECONDS", "0.05")
	p := NewGoSessionProcessor(t.TempDir(), nil, 256, 512)
	if math.Abs(p.replayInterpMaxGap-0.1) > 1e-9 {
		t.Fatalf("expected lower clamp 0.1, got %.3f", p.replayInterpMaxGap)
	}

	t.Setenv("REPLAY_INTERP_MAX_GAP_SECONDS", "5")
	p = NewGoSessionProcessor(t.TempDir(), nil, 256, 512)
	if math.Abs(p.replayInterpMaxGap-2.0) > 1e-9 {
		t.Fatalf("expected upper clamp 2.0, got %.3f", p.replayInterpMaxGap)
	}

	t.Setenv("REPLAY_INTERP_MAX_GAP_SECONDS", "0.7")
	p = NewGoSessionProcessor(t.TempDir(), nil, 256, 512)
	if math.Abs(p.replayInterpMaxGap-0.7) > 1e-9 {
		t.Fatalf("expected configured value 0.7, got %.3f", p.replayInterpMaxGap)
	}
}

func TestWriteReplayFromTimelinesInterpolatesPositionAndCarData(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	defer st.Close()
	ctx := context.Background()

	sessionID, err := st.UpsertSessionInfo(ctx, 2026, 99, "R", map[string]any{
		"event_name": "Test GP",
		"circuit":    "Test Circuit",
		"country":    "Test",
		"drivers": []any{
			map[string]any{
				"abbreviation":  "AAA",
				"driver_number": "1",
				"full_name":     "Driver A",
				"team_name":     "Team A",
				"team_color":    "#FFFFFF",
			},
		},
	})
	if err != nil {
		t.Fatalf("upsert session info: %v", err)
	}

	p := &GoSessionProcessor{
		store:              st,
		sampleEvery:        0.5,
		replayInterpMaxGap: 0.6,
		replayChunkFrames:  16,
		replayChunkCodec:   storage.CodecProtobuf,
	}
	drivers := []driverMeta{{Number: "1", Abbr: "AAA", Team: "Team A", Color: "#FFFFFF"}}
	timing := map[string][]timingState{
		"1": {
			{T: 0.0, Lap: 1, Position: 1},
			{T: 1.0, Lap: 1, Position: 1},
		},
	}
	positions := map[string][]posSample{
		"1": {
			{T: 0.0, X: 10, Y: 5},
			{T: 1.0, X: 20, Y: 5},
		},
	}
	cars := map[string][]carSample{
		"1": {
			{T: 0.0, Speed: 100, Throttle: 20, Brake: false, Gear: 3, RPM: 9000, DRS: 8},
			{T: 1.0, Speed: 200, Throttle: 60, Brake: true, Gear: 6, RPM: 12000, DRS: 10},
		},
	}

	index, err := p.writeReplayFromTimelines(ctx, sessionID, drivers, timing, positions, cars, nil, nil, nil, "R", nil, map[string][]map[string]any{})
	if err != nil {
		t.Fatalf("writeReplayFromTimelines: %v", err)
	}
	frames, ok := index["frames"].([]map[string]any)
	if !ok || len(frames) == 0 {
		t.Fatalf("missing frame index data")
	}
	chunkSeq := asInt(frames[0]["chunk_seq"])
	payload, codec, err := st.GetReplayChunkPayload(ctx, sessionID, chunkSeq)
	if err != nil {
		t.Fatalf("get replay chunk: %v", err)
	}
	decoded, err := storage.DecodeReplayChunk(payload, codec)
	if err != nil {
		t.Fatalf("decode replay chunk: %v", err)
	}

	var mid map[string]any
	for _, fr := range decoded.Frames {
		if fr.TimestampMs != 500 {
			continue
		}
		if err := json.Unmarshal(fr.FrameJson, &mid); err != nil {
			t.Fatalf("unmarshal mid frame: %v", err)
		}
		break
	}
	if mid == nil {
		t.Fatalf("expected frame at 500ms")
	}
	driversAny, ok := mid["drivers"].([]any)
	if !ok || len(driversAny) != 1 {
		t.Fatalf("unexpected drivers payload: %#v", mid["drivers"])
	}
	row, ok := driversAny[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected driver row type: %#v", driversAny[0])
	}

	if math.Abs(asFloat(row["x"], -1)-0.5) > 1e-6 {
		t.Fatalf("expected interpolated x=0.5, got %v", row["x"])
	}
	if math.Abs(asFloat(row["speed"], -1)-150.0) > 1e-6 {
		t.Fatalf("expected interpolated speed=150, got %v", row["speed"])
	}
	if math.Abs(asFloat(row["throttle"], -1)-40.0) > 1e-6 {
		t.Fatalf("expected interpolated throttle=40, got %v", row["throttle"])
	}
	if math.Abs(asFloat(row["rpm"], -1)-10500.0) > 1e-6 {
		t.Fatalf("expected interpolated rpm=10500, got %v", row["rpm"])
	}
	if asInt(row["gear"]) != 3 {
		t.Fatalf("expected previous gear=3, got %v", row["gear"])
	}
	if asInt(row["drs"]) != 8 {
		t.Fatalf("expected previous drs=8, got %v", row["drs"])
	}
	if asBool(row["brake"]) {
		t.Fatalf("expected previous brake=false, got true")
	}
}

func TestWriteReplayFromTimelinesHonorsInterpolationGapGuardWithFallback(t *testing.T) {
	dir := t.TempDir()
	st := newTestStore(t, dir)
	defer st.Close()
	ctx := context.Background()

	sessionID, err := st.UpsertSessionInfo(ctx, 2026, 98, "R", map[string]any{
		"event_name": "Test GP",
		"circuit":    "Test Circuit",
		"country":    "Test",
		"drivers": []any{
			map[string]any{
				"abbreviation":  "AAA",
				"driver_number": "1",
				"full_name":     "Driver A",
				"team_name":     "Team A",
				"team_color":    "#FFFFFF",
			},
		},
	})
	if err != nil {
		t.Fatalf("upsert session info: %v", err)
	}

	p := &GoSessionProcessor{
		store:              st,
		sampleEvery:        1.0,
		replayInterpMaxGap: 0.4,
		replayChunkFrames:  16,
		replayChunkCodec:   storage.CodecProtobuf,
	}
	drivers := []driverMeta{{Number: "1", Abbr: "AAA", Team: "Team A", Color: "#FFFFFF"}}
	timing := map[string][]timingState{
		"1": {
			{T: 0.0, Lap: 1, Position: 1},
			{T: 2.0, Lap: 1, Position: 1},
		},
	}
	positions := map[string][]posSample{
		"1": {
			{T: 0.0, X: 10, Y: 5},
			{T: 2.0, X: 20, Y: 5},
		},
	}
	cars := map[string][]carSample{
		"1": {
			{T: 0.0, Speed: 100, Throttle: 20, Brake: false, Gear: 3, RPM: 9000, DRS: 8},
			{T: 2.0, Speed: 200, Throttle: 60, Brake: true, Gear: 6, RPM: 12000, DRS: 10},
		},
	}

	index, err := p.writeReplayFromTimelines(ctx, sessionID, drivers, timing, positions, cars, nil, nil, nil, "R", nil, map[string][]map[string]any{})
	if err != nil {
		t.Fatalf("writeReplayFromTimelines: %v", err)
	}
	frames, ok := index["frames"].([]map[string]any)
	if !ok || len(frames) == 0 {
		t.Fatalf("missing frame index data")
	}
	chunkSeq := asInt(frames[0]["chunk_seq"])
	payload, codec, err := st.GetReplayChunkPayload(ctx, sessionID, chunkSeq)
	if err != nil {
		t.Fatalf("get replay chunk: %v", err)
	}
	decoded, err := storage.DecodeReplayChunk(payload, codec)
	if err != nil {
		t.Fatalf("decode replay chunk: %v", err)
	}

	var mid map[string]any
	for _, fr := range decoded.Frames {
		if fr.TimestampMs != 1000 {
			continue
		}
		if err := json.Unmarshal(fr.FrameJson, &mid); err != nil {
			t.Fatalf("unmarshal mid frame: %v", err)
		}
		break
	}
	if mid == nil {
		t.Fatalf("expected frame at 1000ms")
	}
	driversAny, ok := mid["drivers"].([]any)
	if !ok || len(driversAny) != 1 {
		t.Fatalf("unexpected drivers payload: %#v", mid["drivers"])
	}
	row, ok := driversAny[0].(map[string]any)
	if !ok {
		t.Fatalf("unexpected driver row type: %#v", driversAny[0])
	}

	// With max gap 0.4 and boundary distance 1.0s, interpolation is suppressed,
	// but replay falls back to nearest/last sample instead of dropping to zero.
	if math.Abs(asFloat(row["speed"], -1)-100.0) > 1e-6 {
		t.Fatalf("expected speed fallback to previous sample 100, got %v", row["speed"])
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

func TestTyreStateForLapUsesTotalLaps(t *testing.T) {
	// Mirrors TimingAppData format seen in 2026 R1:
	// - first stint total 12 laps (MEDIUM)
	// - second stint total 46 laps (HARD)
	stints := []map[string]any{
		{"LapNumber": 11.0, "Compound": "MEDIUM", "TotalLaps": 12.0, "StartLaps": 0.0},
		{"LapNumber": 55.0, "Compound": "HARD", "TotalLaps": 46.0, "StartLaps": 0.0},
	}

	compound44, life44, _, pitStops44 := tyreStateForLap(stints, 44)
	if asString(compound44) != "HARD" {
		t.Fatalf("lap 44 compound: got %v want HARD", compound44)
	}
	if asInt(life44) != 32 {
		t.Fatalf("lap 44 tyre life: got %v want 32", life44)
	}
	if pitStops44 != 1 {
		t.Fatalf("lap 44 pit stops: got %d want 1", pitStops44)
	}

	compound12, life12, _, _ := tyreStateForLap(stints, 12)
	if asString(compound12) != "MEDIUM" || asInt(life12) != 12 {
		t.Fatalf("lap 12 expected MEDIUM/12 got %v/%v", compound12, life12)
	}
	compound13, life13, _, _ := tyreStateForLap(stints, 13)
	if asString(compound13) != "HARD" || asInt(life13) != 1 {
		t.Fatalf("lap 13 expected HARD/1 got %v/%v", compound13, life13)
	}
}

func TestApplyNoCarDataFallbackRaceAfterTenSeconds(t *testing.T) {
	row := map[string]any{
		"x":         0.45,
		"y":         0.33,
		"position":  5,
		"retired":   false,
		"no_timing": false,
		"gap":       "+12.000",
		"interval":  "+1.200",
	}
	applyNoCarDataFallback(row, "R", 12.0)
	if asBool(row["retired"]) != true {
		t.Fatalf("expected retired=true")
	}
	if asBool(row["no_timing"]) != false {
		t.Fatalf("expected no_timing=false for race fallback")
	}
	if row["position"] != nil {
		t.Fatalf("expected position=nil")
	}
	if asFloat(row["x"], -1) != 0 || asFloat(row["y"], -1) != 0 {
		t.Fatalf("expected x/y reset to zero")
	}
}

func TestApplyNoCarDataFallbackRaceBeforeTenSeconds(t *testing.T) {
	row := map[string]any{
		"retired":   false,
		"no_timing": false,
	}
	applyNoCarDataFallback(row, "R", 5.0)
	if asBool(row["retired"]) != false {
		t.Fatalf("expected retired=false before 10s")
	}
	if asBool(row["no_timing"]) != true {
		t.Fatalf("expected no_timing=true before 10s")
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

func TestExtractTimingAppStintsSupportsMapFormat(t *testing.T) {
	timingApp := map[string]any{
		"Lines": map[string]any{
			"63": map[string]any{
				"RacingNumber": "63",
				"Stints": map[string]any{
					"1": map[string]any{"LapNumber": 10.0, "Compound": "MEDIUM"},
					"0": map[string]any{"LapNumber": 1.0, "Compound": "SOFT"},
				},
			},
		},
	}
	byNum := map[string]driverMeta{
		"63": {Abbr: "RUS"},
	}
	stints := extractTimingAppStints(timingApp, byNum)
	got := stints["RUS"]
	if len(got) != 2 {
		t.Fatalf("expected 2 stints, got %d", len(got))
	}
	if asInt(got[0]["LapNumber"]) != 1 || asString(got[0]["Compound"]) != "SOFT" {
		t.Fatalf("expected first stint to be lap 1 soft, got %#v", got[0])
	}
	if asInt(got[1]["LapNumber"]) != 10 || asString(got[1]["Compound"]) != "MEDIUM" {
		t.Fatalf("expected second stint to be lap 10 medium, got %#v", got[1])
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
	meta, name, err := p.resolveCircuitMetadata(sessionInfo)
	if err != nil {
		t.Fatalf("expected graceful fallback metadata, got err=%v", err)
	}
	if name != "Shanghai" {
		t.Fatalf("expected circuit name fallback, got %q", name)
	}
	if meta.CircuitKey != 49 {
		t.Fatalf("expected circuit key 49, got %d", meta.CircuitKey)
	}
	if len(meta.Corners) != 0 || len(meta.MarshalSectors) != 0 {
		t.Fatalf("expected empty markers for fallback metadata")
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
	st := newTestStore(t, dir)
	defer st.Close()
	p := NewGoSessionProcessor(dir, st, 256, 512)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := p.EnsureSchedule(ctx, 2026); err != nil {
		t.Fatalf("EnsureSchedule failed: %v", err)
	}
	_, err := st.GetJSONArtifact(ctx, filepath.ToSlash(filepath.Join("seasons", "2026", "schedule.json")))
	if err != nil {
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
	st := newTestStore(t, dir)
	defer st.Close()
	p := NewGoSessionProcessor(dir, st, 256, 512)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	if err := p.ProcessSession(ctx, year, round, sessionType, nil); err != nil {
		t.Fatalf("ProcessSession failed: %v", err)
	}
	required := []string{
		filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "info.json"),
		filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "track.json"),
		filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "laps.json"),
		filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "results.json"),
		filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "replay.index.json"),
	}
	for _, pth := range required {
		if _, err := st.GetJSONArtifact(ctx, filepath.ToSlash(pth)); err != nil {
			t.Fatalf("required artifact missing %s: %v", pth, err)
		}
	}
	trackPath := filepath.ToSlash(filepath.Join("sessions", strconv.Itoa(year), strconv.Itoa(round), sessionType, "track.json"))
	raw, err := st.GetJSONArtifact(ctx, trackPath)
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
	meta, err := st.LoadReplayMeta(ctx, year, round, sessionType)
	if err != nil {
		t.Fatalf("expected replay meta in sqlite: %v", err)
	}
	if len(meta.Frames) == 0 {
		t.Fatalf("expected replay frames in sqlite")
	}
}

func newTestStore(t *testing.T, dir string) *storage.Store {
	t.Helper()
	dbPath := filepath.Join(dir, "test.db")
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	st, err := storage.Open(ctx, dbPath, 5000, migrationFiles)
	if err != nil {
		t.Fatalf("open sqlite store: %v", err)
	}
	return st
}

func mustLoadTelemetryRow(t *testing.T, ctx context.Context, st *storage.Store, year, round int, sessionType, driver string, lap int) map[string]any {
	t.Helper()
	payload, codec, err := st.GetTelemetryPayload(ctx, year, round, sessionType, driver, lap)
	if err != nil {
		t.Fatalf("get telemetry payload for %s lap %d: %v", driver, lap, err)
	}
	decoded, err := storage.DecodeTelemetryChunk(payload, codec)
	if err != nil {
		t.Fatalf("decode telemetry payload for %s lap %d: %v", driver, lap, err)
	}
	var out map[string]any
	if err := json.Unmarshal(decoded.PayloadJson, &out); err != nil {
		t.Fatalf("unmarshal telemetry json for %s lap %d: %v", driver, lap, err)
	}
	return out
}

func toFloatSlice(v any) []float64 {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]float64, 0, len(arr))
	for _, item := range arr {
		if f, ok := item.(float64); ok {
			out = append(out, f)
		}
	}
	return out
}

func toIntSlice(v any) []int {
	arr, ok := v.([]any)
	if !ok {
		return nil
	}
	out := make([]int, 0, len(arr))
	for _, item := range arr {
		if f, ok := item.(float64); ok {
			out = append(out, int(math.Round(f)))
		}
	}
	return out
}
