package main

import (
	"bytes"
	"compress/flate"
	"encoding/base64"
	"testing"
	"time"
)

func TestDecodeLiveTopicPayloadCompressed(t *testing.T) {
	src := []byte(`{"Position":[{"Entries":{"1":{"X":10,"Y":20,"Status":"OnTrack"}}}]}`)
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

	topic, payload, ok := decodeLiveTopicPayload("Position.z", encoded)
	if !ok {
		t.Fatalf("expected payload decode success")
	}
	if topic != "Position" {
		t.Fatalf("unexpected topic: %s", topic)
	}
	if _, ok := payload["Position"]; !ok {
		t.Fatalf("decoded payload missing Position key")
	}
}

func TestLiveStateSessionEnd(t *testing.T) {
	state := newLiveStateManager("R", pitLossValues{}, nil, nil)
	state.Process("SessionStatus", map[string]any{"Status": "Started"}, float64(time.Now().Unix()))
	state.Process("SessionStatus", map[string]any{"Status": "Finished"}, float64(time.Now().Unix()))
	if !state.SessionEnded() {
		t.Fatalf("expected session to be marked ended")
	}
}

func TestLiveStatePracticeGapComputation(t *testing.T) {
	state := newLiveStateManager("Q", pitLossValues{}, nil, nil)
	state.Process("DriverList", map[string]any{
		"1": map[string]any{"Tla": "VER", "TeamName": "Red Bull", "TeamColour": "3671C6"},
		"2": map[string]any{"Tla": "NOR", "TeamName": "McLaren", "TeamColour": "FF8700"},
	}, float64(time.Now().Unix()))
	state.Process("TimingData", map[string]any{
		"Lines": map[string]any{
			"1": map[string]any{"BestLapTime": map[string]any{"Value": "1:31.000"}},
			"2": map[string]any{"BestLapTime": map[string]any{"Value": "1:31.500"}},
		},
	}, float64(time.Now().Unix()))

	frame := state.Frame()
	drivers, _ := frame["drivers"].([]map[string]any)
	if len(drivers) != 2 {
		t.Fatalf("expected 2 drivers in frame, got %d", len(drivers))
	}
	if asString(drivers[0]["abbr"]) != "VER" {
		t.Fatalf("expected VER first, got %v", drivers[0]["abbr"])
	}
	if asString(drivers[1]["gap"]) != "+0.500" {
		t.Fatalf("expected +0.500 gap, got %v", drivers[1]["gap"])
	}
}
