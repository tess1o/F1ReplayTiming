package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	f1StaticBase = "https://livetiming.formula1.com/static"
	mvAPIBase    = "https://api.multiviewer.app/api/v1/circuits"
)

type seasonIndex struct {
	Meetings []seasonMeeting `json:"Meetings"`
}

type seasonMeeting struct {
	Name         string        `json:"Name"`
	OfficialName string        `json:"OfficialName"`
	Circuit      seasonCircuit `json:"Circuit"`
}

type seasonCircuit struct {
	Key       int    `json:"Key"`
	ShortName string `json:"ShortName"`
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

type circuitMetadataFile struct {
	Version     int                       `json:"version"`
	GeneratedAt string                    `json:"generated_at"`
	Source      string                    `json:"source"`
	Circuits    orderedCircuitMetadataMap `json:"circuits"`
}

type circuitMetadataEntry struct {
	CircuitKey     int              `json:"circuit_key"`
	CircuitName    string           `json:"circuit_name"`
	Rotation       float64          `json:"rotation"`
	Corners        []circuitCorner  `json:"corners"`
	MarshalSectors []circuitMarshal `json:"marshal_sectors"`
}

type circuitCorner struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Number int     `json:"number"`
	Letter string  `json:"letter,omitempty"`
	Angle  float64 `json:"angle"`
}

type circuitMarshal struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Number int     `json:"number"`
}

type orderedCircuitMetadataMap map[string]circuitMetadataEntry

func (m orderedCircuitMetadataMap) MarshalJSON() ([]byte, error) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	var b bytes.Buffer
	b.WriteByte('{')
	for i, k := range keys {
		if i > 0 {
			b.WriteByte(',')
		}
		kb, err := json.Marshal(k)
		if err != nil {
			return nil, err
		}
		vb, err := json.Marshal(m[k])
		if err != nil {
			return nil, err
		}
		b.Write(kb)
		b.WriteByte(':')
		b.Write(vb)
	}
	b.WriteByte('}')
	return b.Bytes(), nil
}

func main() {
	var (
		yearsRaw = flag.String("years", "2024,2025,2026,2027,2028", "comma separated championship years")
		outPath  = flag.String("out", "assets/circuit_metadata.json", "output path")
		timeout  = flag.Duration("timeout", 30*time.Second, "http timeout")
	)
	flag.Parse()

	years, err := parseYears(*yearsRaw)
	if err != nil {
		fatalf("parse -years: %v", err)
	}
	if len(years) == 0 {
		fatalf("no valid years provided")
	}

	client := &http.Client{Timeout: *timeout}
	circuitYear := make(map[int]int)
	for _, year := range years {
		idx, err := fetchSeasonIndex(client, year)
		if err != nil {
			fmt.Printf("warn: skip year %d (%v)\n", year, err)
			continue
		}
		for _, m := range idx.Meetings {
			if isTestingMeeting(m.Name, m.OfficialName) {
				continue
			}
			if m.Circuit.Key <= 0 {
				continue
			}
			if _, exists := circuitYear[m.Circuit.Key]; !exists {
				circuitYear[m.Circuit.Key] = year
			}
		}
	}
	if len(circuitYear) == 0 {
		fatalf("no circuits discovered from provided years")
	}

	keys := make([]int, 0, len(circuitYear))
	for k := range circuitYear {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	circuits := make(orderedCircuitMetadataMap, len(keys))
	for _, key := range keys {
		meta, usedYear, err := fetchCircuitMetadata(client, key, years, circuitYear[key])
		if err != nil {
			fatalf("circuit %d: %v", key, err)
		}
		circuits[strconv.Itoa(key)] = meta
		fmt.Printf("ok: circuit=%d year=%d corners=%d marshal=%d rotation=%.1f\n",
			key, usedYear, len(meta.Corners), len(meta.MarshalSectors), meta.Rotation)
	}

	out := circuitMetadataFile{
		Version:     1,
		GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		Source:      "multiviewer_api_v1+f1_static_index",
		Circuits:    circuits,
	}
	body, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		fatalf("marshal output: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(*outPath), 0o755); err != nil && filepath.Dir(*outPath) != "." {
		fatalf("mkdir output dir: %v", err)
	}
	if err := os.WriteFile(*outPath, body, 0o644); err != nil {
		fatalf("write output: %v", err)
	}
	fmt.Printf("wrote %s (%d bytes, circuits=%d)\n", *outPath, len(body), len(circuits))
}

func fetchSeasonIndex(client *http.Client, year int) (*seasonIndex, error) {
	url := fmt.Sprintf("%s/%d/Index.json", f1StaticBase, year)
	body, err := fetchBytes(client, url)
	if err != nil {
		return nil, err
	}
	var idx seasonIndex
	if err := json.Unmarshal(body, &idx); err != nil {
		return nil, fmt.Errorf("decode season index: %w", err)
	}
	return &idx, nil
}

func fetchCircuitMetadata(client *http.Client, circuitKey int, years []int, preferredYear int) (circuitMetadataEntry, int, error) {
	candidates := orderedCandidateYears(years, preferredYear)
	for _, year := range candidates {
		url := fmt.Sprintf("%s/%d/%d", mvAPIBase, circuitKey, year)
		body, err := fetchBytes(client, url)
		if err != nil {
			continue
		}
		var raw mvCircuitResponse
		if err := json.Unmarshal(body, &raw); err != nil {
			continue
		}
		meta, err := convertCircuitMetadata(raw, circuitKey)
		if err != nil {
			continue
		}
		return meta, year, nil
	}
	return circuitMetadataEntry{}, 0, fmt.Errorf("no valid metadata found via MultiViewer")
}

func convertCircuitMetadata(raw mvCircuitResponse, fallbackKey int) (circuitMetadataEntry, error) {
	key := raw.CircuitKey
	if key <= 0 {
		key = fallbackKey
	}
	if key <= 0 {
		return circuitMetadataEntry{}, fmt.Errorf("missing circuit key")
	}
	if len(raw.Corners) == 0 || len(raw.MarshalSectors) == 0 {
		return circuitMetadataEntry{}, fmt.Errorf("missing corners/marshal sectors")
	}
	corners := make([]circuitCorner, 0, len(raw.Corners))
	for _, c := range raw.Corners {
		if c.Number <= 0 {
			continue
		}
		corners = append(corners, circuitCorner{
			X:      c.TrackPosition.X,
			Y:      c.TrackPosition.Y,
			Number: c.Number,
			Letter: strings.TrimSpace(c.Letter),
			Angle:  c.Angle,
		})
	}
	marshal := make([]circuitMarshal, 0, len(raw.MarshalSectors))
	for _, s := range raw.MarshalSectors {
		if s.Number <= 0 {
			continue
		}
		marshal = append(marshal, circuitMarshal{
			X:      s.TrackPosition.X,
			Y:      s.TrackPosition.Y,
			Number: s.Number,
		})
	}
	if len(corners) == 0 || len(marshal) == 0 {
		return circuitMetadataEntry{}, fmt.Errorf("invalid marker content")
	}
	return circuitMetadataEntry{
		CircuitKey:     key,
		CircuitName:    strings.TrimSpace(raw.CircuitName),
		Rotation:       raw.Rotation,
		Corners:        corners,
		MarshalSectors: marshal,
	}, nil
}

func fetchBytes(client *http.Client, url string) ([]byte, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		return nil, fmt.Errorf("http %d for %s: %s", resp.StatusCode, url, strings.TrimSpace(string(body)))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return trimBOM(body), nil
}

func trimBOM(b []byte) []byte {
	return bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
}

func parseYears(raw string) ([]int, error) {
	parts := strings.Split(raw, ",")
	out := make([]int, 0, len(parts))
	seen := map[int]struct{}{}
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		y, err := strconv.Atoi(p)
		if err != nil {
			return nil, fmt.Errorf("invalid year %q", p)
		}
		if _, ok := seen[y]; ok {
			continue
		}
		seen[y] = struct{}{}
		out = append(out, y)
	}
	sort.Ints(out)
	return out, nil
}

func orderedCandidateYears(years []int, preferred int) []int {
	out := make([]int, 0, len(years))
	seen := map[int]struct{}{}
	if preferred > 0 {
		out = append(out, preferred)
		seen[preferred] = struct{}{}
	}
	for i := len(years) - 1; i >= 0; i-- {
		y := years[i]
		if _, ok := seen[y]; ok {
			continue
		}
		seen[y] = struct{}{}
		out = append(out, y)
	}
	return out
}

func isTestingMeeting(name, official string) bool {
	n := strings.ToLower(name + " " + official)
	return strings.Contains(n, "testing")
}

func fatalf(format string, args ...any) {
	_, _ = fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
