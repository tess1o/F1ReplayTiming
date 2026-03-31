package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

const circuitMetadataVersion = 1

//go:embed circuit_metadata.json
var embeddedCircuitMetadata []byte

type circuitMetadataIndex struct {
	Version  int
	Circuits map[int]circuitMetadataEntry
}

type circuitMetadataEntry struct {
	CircuitKey     int
	CircuitName    string
	Rotation       float64
	Corners        []circuitCornerMeta
	MarshalSectors []circuitMarshalSectorMeta
}

type circuitCornerMeta struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Number int     `json:"number"`
	Letter string  `json:"letter,omitempty"`
	Angle  float64 `json:"angle"`
}

type circuitMarshalSectorMeta struct {
	X      float64 `json:"x"`
	Y      float64 `json:"y"`
	Number int     `json:"number"`
}

type circuitMetadataFile struct {
	Version     int                                 `json:"version"`
	GeneratedAt string                              `json:"generated_at,omitempty"`
	Source      string                              `json:"source,omitempty"`
	Circuits    map[string]circuitMetadataFileEntry `json:"circuits"`
}

type circuitMetadataFileEntry struct {
	CircuitKey     int                        `json:"circuit_key,omitempty"`
	CircuitName    string                     `json:"circuit_name,omitempty"`
	Rotation       float64                    `json:"rotation"`
	Corners        []circuitCornerMeta        `json:"corners"`
	MarshalSectors []circuitMarshalSectorMeta `json:"marshal_sectors"`
}

func loadCircuitMetadata(raw []byte) (*circuitMetadataIndex, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil, fmt.Errorf("circuit metadata payload is empty")
	}
	var file circuitMetadataFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return nil, fmt.Errorf("decode circuit metadata: %w", err)
	}
	if file.Version != circuitMetadataVersion {
		return nil, fmt.Errorf("unsupported circuit metadata version: got %d want %d", file.Version, circuitMetadataVersion)
	}
	if len(file.Circuits) == 0 {
		return nil, fmt.Errorf("circuit metadata has no circuits")
	}

	parsed := &circuitMetadataIndex{
		Version:  file.Version,
		Circuits: make(map[int]circuitMetadataEntry, len(file.Circuits)),
	}

	keys := make([]string, 0, len(file.Circuits))
	for k := range file.Circuits {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, keyStr := range keys {
		entry := file.Circuits[keyStr]
		key, err := strconv.Atoi(strings.TrimSpace(keyStr))
		if err != nil || key <= 0 {
			return nil, fmt.Errorf("invalid circuit key %q in metadata", keyStr)
		}
		if entry.CircuitKey == 0 {
			entry.CircuitKey = key
		}
		if entry.CircuitKey != key {
			return nil, fmt.Errorf("circuit key mismatch for %q: entry=%d", keyStr, entry.CircuitKey)
		}
		if !isFinite(entry.Rotation) {
			return nil, fmt.Errorf("invalid rotation for circuit %d", key)
		}
		if len(entry.Corners) == 0 {
			return nil, fmt.Errorf("missing corners for circuit %d", key)
		}
		if len(entry.MarshalSectors) == 0 {
			return nil, fmt.Errorf("missing marshal sectors for circuit %d", key)
		}
		for i, c := range entry.Corners {
			if c.Number <= 0 || !isFinite(c.X) || !isFinite(c.Y) || !isFinite(c.Angle) {
				return nil, fmt.Errorf("invalid corner %d for circuit %d", i, key)
			}
		}
		for i, s := range entry.MarshalSectors {
			if s.Number <= 0 || !isFinite(s.X) || !isFinite(s.Y) {
				return nil, fmt.Errorf("invalid marshal sector %d for circuit %d", i, key)
			}
		}
		if _, exists := parsed.Circuits[key]; exists {
			return nil, fmt.Errorf("duplicate circuit key %d in metadata", key)
		}
		parsed.Circuits[key] = circuitMetadataEntry{
			CircuitKey:     entry.CircuitKey,
			CircuitName:    strings.TrimSpace(entry.CircuitName),
			Rotation:       entry.Rotation,
			Corners:        append([]circuitCornerMeta(nil), entry.Corners...),
			MarshalSectors: append([]circuitMarshalSectorMeta(nil), entry.MarshalSectors...),
		}
	}
	return parsed, nil
}

func (p *GoSessionProcessor) resolveCircuitMetadata(sessionInfo map[string]any) (circuitMetadataEntry, string, error) {
	if p.metaLoadErr != nil {
		return circuitMetadataEntry{}, "", fmt.Errorf("circuit metadata load failed: %w", p.metaLoadErr)
	}
	if p.circuitMeta == nil || len(p.circuitMeta.Circuits) == 0 {
		return circuitMetadataEntry{}, "", fmt.Errorf("circuit metadata is not initialized")
	}
	circuitKey, circuitName, err := extractCircuitKeyFromSessionInfo(sessionInfo)
	if err != nil {
		return circuitMetadataEntry{}, "", err
	}
	meta, ok := p.circuitMeta.Circuits[circuitKey]
	if !ok {
		return circuitMetadataEntry{}, "", fmt.Errorf("circuit metadata missing for key=%d (%s)", circuitKey, defaultString(circuitName, "unknown"))
	}
	if circuitName == "" {
		circuitName = meta.CircuitName
	}
	return meta, circuitName, nil
}

func extractCircuitKeyFromSessionInfo(sessionInfo map[string]any) (int, string, error) {
	meeting, ok := sessionInfo["Meeting"].(map[string]any)
	if !ok {
		return 0, "", fmt.Errorf("session info missing Meeting object")
	}
	circuit, ok := meeting["Circuit"].(map[string]any)
	if !ok {
		return 0, "", fmt.Errorf("session info missing Meeting.Circuit")
	}
	key := asInt(circuit["Key"])
	name := strings.TrimSpace(asString(circuit["ShortName"]))
	if name == "" {
		name = strings.TrimSpace(asString(meeting["Location"]))
	}
	if name == "" {
		name = strings.TrimSpace(asString(meeting["Name"]))
	}
	if key <= 0 {
		return 0, name, fmt.Errorf("session info has invalid Meeting.Circuit.Key")
	}
	return key, name, nil
}

func isFinite(v float64) bool {
	return !math.IsNaN(v) && !math.IsInf(v, 0)
}
