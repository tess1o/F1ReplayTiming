package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"f1replaytiming/backend/storage"
)

const defaultF1StaticBase = "https://livetiming.formula1.com/static"

type GoSessionProcessor struct {
	dataDir               string
	store                 *storage.Store
	baseURL               string
	httpClient            *http.Client
	sampleEvery           float64
	fetchWorkers          int
	parseWorkers          int
	replayChunkFrames     int
	telemetryChunkSamples int
	rawMinDelta           float64
	replayChunkCodec      string
	telemetryChunkCodec   string
	circuitMetaMu         sync.RWMutex
	circuitMeta           *circuitMetadataIndex
	metaLoadErr           error
}

type seasonIndex struct {
	Year     int             `json:"Year"`
	Meetings []seasonMeeting `json:"Meetings"`
}

type seasonMeeting struct {
	Number       int              `json:"Number"`
	Name         string           `json:"Name"`
	OfficialName string           `json:"OfficialName"`
	Location     string           `json:"Location"`
	Country      meetingCountry   `json:"Country"`
	Circuit      meetingCircuit   `json:"Circuit"`
	Sessions     []meetingSession `json:"Sessions"`
}

type meetingCountry struct {
	Name string `json:"Name"`
}

type meetingCircuit struct {
	ShortName string `json:"ShortName"`
}

type meetingSession struct {
	Name      string `json:"Name"`
	Type      string `json:"Type"`
	Number    *int   `json:"Number"`
	StartDate string `json:"StartDate"`
	EndDate   string `json:"EndDate"`
	GmtOffset string `json:"GmtOffset"`
	Path      string `json:"Path"`
}

type sessionIndex struct {
	Feeds map[string]sessionFeed `json:"Feeds"`
}

type sessionFeed struct {
	KeyFramePath string `json:"KeyFramePath"`
	StreamPath   string `json:"StreamPath"`
}

type driverMeta struct {
	Number string
	Abbr   string
	Name   string
	Team   string
	Color  string
}

type timingState struct {
	T        float64
	Lap      int
	Position int
	Gap      string
	Interval string
	InPit    bool
	PitOut   bool
	Retired  bool
}

type posSample struct {
	T float64
	X float64
	Y float64
}

type carSample struct {
	T        float64
	Speed    float64
	Throttle float64
	Brake    bool
	Gear     int
	RPM      float64
	DRS      int
}

type weatherPoint struct {
	T      float64
	Values map[string]any
}

type trackStatusPoint struct {
	T      float64
	Status string
}

type raceControlPoint struct {
	T       float64
	Message map[string]any
}

type sessionStatusPoint struct {
	T      float64
	Status string
}

type replayWindow struct {
	Start float64
	End   float64
}

type replayWriter struct {
	path       string
	tmpPath    string
	f          *os.File
	offset     int64
	count      int
	frames     []replayFrameMeta
	quali      []map[string]any
	seenPhases map[string]struct{}
	totalTime  float64
	totalLaps  int
}

func NewGoSessionProcessor(dataDir string, store *storage.Store, replayChunkFrames, telemetryChunkSamples int) *GoSessionProcessor {
	baseURL := strings.TrimSpace(os.Getenv("F1_STATIC_BASE_URL"))
	if baseURL == "" {
		baseURL = defaultF1StaticBase
	}
	sampleEvery := 0.5
	if raw := strings.TrimSpace(os.Getenv("REPLAY_SAMPLE_INTERVAL_SECONDS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			sampleEvery = v
		}
	}
	if sampleEvery < 0.25 {
		sampleEvery = 0.25
	}
	fetchWorkers := 3
	if raw := strings.TrimSpace(os.Getenv("PROCESS_FETCH_CONCURRENCY")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			fetchWorkers = v
		}
	}
	parseWorkers := 2
	if raw := strings.TrimSpace(os.Getenv("PROCESS_PARSE_CONCURRENCY")); raw != "" {
		if v, err := strconv.Atoi(raw); err == nil && v > 0 {
			parseWorkers = v
		}
	}
	meta, metaErr := loadCircuitMetadata(embeddedCircuitMetadata)
	if store != nil {
		if raw, err := store.GetJSONArtifact(context.Background(), "circuit_metadata.json"); err == nil {
			if loaded, loadErr := loadCircuitMetadata(raw); loadErr == nil {
				meta = loaded
				metaErr = nil
			} else {
				log.Printf("processor(go): invalid circuit_metadata.json override, using embedded defaults: %v", loadErr)
			}
		}
	}
	if replayChunkFrames <= 0 {
		replayChunkFrames = 256
	}
	if telemetryChunkSamples <= 0 {
		telemetryChunkSamples = 512
	}
	rawMinDelta := 0.10
	if raw := strings.TrimSpace(os.Getenv("PROCESS_RAW_MIN_DT_SECONDS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v >= 0 {
			rawMinDelta = v
		}
	}
	chunkCodec := strings.ToLower(strings.TrimSpace(os.Getenv("PROCESS_CHUNK_CODEC")))
	if chunkCodec == "" {
		chunkCodec = storage.CodecProtobuf
	}
	if chunkCodec != storage.CodecProtobuf && chunkCodec != storage.CodecProtobufZstd {
		chunkCodec = storage.CodecProtobuf
	}

	return &GoSessionProcessor{
		dataDir: dataDir,
		store:   store,
		baseURL: strings.TrimRight(baseURL, "/"),
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		sampleEvery:           sampleEvery,
		fetchWorkers:          fetchWorkers,
		parseWorkers:          parseWorkers,
		replayChunkFrames:     replayChunkFrames,
		telemetryChunkSamples: telemetryChunkSamples,
		rawMinDelta:           rawMinDelta,
		replayChunkCodec:      chunkCodec,
		telemetryChunkCodec:   chunkCodec,
		circuitMeta:           meta,
		metaLoadErr:           metaErr,
	}
}

func (p *GoSessionProcessor) SetCircuitMetadata(raw []byte) error {
	loaded, err := loadCircuitMetadata(raw)
	if err != nil {
		return err
	}
	p.circuitMetaMu.Lock()
	defer p.circuitMetaMu.Unlock()
	p.circuitMeta = loaded
	p.metaLoadErr = nil
	return nil
}
