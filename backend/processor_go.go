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
const defaultF1EditorialBase = "https://api.formula1.com"
const defaultF1EditorialAPIKey = "BQ1SiSmLUOsp460VzXBlLrh689kGgYEZ"
const defaultF1EditorialLocale = "en"
const defaultOpenF1Base = "https://api.openf1.org/v1"

type GoSessionProcessor struct {
	dataDir               string
	store                 *storage.Store
	baseURL               string
	editorialBaseURL      string
	editorialAPIKey       string
	editorialLocale       string
	openF1BaseURL         string
	openF1MinInterval     time.Duration
	openF1MaxRetries      int
	openF1ReqMu           sync.Mutex
	openF1LastReqAt       time.Time
	httpClient            *http.Client
	sampleEvery           float64
	replayInterpMaxGap    float64
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

type qualifyingPhasePoint struct {
	T    float64
	Part int
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
	sampleEvery := 0.25
	if raw := strings.TrimSpace(os.Getenv("REPLAY_SAMPLE_INTERVAL_SECONDS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			sampleEvery = v
		}
	}
	if sampleEvery < 0.25 {
		sampleEvery = 0.25
	}
	replayInterpMaxGap := 1.2
	if raw := strings.TrimSpace(os.Getenv("REPLAY_INTERP_MAX_GAP_SECONDS")); raw != "" {
		if v, err := strconv.ParseFloat(raw, 64); err == nil && v > 0 {
			replayInterpMaxGap = v
		}
	}
	if replayInterpMaxGap < 0.1 {
		replayInterpMaxGap = 0.1
	}
	if replayInterpMaxGap > 2.0 {
		replayInterpMaxGap = 2.0
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
	editorialBaseURL := strings.TrimSpace(os.Getenv("F1_EDITORIAL_BASE_URL"))
	if editorialBaseURL == "" {
		editorialBaseURL = defaultF1EditorialBase
	}
	editorialAPIKey := strings.TrimSpace(os.Getenv("F1_EDITORIAL_API_KEY"))
	if editorialAPIKey == "" {
		editorialAPIKey = defaultF1EditorialAPIKey
	}
	editorialLocale := strings.TrimSpace(os.Getenv("F1_EDITORIAL_LOCALE"))
	if editorialLocale == "" {
		editorialLocale = defaultF1EditorialLocale
	}
	openF1BaseURL := strings.TrimSpace(os.Getenv("OPENF1_BASE_URL"))
	if openF1BaseURL == "" {
		openF1BaseURL = defaultOpenF1Base
	}
	openF1MinIntervalMS := readPositiveIntEnv("OPENF1_MIN_REQUEST_INTERVAL_MS", 450)
	openF1MaxRetries := readPositiveIntEnv("OPENF1_MAX_RETRIES", 5)

	return &GoSessionProcessor{
		dataDir:           dataDir,
		store:             store,
		baseURL:           strings.TrimRight(baseURL, "/"),
		editorialBaseURL:  strings.TrimRight(editorialBaseURL, "/"),
		editorialAPIKey:   editorialAPIKey,
		editorialLocale:   editorialLocale,
		openF1BaseURL:     strings.TrimRight(openF1BaseURL, "/"),
		openF1MinInterval: time.Duration(openF1MinIntervalMS) * time.Millisecond,
		openF1MaxRetries:  openF1MaxRetries,
		httpClient: &http.Client{
			Timeout: 120 * time.Second,
		},
		sampleEvery:           sampleEvery,
		replayInterpMaxGap:    replayInterpMaxGap,
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
