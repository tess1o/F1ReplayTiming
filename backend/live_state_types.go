package main

import (
	"math"
	"strings"
	"sync"
	"time"
)

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
