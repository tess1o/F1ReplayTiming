package main

import (
	"math"
	"sort"
)

func lapAt(tl []timingState, t float64) int {
	s := latestTimingAt(tl, t)
	if s == nil {
		return 0
	}
	return s.Lap
}

func latestTimingAt(tl []timingState, t float64) *timingState {
	if len(tl) == 0 {
		return nil
	}
	i := sort.Search(len(tl), func(i int) bool { return tl[i].T > t }) - 1
	if i < 0 {
		return nil
	}
	return &tl[i]
}

func nearestPosSample(arr []posSample, t float64) *posSample {
	if len(arr) == 0 {
		return nil
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T >= t })
	if i == 0 {
		return &arr[0]
	}
	if i >= len(arr) {
		return &arr[len(arr)-1]
	}
	prev := arr[i-1]
	next := arr[i]
	if math.Abs(prev.T-t) <= math.Abs(next.T-t) {
		return &prev
	}
	return &next
}

func nearestPosSampleWithin(arr []posSample, t, maxDeltaSeconds float64) *posSample {
	ps := nearestPosSample(arr, t)
	if ps == nil {
		return nil
	}
	if maxDeltaSeconds <= 0 {
		return ps
	}
	if math.Abs(ps.T-t) > maxDeltaSeconds {
		return nil
	}
	return ps
}

func latestCarAt(arr []carSample, t float64) *carSample {
	if len(arr) == 0 {
		return nil
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
	if i < 0 {
		return nil
	}
	return &arr[i]
}

func trackStatusAt(arr []trackStatusPoint, t float64) string {
	if len(arr) == 0 {
		return "green"
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
	if i < 0 {
		return "green"
	}
	switch asString(arr[i].Status) {
	case "1", "7":
		return "green"
	case "2":
		return "yellow"
	case "4":
		return "sc"
	case "5":
		return "red"
	case "6":
		return "vsc"
	default:
		return "green"
	}
}

func weatherAt(arr []weatherPoint, t float64) map[string]any {
	if len(arr) == 0 {
		return nil
	}
	i := sort.Search(len(arr), func(i int) bool { return arr[i].T > t }) - 1
	if i < 0 {
		return nil
	}
	return arr[i].Values
}

func raceControlAt(arr []raceControlPoint, t float64) []map[string]any {
	if len(arr) == 0 {
		return nil
	}
	out := make([]map[string]any, 0, 50)
	for i := len(arr) - 1; i >= 0 && len(out) < 50; i-- {
		msgTs := asFloat(arr[i].Message["timestamp"], 0)
		if msgTs <= 0 || msgTs <= t+1e-9 {
			out = append(out, arr[i].Message)
		}
	}
	return out
}
