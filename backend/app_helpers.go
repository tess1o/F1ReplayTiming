package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
)

func parseYearRound(w http.ResponseWriter, r *http.Request) (int, int, bool) {
	year, err1 := strconv.Atoi(r.PathValue("year"))
	round, err2 := strconv.Atoi(r.PathValue("round"))
	if err1 != nil || err2 != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"detail": "Invalid path"})
		return 0, 0, false
	}
	return year, round, true
}

func defaultString(v, def string) string {
	if strings.TrimSpace(v) == "" {
		return def
	}
	return v
}

func asString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case fmt.Stringer:
		return x.String()
	default:
		return ""
	}
}

func asInt(v any) int {
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case json.Number:
		i, _ := x.Int64()
		return int(i)
	case string:
		i, _ := strconv.Atoi(x)
		return i
	default:
		return 0
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func asFloat(v any, def float64) float64 {
	switch x := v.(type) {
	case float64:
		return x
	case int:
		return float64(x)
	case json.Number:
		f, _ := x.Float64()
		return f
	case string:
		f, err := strconv.ParseFloat(strings.TrimSpace(x), 64)
		if err == nil {
			return f
		}
	}
	return def
}

func asBool(v any) bool {
	switch x := v.(type) {
	case bool:
		return x
	case string:
		return isTrue(x)
	default:
		return false
	}
}

func parseGap(gap string) (float64, bool) {
	gap = strings.TrimSpace(gap)
	if gap == "" || strings.HasPrefix(gap, "LAP ") {
		return 0, false
	}
	if strings.Contains(strings.ToUpper(gap), "LAP") {
		return 0, false
	}
	gap = strings.TrimPrefix(gap, "+")
	v, err := strconv.ParseFloat(gap, 64)
	if err != nil {
		return 0, false
	}
	return v, true
}

func roundTo(v float64, places int) float64 {
	pow := 1.0
	for i := 0; i < places; i++ {
		pow *= 10
	}
	return float64(int(v*pow+0.5)) / pow
}
