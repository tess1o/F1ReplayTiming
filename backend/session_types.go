package main

import "strings"

var sessionNameToType = map[string]string{
	"Race":              "R",
	"Qualifying":        "Q",
	"Sprint":            "S",
	"Sprint Qualifying": "SQ",
	"Sprint Shootout":   "SQ",
	"Practice 1":        "FP1",
	"Practice 2":        "FP2",
	"Practice 3":        "FP3",
}

func normalizeSessionType(raw string) string {
	raw = strings.ToUpper(strings.TrimSpace(raw))
	switch raw {
	case "R", "Q", "S", "SQ", "FP1", "FP2", "FP3":
		return raw
	default:
		return ""
	}
}
