package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"net/http"
	"os"
	"strings"
)

func (a *app) withMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		a.applyCORS(w, r)
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		if strings.HasPrefix(r.URL.Path, "/api") && a.authEnabled {
			if r.URL.Path != "/api/auth/status" && r.URL.Path != "/api/auth/login" && r.URL.Path != "/api/health" {
				token := strings.TrimSpace(strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "))
				if !a.verifyToken(token) {
					writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "Unauthorized"})
					return
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}

func (a *app) applyCORS(w http.ResponseWriter, r *http.Request) {
	if len(a.allowedOrigins) == 0 {
		return
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return
	}
	if !a.isAllowedOrigin(origin) {
		return
	}
	w.Header().Set("Access-Control-Allow-Origin", origin)
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Methods", "GET,POST,PUT,PATCH,DELETE,OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Authorization,Content-Type")
}

func buildAllowedOrigins() map[string]struct{} {
	var origins []string
	if o := strings.TrimSpace(os.Getenv("FRONTEND_URL")); o != "" {
		origins = append(origins, o)
	}
	if extra := strings.TrimSpace(os.Getenv("EXTRA_ORIGINS")); extra != "" {
		for _, o := range strings.Split(extra, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				origins = append(origins, o)
			}
		}
	}
	for _, o := range append([]string{}, origins...) {
		if strings.HasPrefix(o, "https://") {
			origins = append(origins, "http://"+strings.TrimPrefix(o, "https://"))
		} else if strings.HasPrefix(o, "http://") && !strings.Contains(o, "localhost") {
			origins = append(origins, "https://"+strings.TrimPrefix(o, "http://"))
		}
	}
	out := make(map[string]struct{})
	for _, o := range origins {
		out[o] = struct{}{}
	}
	return out
}

func (a *app) isAllowedOrigin(origin string) bool {
	if origin == "" {
		return false
	}
	_, ok := a.allowedOrigins[origin]
	return ok
}

func (a *app) wsOriginAllowed(r *http.Request) bool {
	// In same-origin proxy deployments, CORS is not needed and WS requests may be
	// forwarded without explicit origin allow-list config.
	if len(a.allowedOrigins) == 0 {
		return true
	}
	origin := strings.TrimSpace(r.Header.Get("Origin"))
	if origin == "" {
		return true
	}
	return a.isAllowedOrigin(origin)
}

func isTrue(v string) bool {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

func (a *app) makeToken(passphrase string) string {
	h := sha256.Sum256([]byte("f1replay:" + passphrase))
	return hex.EncodeToString(h[:])
}

func (a *app) verifyToken(token string) bool {
	if !a.authEnabled || token == "" || a.authPassphrase == "" {
		return false
	}
	expected := a.makeToken(a.authPassphrase)
	if len(token) != len(expected) {
		return false
	}
	return subtle.ConstantTimeCompare([]byte(token), []byte(expected)) == 1
}

func (a *app) checkWSAuth(r *http.Request) bool {
	if !a.authEnabled {
		return true
	}
	return a.verifyToken(strings.TrimSpace(r.URL.Query().Get("token")))
}
