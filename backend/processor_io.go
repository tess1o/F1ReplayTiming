package main

import (
	"bytes"
	"compress/flate"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	defaultBrowserUserAgent = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/147.0.0.0 Safari/537.36"
	defaultBrowserAccept    = "application/json,text/plain,*/*"
	defaultBrowserLanguage  = "en-US,en;q=0.9"
	defaultBrowserReferer   = "https://www.formula1.com/"
)

func newReplayWriter(absPath string) (*replayWriter, error) {
	if err := os.MkdirAll(filepath.Dir(absPath), 0o755); err != nil {
		return nil, err
	}
	tmp := absPath + ".tmp"
	f, err := os.Create(tmp)
	if err != nil {
		return nil, err
	}
	if _, err := f.Write([]byte("[")); err != nil {
		_ = f.Close()
		return nil, err
	}
	return &replayWriter{
		path:       absPath,
		tmpPath:    tmp,
		f:          f,
		offset:     1,
		frames:     make([]replayFrameMeta, 0, 4096),
		quali:      make([]map[string]any, 0, 3),
		seenPhases: map[string]struct{}{},
	}, nil
}

func (w *replayWriter) writeFrame(frame map[string]any) error {
	encoded, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	if w.count > 0 {
		if _, err := w.f.Write([]byte(",")); err != nil {
			return err
		}
		w.offset++
	}
	start := w.offset
	if _, err := w.f.Write(encoded); err != nil {
		return err
	}
	w.offset += int64(len(encoded))
	end := w.offset
	w.frames = append(w.frames, replayFrameMeta{
		Start:     start,
		End:       end,
		Timestamp: asFloat(frame["timestamp"], 0),
		Lap:       asInt(frame["lap"]),
	})
	w.totalTime = asFloat(frame["timestamp"], 0)
	w.totalLaps = asInt(frame["total_laps"])
	if qp, ok := frame["quali_phase"].(map[string]any); ok {
		phase := strings.TrimSpace(asString(qp["phase"]))
		if phase != "" {
			if _, seen := w.seenPhases[phase]; !seen {
				w.seenPhases[phase] = struct{}{}
				w.quali = append(w.quali, map[string]any{
					"phase":     phase,
					"timestamp": asFloat(frame["timestamp"], 0),
				})
			}
		}
	}
	w.count++
	return nil
}

func (w *replayWriter) commit() error {
	if _, err := w.f.Write([]byte("]")); err != nil {
		return err
	}
	if err := w.f.Close(); err != nil {
		return err
	}
	w.f = nil
	return os.Rename(w.tmpPath, w.path)
}

func (w *replayWriter) closeNoReplace() {
	if w.f != nil {
		_ = w.f.Close()
		w.f = nil
	}
}

func (p *GoSessionProcessor) writeJSONAtomic(rel string, data any) error {
	body, err := json.Marshal(data)
	if err != nil {
		return err
	}
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	rel = filepath.ToSlash(rel)
	if err := p.store.PutJSONArtifact(context.Background(), rel, body); err != nil {
		return err
	}
	log.Printf("processor(go): saved %s (%d bytes)", rel, len(body))
	return nil
}

func (p *GoSessionProcessor) writeJSONObjectItems(rel string, items []struct {
	K string
	V map[string]any
}) error {
	if p.store == nil {
		return errors.New("sqlite store is not configured")
	}
	buf := bytes.NewBuffer(make([]byte, 0, 1024))
	if _, err := buf.Write([]byte("{")); err != nil {
		return err
	}
	for i, it := range items {
		if i > 0 {
			if _, err := buf.Write([]byte(",")); err != nil {
				return err
			}
		}
		kb, _ := json.Marshal(it.K)
		vb, _ := json.Marshal(it.V)
		if _, err := buf.Write(kb); err != nil {
			return err
		}
		if _, err := buf.Write([]byte(":")); err != nil {
			return err
		}
		if _, err := buf.Write(vb); err != nil {
			return err
		}
	}
	if _, err := buf.Write([]byte("}")); err != nil {
		return err
	}
	return p.store.PutJSONArtifact(context.Background(), filepath.ToSlash(rel), buf.Bytes())
}

func (p *GoSessionProcessor) fetchJSON(ctx context.Context, url string, out any) error {
	body, err := p.fetchBytes(ctx, url)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, out)
}

func (p *GoSessionProcessor) fetchJSONWithHeaders(ctx context.Context, url string, headers map[string]string, out any) error {
	resp, err := p.doRequestWithHeaders(ctx, url, headers)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(trimBOM(body), out)
}

func (p *GoSessionProcessor) fetchBytes(ctx context.Context, url string) ([]byte, error) {
	resp, err := p.doRequest(ctx, url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return trimBOM(body), nil
}

func trimBOM(b []byte) []byte {
	return bytes.TrimPrefix(b, []byte("\xef\xbb\xbf"))
}

func (p *GoSessionProcessor) doRequest(ctx context.Context, url string) (*http.Response, error) {
	return p.doRequestWithHeaders(ctx, url, nil)
}

func (p *GoSessionProcessor) doRequestWithHeaders(ctx context.Context, url string, headers map[string]string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	applyDefaultRequestHeaders(req)
	applyRequestHeaders(req, headers)
	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
		_ = resp.Body.Close()
		return nil, fmt.Errorf("http %s -> %d: %s", url, resp.StatusCode, strings.TrimSpace(string(body)))
	}
	return resp, nil
}

func applyDefaultRequestHeaders(req *http.Request) {
	// Some upstream endpoints (notably livetiming.formula1.com behind CloudFront)
	// can block requests that look like non-browser traffic.
	req.Header.Set("User-Agent", defaultBrowserUserAgent)
	req.Header.Set("Accept", defaultBrowserAccept)
	req.Header.Set("Accept-Language", defaultBrowserLanguage)
	req.Header.Set("Referer", defaultBrowserReferer)
}

func applyRequestHeaders(req *http.Request, headers map[string]string) {
	for k, v := range headers {
		if strings.TrimSpace(k) == "" || strings.TrimSpace(v) == "" {
			continue
		}
		req.Header.Set(k, v)
	}
}

func parseStreamTimestamp(raw string) (float64, bool) {
	raw = strings.TrimSpace(raw)
	if len(raw) < 12 {
		return 0, false
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 {
		return 0, false
	}
	h, err1 := strconv.Atoi(parts[0])
	m, err2 := strconv.Atoi(parts[1])
	sec, err3 := strconv.ParseFloat(parts[2], 64)
	if err1 != nil || err2 != nil || err3 != nil {
		return 0, false
	}
	return float64(h*3600+m*60) + sec, true
}

func decodeMaybeZStreamPayload(payload string) ([]byte, error) {
	payload = strings.TrimSpace(payload)
	if payload == "" {
		return nil, errors.New("empty payload")
	}
	// For .z streams payload is a JSON string, not an object.
	if strings.HasPrefix(payload, "{") || strings.HasPrefix(payload, "[") {
		return []byte(payload), nil
	}
	var encoded string
	if strings.HasPrefix(payload, "\"") {
		if err := json.Unmarshal([]byte(payload), &encoded); err != nil {
			return nil, err
		}
	} else {
		encoded = payload
	}
	return decodeZPayload(encoded)
}

func decodeZPayload(encoded string) ([]byte, error) {
	enc := strings.TrimSpace(encoded)
	if enc == "" {
		return nil, errors.New("empty encoded payload")
	}
	data, err := base64.StdEncoding.DecodeString(enc)
	if err != nil {
		return nil, err
	}
	r := flate.NewReader(bytes.NewReader(data))
	defer r.Close()
	return io.ReadAll(r)
}

func round1(v float64) float64 {
	return math.Round(v*10) / 10
}

func round3(v float64) float64 {
	return math.Round(v*1000) / 1000
}

func zeroToNil(v int) any {
	if v <= 0 {
		return nil
	}
	return v
}
