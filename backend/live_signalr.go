package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strings"
	"time"

	"github.com/gorilla/websocket"
)

const (
	liveSignalRBaseURL  = "https://livetiming.formula1.com/signalrcore"
	liveSignalRWSURL    = "wss://livetiming.formula1.com/signalrcore"
	liveSignalRRecordRS = '\x1e'
)

var liveSignalRTopics = []string{
	"TimingData",
	"TimingAppData",
	"TimingStats",
	"DriverList",
	"RaceControlMessages",
	"TrackStatus",
	"WeatherData",
	"LapCount",
	"ExtrapolatedClock",
	"SessionInfo",
	"SessionStatus",
	"SessionData",
	"Position.z",
}

type liveSignalRClient struct {
	httpClient *http.Client
	dialer     websocket.Dialer
}

func newLiveSignalRClient() *liveSignalRClient {
	jar, _ := cookiejar.New(nil)
	return &liveSignalRClient{
		httpClient: &http.Client{
			Timeout: 15 * time.Second,
			Jar:     jar,
		},
		dialer: websocket.Dialer{
			Proxy:            http.ProxyFromEnvironment,
			HandshakeTimeout: 15 * time.Second,
		},
	}
}

func (c *liveSignalRClient) stream(ctx context.Context, onMessage func(topic string, data map[string]any, ts float64)) error {
	backoff := time.Second
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		err := c.runSession(ctx, onMessage)
		if err == nil || errors.Is(err, io.EOF) {
			backoff = time.Second
		}
		if err := ctx.Err(); err != nil {
			return nil
		}
		wait := time.NewTimer(backoff)
		select {
		case <-ctx.Done():
			wait.Stop()
			return nil
		case <-wait.C:
		}
		if backoff < 30*time.Second {
			backoff *= 2
			if backoff > 30*time.Second {
				backoff = 30 * time.Second
			}
		}
	}
}

func (c *liveSignalRClient) runSession(ctx context.Context, onMessage func(topic string, data map[string]any, ts float64)) error {
	token, cookie, err := c.negotiate(ctx)
	if err != nil {
		return err
	}

	wsURL := liveSignalRWSURL + "?id=" + url.QueryEscape(token)
	header := http.Header{}
	if cookie != "" {
		header.Set("Cookie", cookie)
	}
	ws, _, err := c.dialer.DialContext(ctx, wsURL, header)
	if err != nil {
		return err
	}
	defer ws.Close()
	closeOnCancelDone := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			_ = ws.Close()
		case <-closeOnCancelDone:
		}
	}()
	defer close(closeOnCancelDone)

	if err := c.writeSignalR(ws, map[string]any{"protocol": "json", "version": 1}); err != nil {
		return err
	}
	_, hsRaw, err := ws.ReadMessage()
	if err != nil {
		return err
	}
	if err := validateSignalRHandshake(hsRaw); err != nil {
		return err
	}
	if err := c.writeSignalR(ws, map[string]any{
		"type":         1,
		"invocationId": "1",
		"target":       "Subscribe",
		"arguments":    []any{liveSignalRTopics},
	}); err != nil {
		return err
	}

	buf := ""
	for {
		if err := ctx.Err(); err != nil {
			return nil
		}
		_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		_, payload, err := ws.ReadMessage()
		if err != nil {
			var netErr interface{ Timeout() bool }
			if errors.As(err, &netErr) && netErr.Timeout() {
				_ = c.writeSignalR(ws, map[string]any{"type": 6})
				continue
			}
			return err
		}
		recvTS := float64(time.Now().UnixNano()) / 1e9
		buf += string(payload)
		for {
			idx := strings.IndexByte(buf, liveSignalRRecordRS)
			if idx < 0 {
				break
			}
			msgRaw := strings.TrimSpace(buf[:idx])
			buf = buf[idx+1:]
			if msgRaw == "" {
				continue
			}
			var msg map[string]any
			if err := json.Unmarshal([]byte(msgRaw), &msg); err != nil {
				continue
			}
			if err := c.handleSignalRMessage(ctx, ws, msg, recvTS, onMessage); err != nil {
				if errors.Is(err, io.EOF) {
					return nil
				}
				return err
			}
		}
	}
}

func (c *liveSignalRClient) negotiate(ctx context.Context) (connectionToken string, cookie string, err error) {
	negotiateURL := liveSignalRBaseURL + "/negotiate?negotiateVersion=1"

	reqOptions, _ := http.NewRequestWithContext(ctx, http.MethodOptions, negotiateURL, nil)
	reqOptions.Header.Set("Accept", "*/*")
	_, _ = c.httpClient.Do(reqOptions)

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, negotiateURL, strings.NewReader(""))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 2048))
		return "", "", fmt.Errorf("signalr negotiate failed: %d %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}
	var out struct {
		ConnectionToken string `json:"connectionToken"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return "", "", err
	}
	if strings.TrimSpace(out.ConnectionToken) == "" {
		return "", "", errors.New("signalr negotiate returned empty connection token")
	}

	baseURL, _ := url.Parse(liveSignalRBaseURL)
	if c.httpClient.Jar != nil && baseURL != nil {
		for _, ck := range c.httpClient.Jar.Cookies(baseURL) {
			if ck.Name == "AWSALBCORS" {
				cookie = ck.Name + "=" + ck.Value
				break
			}
		}
	}
	return out.ConnectionToken, cookie, nil
}

func validateSignalRHandshake(raw []byte) error {
	parts := strings.Split(string(raw), string(liveSignalRRecordRS))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		var hs map[string]any
		if err := json.Unmarshal([]byte(p), &hs); err != nil {
			return err
		}
		if errMsg := strings.TrimSpace(asString(hs["error"])); errMsg != "" {
			return fmt.Errorf("signalr handshake error: %s", errMsg)
		}
	}
	return nil
}

func (c *liveSignalRClient) writeSignalR(ws *websocket.Conn, payload map[string]any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	raw = append(raw, byte(liveSignalRRecordRS))
	return ws.WriteMessage(websocket.TextMessage, raw)
}

func (c *liveSignalRClient) handleSignalRMessage(
	ctx context.Context,
	ws *websocket.Conn,
	msg map[string]any,
	recvTS float64,
	onMessage func(topic string, data map[string]any, ts float64),
) error {
	_ = ctx
	msgType := asInt(msg["type"])
	switch msgType {
	case 6: // ping
		_ = c.writeSignalR(ws, map[string]any{"type": 6})
		return nil
	case 7: // close
		return io.EOF
	case 1: // invocation
		target := asString(msg["target"])
		args, _ := msg["arguments"].([]any)
		if target == "" {
			return nil
		}
		if target == "feed" {
			if len(args) >= 2 {
				if topic, ok := args[0].(string); ok {
					c.dispatchLiveTopic(topic, args[1], recvTS, onMessage)
					return nil
				}
			}
			if len(args) >= 1 {
				if root, ok := args[0].(map[string]any); ok {
					for topic, payload := range root {
						c.dispatchLiveTopic(topic, payload, recvTS, onMessage)
					}
				}
			}
			return nil
		}
		if len(args) == 0 {
			return nil
		}
		c.dispatchLiveTopic(target, args[0], recvTS, onMessage)
		return nil
	case 2: // stream item (can include Position.z)
		item := msg["item"]
		switch v := item.(type) {
		case map[string]any:
			onMessage("Position", v, recvTS)
		case string:
			dec, err := decodeZPayload(v)
			if err != nil {
				return nil
			}
			var out map[string]any
			if err := json.Unmarshal(dec, &out); err == nil {
				onMessage("Position", out, recvTS)
			}
		}
		return nil
	case 3: // completion (initial snapshot)
		result, _ := msg["result"].(map[string]any)
		for topic, payload := range result {
			c.dispatchLiveTopic(topic, payload, recvTS, onMessage)
		}
		return nil
	default:
		return nil
	}
}

func (c *liveSignalRClient) dispatchLiveTopic(topic string, payload any, ts float64, onMessage func(topic string, data map[string]any, ts float64)) {
	decodedTopic, decodedData, ok := decodeLiveTopicPayload(topic, payload)
	if !ok {
		return
	}
	onMessage(decodedTopic, decodedData, ts)
}

func decodeLiveTopicPayload(topic string, payload any) (string, map[string]any, bool) {
	effectiveTopic := topic
	switch v := payload.(type) {
	case map[string]any:
		return stripTopicZ(effectiveTopic), v, true
	case string:
		if strings.HasSuffix(effectiveTopic, ".z") {
			dec, err := decodeZPayload(v)
			if err != nil {
				return "", nil, false
			}
			var out map[string]any
			if err := json.Unmarshal(dec, &out); err != nil {
				return "", nil, false
			}
			return stripTopicZ(effectiveTopic), out, true
		}
		var out map[string]any
		if err := json.Unmarshal([]byte(v), &out); err != nil {
			return "", nil, false
		}
		return stripTopicZ(effectiveTopic), out, true
	default:
		return "", nil, false
	}
}

func stripTopicZ(topic string) string {
	if strings.HasSuffix(topic, ".z") {
		return strings.TrimSuffix(topic, ".z")
	}
	return topic
}
