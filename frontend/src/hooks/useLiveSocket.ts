"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import { wsUrl } from "@/lib/api";
import type { ReplayDriver, ReplayFrame, WeatherData, QualiPhase, RCMessage } from "./useReplaySocket";

export { type ReplayDriver, type ReplayFrame, type WeatherData, type QualiPhase, type RCMessage };

interface LiveState {
  connected: boolean;
  ready: boolean;
  loading: boolean;
  frame: ReplayFrame | null;
  rcMessages: RCMessage[];
  finished: boolean;
  sessionEnded: boolean;
  error: string | null;
}

interface BufferedFrame {
  frame: ReplayFrame;
  receivedAt: number; // Date.now() when received
}

export function useLiveSocket(
  year: number,
  round: number,
  sessionType: string = "R",
  speed: number = 10,
  delayOffset: number = 0,
) {
  const wsRef = useRef<WebSocket | null>(null);
  const bufferRef = useRef<BufferedFrame[]>([]);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const [state, setState] = useState<LiveState>({
    connected: false,
    ready: false,
    loading: true,
    frame: null,
    rcMessages: [],
    finished: false,
    sessionEnded: false,
    error: null,
  });

  // Delay processing: when delayOffset > 0, buffer frames and release them
  // after the delay. When delayOffset < 0, skip frames (show "future" = instant).
  // When delayOffset === 0, pass frames through immediately.
  useEffect(() => {
    if (delayOffset <= 0) {
      // No positive delay — clear buffer and timer
      bufferRef.current = [];
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
      return;
    }

    // Positive delay: set up an interval to check buffer and release frames
    const delayMs = delayOffset * 1000;
    timerRef.current = setInterval(() => {
      const now = Date.now();
      const buffer = bufferRef.current;
      // Find the latest frame that's been buffered long enough
      let releaseIdx = -1;
      for (let i = buffer.length - 1; i >= 0; i--) {
        if (now - buffer[i].receivedAt >= delayMs) {
          releaseIdx = i;
          break;
        }
      }
      if (releaseIdx >= 0) {
        const frame = buffer[releaseIdx].frame;
        // Remove released and older frames
        bufferRef.current = buffer.slice(releaseIdx + 1);
        setState((s) => ({ ...s, frame }));
      }
    }, 200);

    return () => {
      if (timerRef.current) {
        clearInterval(timerRef.current);
        timerRef.current = null;
      }
    };
  }, [delayOffset]);

  useEffect(() => {
    let aborted = false;
    const url = wsUrl(`/ws/live/${year}/${round}?type=${sessionType}&speed=${speed}`);
    const ws = new WebSocket(url);
    wsRef.current = ws;

    ws.onopen = () => {
      if (!aborted) setState((s) => ({ ...s, connected: true }));
    };

    ws.onmessage = (event) => {
      if (aborted) return;
      const msg = JSON.parse(event.data);

      switch (msg.type) {
        case "status":
          setState((s) => ({ ...s, loading: true }));
          break;
        case "ready":
          setState((s) => ({
            ...s,
            ready: true,
            loading: false,
          }));
          break;
        case "frame": {
          const frame: ReplayFrame = {
            timestamp: msg.timestamp,
            lap: msg.lap,
            total_laps: msg.total_laps,
            session_type: msg.session_type,
            drivers: msg.drivers,
            status: msg.status,
            weather: msg.weather,
            quali_phase: msg.quali_phase,
          };
          const rcMessages: RCMessage[] = msg.rc_messages || [];

          if (delayOffset > 0) {
            // Buffer the frame for delayed release
            bufferRef.current.push({ frame, receivedAt: Date.now() });
          } else {
            // No delay (or negative) — show immediately
            setState((s) => ({ ...s, frame, rcMessages }));
          }
          break;
        }
        case "finished":
          setState((s) => ({
            ...s,
            finished: true,
            sessionEnded: true,
          }));
          break;
        case "error":
          setState((s) => ({ ...s, error: msg.message, loading: false }));
          break;
      }
    };

    ws.onerror = () => {
      if (!aborted) {
        setState((s) => ({ ...s, error: "WebSocket connection error", loading: false }));
      }
    };

    ws.onclose = () => {
      if (!aborted) {
        setState((s) => ({ ...s, connected: false }));
      }
    };

    return () => {
      aborted = true;
      ws.close();
    };
  }, [year, round, sessionType, speed]);

  const send = useCallback((msg: string) => {
    if (wsRef.current?.readyState === WebSocket.OPEN) {
      wsRef.current.send(msg);
    }
  }, []);

  return { ...state, send };
}
