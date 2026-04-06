"use client";

import { useState, useEffect, useRef, useCallback, useMemo } from "react";
import { useParams, useSearchParams } from "react-router-dom";
import { useApi } from "@/hooks/useApi";
import { useReplaySocket } from "@/hooks/useReplaySocket";
import { useSettings } from "@/hooks/useSettings";
import { apiFetch, apiRequest } from "@/lib/api";
import SessionBanner from "@/components/SessionBanner";
import TrackCanvas from "@/components/TrackCanvas";
import Leaderboard, { type LapEntry } from "@/components/Leaderboard";
import PlaybackControls from "@/components/PlaybackControls";
import TelemetryChart from "@/components/TelemetryChart";
import PiPWindow from "@/components/PiPWindow";
import LapAnalysisPanel from "@/components/LapAnalysisPanel";
import type { SectorOverlay, Q3CompareLine } from "@/lib/trackRenderer";
import {
  type Q3LineDriver,
  type Q3LinesData,
  type Q3SectorCell,
  buildQ3CompareLines,
  buildQ3SectorReveal,
  computeQ3LapDelta,
  computeQ3LiveDelta,
  computeQ3SectorDelta,
} from "@/lib/q3Compare";
import { Maximize, Minimize, ArrowUpRight } from "lucide-react";

interface TrackData {
  track_points: { x: number; y: number }[];
  rotation: number;
  circuit_name: string;
  sector_boundaries?: { s1_end: number; s2_end: number; total: number } | null;
  corners?: { x: number; y: number; number: number; letter: string; angle: number }[] | null;
  marshal_sectors?: { x: number; y: number; number: number }[] | null;
}

interface SessionData {
  year: number;
  round_number: number;
  event_name: string;
  circuit: string;
  country: string;
  session_type: string;
  drivers: Array<{
    abbreviation: string;
    driver_number: string;
    full_name: string;
    team_name: string;
    team_color: string;
  }>;
}

interface DownloadStatus {
  year: number;
  round: number;
  session_type: string;
  download_state: "not_downloaded" | "queued" | "processing" | "downloaded" | "failed";
  downloaded: boolean;
  last_error?: string;
  updated_at?: string;
  message?: string;
  queue_position?: number;
  attempt?: number;
  max_attempts?: number;
}

function Q3LegendCar({ color, outlined = false }: { color: string; outlined?: boolean }) {
  const body = outlined ? "#111827" : color;
  const stroke = outlined ? color : "#FFFFFF";
  const strokeWidth = outlined ? 1.4 : 1.1;
  return (
    <svg
      width="16"
      height="10"
      viewBox="0 0 16 10"
      aria-hidden="true"
      className="shrink-0"
      style={{ overflow: "visible" }}
    >
      <rect x="0.5" y="3" width="2.1" height="4" rx="0.6" fill={body} stroke={stroke} strokeWidth={strokeWidth} />
      <rect x="2.8" y="3.2" width="7.1" height="3.6" rx="1.1" fill={body} stroke={stroke} strokeWidth={strokeWidth} />
      <path d="M9.9 3.6 L14.1 5 L9.9 6.4 Z" fill={body} stroke={stroke} strokeWidth={strokeWidth} strokeLinejoin="round" />
      <rect x="13.9" y="2.9" width="1.7" height="4.2" rx="0.6" fill={body} stroke={stroke} strokeWidth={strokeWidth} />
      <circle cx="4.4" cy="2.6" r="0.95" fill="#0A0A0A" />
      <circle cx="4.4" cy="7.4" r="0.95" fill="#0A0A0A" />
      <circle cx="8.2" cy="2.6" r="0.95" fill="#0A0A0A" />
      <circle cx="8.2" cy="7.4" r="0.95" fill="#0A0A0A" />
    </svg>
  );
}

export default function ReplayPage() {
  const params = useParams<{ year: string; round: string }>();
  const [searchParams] = useSearchParams();
  const year = Number(params.year);
  const round = Number(params.round);
  const sessionType = searchParams.get("type") || "R";

  const [selectedDrivers, setSelectedDrivers] = useState<string[]>([]);
  const [showTelemetry, setShowTelemetry] = useState(false);
  const [telemetryPosition, setTelemetryPosition] = useState<"left" | "bottom">("left");
  const [pipActive, setPipActive] = useState(false);
  const [fullscreen, setFullscreen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const [isMediumScreen, setIsMediumScreen] = useState(false);
  const [mobileTrackOpen, setMobileTrackOpen] = useState(true);
  const [mobileLeaderboardOpen, setMobileLeaderboardOpen] = useState(true);
  const [mobileTelemetryOpen, setMobileTelemetryOpen] = useState(false);
  const [mobileRcOpen, setMobileRcOpen] = useState(true);
  const [lapAnalysisOpen, setLapAnalysisOpen] = useState(false);
  const [mobileLapAnalysisOpen, setMobileLapAnalysisOpen] = useState(false);
  // Force telemetry to bottom when lap analysis panel is open to avoid squashing the track map
  // effectiveTelemetryPosition is computed later after settings is available
  const [forceBottomTelemetry, setForceBottomTelemetry] = useState(false);
  const effectiveTelemetryPosition = (lapAnalysisOpen || forceBottomTelemetry) && telemetryPosition === "left" ? "bottom" : telemetryPosition;
  const [leaderboardScale, setLeaderboardScale] = useState(1);
  const [pipTrackOpen, setPipTrackOpen] = useState(true);
  const [pipTelemetryOpen, setPipTelemetryOpen] = useState(false);
  const [pipRcOpen, setPipRcOpen] = useState(true);
  const [pipLeaderboardOpen, setPipLeaderboardOpen] = useState(true);
  const [showSectorOverlay, setShowSectorOverlay] = useState(false);
  const [sectorFocusDriver, setSectorFocusDriver] = useState<string | null>(null);
  const [rcPanelOpen, setRcPanelOpen] = useState(false);
  const [rcPinned, setRcPinned] = useState(false);
  const [q3CompareMode, setQ3CompareMode] = useState(false);
  const [q3SelectedDrivers, setQ3SelectedDrivers] = useState<[string | null, string | null]>([null, null]);

  // Persist panel layout per session type
  const layoutCategory = sessionType === "R" || sessionType === "S" ? "race"
    : sessionType === "Q" || sessionType === "SQ" ? "qualifying"
    : "practice";
  const layoutKey = `f1replay_layout_${layoutCategory}`;
  const layoutLoadedRef = useRef(false);

  // Load saved layout on mount
  useEffect(() => {
    try {
      const saved = localStorage.getItem(layoutKey);
      if (saved) {
        const layout = JSON.parse(saved);
        if (layout.showTelemetry != null) setShowTelemetry(layout.showTelemetry);
        if (layout.telemetryPosition != null) setTelemetryPosition(layout.telemetryPosition);
        if (layout.rcPinned != null) setRcPinned(layout.rcPinned);
        if (layout.rcPanelOpen != null) setRcPanelOpen(layout.rcPanelOpen);
        if (layout.lapAnalysisOpen != null) setLapAnalysisOpen(layout.lapAnalysisOpen);
        if (layout.showSectorOverlay != null) setShowSectorOverlay(layout.showSectorOverlay);
      }
    } catch {}
    // Allow saving after load completes
    setTimeout(() => { layoutLoadedRef.current = true; }, 100);
  }, [layoutKey]);

  // Save layout when panel states change (only after initial load)
  useEffect(() => {
    if (!layoutLoadedRef.current) return;
    try {
      localStorage.setItem(layoutKey, JSON.stringify({
        showTelemetry,
        telemetryPosition,
        rcPinned,
        rcPanelOpen,
        lapAnalysisOpen,
        showSectorOverlay,
      }));
    } catch {}
  }, [showTelemetry, telemetryPosition, rcPinned, rcPanelOpen, lapAnalysisOpen, showSectorOverlay, layoutKey]);
  const [rcPanelSize, setRcPanelSize] = useState<"sm" | "md" | "lg">("md");
  const [rcPosition, setRcPosition] = useState<{ x: number; y: number } | null>(null);
  const rcDragRef = useRef<{ startX: number; startY: number; origX: number; origY: number } | null>(null);
  const rcPanelRef = useRef<HTMLDivElement>(null);
  const telemetryPanelRef = useRef<HTMLDivElement>(null);
  const [telemetryHeight, setTelemetryHeight] = useState<number>(0);

  const [isIOS, setIsIOS] = useState(false);

  useEffect(() => {
    function check() { setIsMobile(window.innerWidth < 640); setIsMediumScreen(window.innerWidth >= 640 && window.innerWidth < 1400); }
    check();
    window.addEventListener("resize", check);
    setIsIOS(/iPad|iPhone|iPod/.test(navigator.userAgent) || (navigator.platform === "MacIntel" && navigator.maxTouchPoints > 1));
    return () => window.removeEventListener("resize", check);
  }, []);

  useEffect(() => {
    const onFsChange = () => { if (!document.fullscreenElement) setFullscreen(false); };
    document.addEventListener("fullscreenchange", onFsChange);
    return () => document.removeEventListener("fullscreenchange", onFsChange);
  }, []);

  const onRcDragStart = useCallback((e: React.MouseEvent | React.TouchEvent) => {
    e.preventDefault();
    const clientX = "touches" in e ? e.touches[0].clientX : e.clientX;
    const clientY = "touches" in e ? e.touches[0].clientY : e.clientY;
    const panel = rcPanelRef.current;
    if (!panel) return;
    const rect = panel.getBoundingClientRect();
    rcDragRef.current = { startX: clientX, startY: clientY, origX: rect.left, origY: rect.top };

    const onMove = (ev: MouseEvent | TouchEvent) => {
      ev.preventDefault();
      if (!rcDragRef.current) return;
      const cx = "touches" in ev ? ev.touches[0].clientX : ev.clientX;
      const cy = "touches" in ev ? ev.touches[0].clientY : ev.clientY;
      const dx = cx - rcDragRef.current.startX;
      const dy = cy - rcDragRef.current.startY;
      setRcPosition({ x: rcDragRef.current.origX + dx, y: rcDragRef.current.origY + dy });
    };
    const onUp = () => {
      rcDragRef.current = null;
      document.removeEventListener("mousemove", onMove);
      document.removeEventListener("mouseup", onUp);
      document.removeEventListener("touchmove", onMove);
      document.removeEventListener("touchend", onUp);
    };
    document.addEventListener("mousemove", onMove);
    document.addEventListener("mouseup", onUp);
    document.addEventListener("touchmove", onMove, { passive: false });
    document.addEventListener("touchend", onUp);
  }, []);

  function handleDriverClick(abbr: string) {
    setSelectedDrivers((prev) => {
      if (prev.includes(abbr)) {
        return prev.filter((d) => d !== abbr);
      }
      return [...prev, abbr];
    });
  }
  const { settings, update: updateSetting } = useSettings();

  const [downloadStatus, setDownloadStatus] = useState<DownloadStatus | null>(null);
  const [statusLoading, setStatusLoading] = useState(true);
  const [statusError, setStatusError] = useState<string | null>(null);
  const [retryingDownload, setRetryingDownload] = useState(false);
  const autoEnqueueRef = useRef(false);

  const fetchDownloadStatus = useCallback(async () => {
    const status = await apiFetch<DownloadStatus>(
      `/api/downloads/session-status?year=${year}&round=${round}&type=${sessionType}`,
    );
    setDownloadStatus(status);
    setStatusError(null);
    return status;
  }, [year, round, sessionType]);

  useEffect(() => {
    let cancelled = false;
    setStatusLoading(true);
    fetchDownloadStatus()
      .catch((err) => {
        if (!cancelled) setStatusError(err.message || "Failed to load session status");
      })
      .finally(() => {
        if (!cancelled) setStatusLoading(false);
      });

    const timer = setInterval(() => {
      fetchDownloadStatus().catch((err) => {
        if (!cancelled) setStatusError(err.message || "Failed to load session status");
      });
    }, 3000);

    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [fetchDownloadStatus]);

  useEffect(() => {
    if (!downloadStatus || autoEnqueueRef.current) {
      return;
    }
    if (downloadStatus.download_state !== "not_downloaded") {
      return;
    }
    autoEnqueueRef.current = true;
    apiRequest<{ counts: { enqueued: number } }>("/api/downloads/enqueue", {
      method: "POST",
      body: JSON.stringify({
        mode: "session",
        year,
        round,
        session_type: sessionType,
      }),
    })
      .then(() => fetchDownloadStatus())
      .catch((err) => setStatusError(err.message || "Failed to queue session download"));
  }, [downloadStatus, fetchDownloadStatus, round, sessionType, year]);

  const retryDownload = useCallback(async () => {
    setRetryingDownload(true);
    try {
      await apiRequest("/api/downloads/enqueue", {
        method: "POST",
        body: JSON.stringify({
          mode: "session",
          year,
          round,
          session_type: sessionType,
        }),
      });
      await fetchDownloadStatus();
      setStatusError(null);
    } catch (err) {
      const msg = err instanceof Error ? err.message : "Failed to retry download";
      setStatusError(msg);
    } finally {
      setRetryingDownload(false);
    }
  }, [fetchDownloadStatus, round, sessionType, year]);

  const downloaded = downloadStatus?.downloaded === true;

  const { data: sessionData, loading: sessionLoading, error: sessionError } = useApi<SessionData>(
    downloaded ? `/api/sessions/${year}/${round}?type=${sessionType}` : null,
  );

  const { data: trackData, loading: trackLoading, error: trackError } = useApi<TrackData>(
    downloaded ? `/api/sessions/${year}/${round}/track?type=${sessionType}` : null,
  );

  // Fetch lap data for last lap time column (race/sprint only)
  // Fetch lap data for last lap time column (all session types)
  const { data: lapsResponse } = useApi<{ laps: LapEntry[] }>(
    downloaded ? `/api/sessions/${year}/${round}/laps?type=${sessionType}` : null,
  );

  const { data: q3LinesResponse } = useApi<Q3LinesData>(
    downloaded && (sessionType === "Q" || sessionType === "SQ")
      ? `/api/sessions/${year}/${round}/q3-lines?type=${sessionType}`
      : null,
  );

  // Build lookup: driver -> lap_number -> lap_time
  const lapData = useMemo(() => {
    if (!lapsResponse?.laps) return undefined;
    const map = new Map<string, Map<number, { time: string; completedAt: number | null }>>();
    for (const lap of lapsResponse.laps) {
      if (!lap.lap_time) continue;
      let driverMap = map.get(lap.driver);
      if (!driverMap) {
        driverMap = new Map();
        map.set(lap.driver, driverMap);
      }
      driverMap.set(lap.lap_number, { time: lap.lap_time, completedAt: lap.time ?? null });
    }
    return map;
  }, [lapsResponse]);

  const replay = useReplaySocket(year, round, sessionType, downloaded);

  // RC sound notification
  const lastRcCountRef = useRef(0);
  useEffect(() => {
    const msgs = replay.frame?.rc_messages || [];
    if (msgs.length > lastRcCountRef.current && lastRcCountRef.current > 0 && settings.rcSound) {
      try {
        const ctx = new (window.AudioContext || (window as unknown as { webkitAudioContext: typeof AudioContext }).webkitAudioContext)();
        const osc = ctx.createOscillator();
        const gain = ctx.createGain();
        osc.connect(gain);
        gain.connect(ctx.destination);
        osc.frequency.value = 880;
        gain.gain.value = 0.15;
        osc.start();
        gain.gain.exponentialRampToValueAtTime(0.001, ctx.currentTime + 0.15);
        osc.stop(ctx.currentTime + 0.15);
      } catch {}
    }
    lastRcCountRef.current = msgs.length;
  }, [replay.frame?.rc_messages?.length, settings.rcSound]);

  const [telemetryWidth, setTelemetryWidth] = useState<number>(0);
  useEffect(() => {
    if (telemetryPanelRef.current) {
      setTelemetryHeight(telemetryPanelRef.current.offsetHeight);
      setTelemetryWidth(telemetryPanelRef.current.offsetWidth);
    }
  }, [selectedDrivers.length, showTelemetry, effectiveTelemetryPosition]);

  const isRace = sessionType === "R" || sessionType === "S";
  const isQualifyingSession = sessionType === "Q" || sessionType === "SQ";

  const q3DriverMap = useMemo(() => {
    const map = new Map<string, Q3LineDriver>();
    for (const d of q3LinesResponse?.drivers || []) {
      map.set(d.abbr, d);
    }
    return map;
  }, [q3LinesResponse]);

  useEffect(() => {
    if (!isQualifyingSession) {
      setQ3CompareMode(false);
      setQ3SelectedDrivers([null, null]);
      return;
    }
    const available = q3LinesResponse?.drivers || [];
    if (available.length === 0) {
      setQ3CompareMode(false);
      setQ3SelectedDrivers([null, null]);
      return;
    }
    const allowed = new Set(available.map((d) => d.abbr));
    const defaults = q3LinesResponse?.default_pair || [];
    const fallbackFirst = defaults[0] || available[0]?.abbr || null;
    const fallbackSecond = defaults[1] || available[1]?.abbr || null;

    setQ3SelectedDrivers((prev) => {
      let first = prev[0];
      let second = prev[1];
      if (!first || !allowed.has(first)) first = fallbackFirst;
      if (!second || !allowed.has(second) || second === first) second = fallbackSecond && fallbackSecond !== first ? fallbackSecond : (available.find((d) => d.abbr !== first)?.abbr || null);
      if (first === prev[0] && second === prev[1]) return prev;
      return [first, second];
    });
  }, [isQualifyingSession, q3LinesResponse]);

  const q3CompareLines = useMemo<Q3CompareLine[]>(
    () => buildQ3CompareLines(q3CompareMode && isQualifyingSession, q3SelectedDrivers, q3DriverMap),
    [isQualifyingSession, q3CompareMode, q3SelectedDrivers, q3DriverMap],
  );

  const q3CompareActive = q3CompareMode && q3CompareLines.length === 2;
  const q3ComputedTotalTime = useMemo(() => {
    if (!q3CompareActive) return 0;
    return Math.max(q3CompareLines[0].lapTimeSeconds, q3CompareLines[1].lapTimeSeconds, 0);
  }, [q3CompareActive, q3CompareLines]);
  const [q3TotalTime, setQ3TotalTime] = useState(0);
  const [q3PlaybackPlaying, setQ3PlaybackPlaying] = useState(false);
  const [q3PlaybackSpeed, setQ3PlaybackSpeed] = useState(1);
  const [q3PlaybackTime, setQ3PlaybackTime] = useState(0);
  const [q3PlaybackFinished, setQ3PlaybackFinished] = useState(false);
  const q3LastTickRef = useRef<number | null>(null);
  const q3EffectiveTotalTime = q3TotalTime > 0 ? q3TotalTime : q3ComputedTotalTime;

  const q3LiveDelta = useMemo(
    () => computeQ3LiveDelta(q3CompareLines, q3PlaybackTime),
    [q3CompareLines, q3PlaybackTime],
  );

  const q3SectorReveal = useMemo(
    () => buildQ3SectorReveal(q3CompareLines, q3DriverMap, q3PlaybackTime),
    [q3CompareLines, q3DriverMap, q3PlaybackTime],
  );

  const q3SectorDelta = useMemo(
    () => computeQ3SectorDelta(q3CompareLines, q3SectorReveal),
    [q3CompareLines, q3SectorReveal],
  );

  const q3LapDelta = useMemo(
    () => computeQ3LapDelta(q3CompareLines),
    [q3CompareLines],
  );

  const q3PairSignature = useMemo(
    () => q3CompareLines.map((d) => `${d.abbr}:${d.lapTimeSeconds}`).join("|"),
    [q3CompareLines],
  );

  useEffect(() => {
    if (!q3CompareMode) {
      setQ3PlaybackPlaying(false);
      setQ3PlaybackTime(0);
      setQ3PlaybackFinished(false);
      setQ3TotalTime(0);
      q3LastTickRef.current = null;
      return;
    }
    setQ3PlaybackPlaying(false);
    setQ3PlaybackTime(0);
    setQ3PlaybackFinished(false);
    setQ3TotalTime(q3ComputedTotalTime);
    q3LastTickRef.current = null;
  }, [q3CompareMode, q3PairSignature, q3ComputedTotalTime]);

  useEffect(() => {
    if (!q3CompareActive || !q3PlaybackPlaying || q3PlaybackFinished || q3EffectiveTotalTime <= 0) {
      q3LastTickRef.current = null;
      return;
    }

    let rafId = 0;
    const step = (ts: number) => {
      if (q3LastTickRef.current == null) {
        q3LastTickRef.current = ts;
      }
      const deltaSeconds = (ts - q3LastTickRef.current) / 1000;
      q3LastTickRef.current = ts;

      setQ3PlaybackTime((prev) => {
        const next = prev + deltaSeconds * Math.max(q3PlaybackSpeed, 0.25);
        if (next >= q3EffectiveTotalTime) {
          setQ3PlaybackPlaying(false);
          setQ3PlaybackFinished(true);
          q3LastTickRef.current = null;
          return q3EffectiveTotalTime;
        }
        return next;
      });

      rafId = window.requestAnimationFrame(step);
    };

    rafId = window.requestAnimationFrame(step);
    return () => window.cancelAnimationFrame(rafId);
  }, [q3CompareActive, q3PlaybackPlaying, q3PlaybackFinished, q3PlaybackSpeed, q3EffectiveTotalTime]);

  const q3Play = useCallback(() => {
    if (!q3CompareActive || q3EffectiveTotalTime <= 0) return;
    if (q3PlaybackFinished || q3PlaybackTime >= q3EffectiveTotalTime) {
      setQ3PlaybackTime(0);
      setQ3PlaybackFinished(false);
    }
    q3LastTickRef.current = null;
    setQ3PlaybackPlaying(true);
  }, [q3CompareActive, q3PlaybackFinished, q3PlaybackTime, q3EffectiveTotalTime]);

  const q3Pause = useCallback(() => {
    setQ3PlaybackPlaying(false);
    q3LastTickRef.current = null;
  }, []);

  const q3Seek = useCallback((time: number) => {
    const clamped = Math.max(0, Math.min(q3EffectiveTotalTime, time));
    setQ3PlaybackTime(clamped);
    const reachedEnd = q3EffectiveTotalTime > 0 && clamped >= q3EffectiveTotalTime;
    setQ3PlaybackFinished(reachedEnd);
    if (reachedEnd) {
      setQ3PlaybackPlaying(false);
    }
    q3LastTickRef.current = null;
  }, [q3EffectiveTotalTime]);

  const q3Reset = useCallback(() => {
    setQ3PlaybackPlaying(false);
    setQ3PlaybackTime(0);
    setQ3PlaybackFinished(false);
    q3LastTickRef.current = null;
  }, []);

  const q3SetSpeed = useCallback((speed: number) => {
    setQ3PlaybackSpeed(speed);
    q3LastTickRef.current = null;
  }, []);

  // "Open all data panels" — must be before early returns to maintain hook order
  useEffect(() => {
    if (settings.showAllPanels) {
      setShowTelemetry(true);
      setRcPinned(true);
      setRcPanelOpen(false);
      setForceBottomTelemetry(true);
      if (isRace) setLapAnalysisOpen(true);
    } else {
      setForceBottomTelemetry(false);
    }
  }, [settings.showAllPanels]); // eslint-disable-line react-hooks/exhaustive-deps

  const isLoading = downloaded && (sessionLoading || trackLoading);
  const dataError = sessionError || trackError;

  if (statusLoading && !downloadStatus) {
    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block w-12 h-12 border-[3px] border-f1-muted border-t-f1-red rounded-full animate-spin mb-6" />
          <p className="text-white text-lg font-bold">Checking session status...</p>
          <p className="text-f1-muted text-sm mt-2">Preparing download state</p>
        </div>
      </div>
    );
  }

  if (!downloaded) {
    const state = downloadStatus?.download_state || "not_downloaded";
    const statusText = (() => {
      if (state === "processing") return downloadStatus?.message || "Processing session data...";
      if (state === "queued") {
        const pos = downloadStatus?.queue_position;
        return pos ? `Queued for download (position ${pos})` : "Queued for download";
      }
      if (state === "failed") return "Session download failed";
      return "Queuing session download...";
    })();
    const details = (() => {
      if (state === "processing") {
        const a = downloadStatus?.attempt || 0;
        const m = downloadStatus?.max_attempts || 0;
        if (a > 0 && m > 0) return `Attempt ${a} of ${m}. First processing can take a few minutes.`;
        return "First processing can take a few minutes.";
      }
      if (state === "queued") return "The backend is processing jobs in FIFO order.";
      if (state === "failed") return downloadStatus?.last_error || "Please retry download.";
      return "This session will start automatically when data is ready.";
    })();

    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center px-4">
        <div className="text-center max-w-xl">
          <div className="inline-block w-12 h-12 border-[3px] border-f1-muted border-t-f1-red rounded-full animate-spin mb-6" />
          <p className="text-white text-xl font-bold mb-2">{statusText}</p>
          <p className="text-f1-muted text-sm mb-2">{details}</p>
          {statusError && <p className="text-red-400 text-sm mb-4">{statusError}</p>}
          <div className="flex items-center justify-center gap-3 mt-6">
            {state === "failed" && (
              <button
                onClick={retryDownload}
                disabled={retryingDownload}
                className="px-4 py-2 bg-f1-red text-white font-bold text-sm rounded hover:bg-red-700 transition-colors disabled:opacity-50"
              >
                {retryingDownload ? "Retrying..." : "Retry Download"}
              </button>
            )}
            <a
              href="/downloads"
              className="px-4 py-2 bg-f1-border text-white font-bold text-sm rounded hover:bg-white/15 transition-colors"
            >
              Open Downloads
            </a>
            <a
              href="/"
              className="px-4 py-2 bg-f1-border text-white font-bold text-sm rounded hover:bg-white/15 transition-colors"
            >
              Back
            </a>
          </div>
        </div>
      </div>
    );
  }

  // Show loading until session + track + replay frames are all ready
  if (isLoading || (!dataError && replay.loading)) {
    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block w-12 h-12 border-[3px] border-f1-muted border-t-f1-red rounded-full animate-spin mb-6" />
          <p className="text-white text-lg font-bold">
            {replay.statusMessage || "Loading session data..."}
          </p>
          <p className="text-f1-muted text-sm mt-2">
            {replay.statusMessage ? "This may take a few minutes the first time a session loads" : "First load may take up to 60 seconds while data is fetched"}
          </p>
        </div>
      </div>
    );
  }

  if (dataError) {
    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center max-w-md">
          <p className="text-red-400 text-lg font-bold mb-2">Session Unavailable</p>
          <p className="text-f1-muted mb-1">
            Data for this session is not available yet.
          </p>
          <p className="text-f1-muted text-sm mb-6">
            If the session just finished, data typically becomes available 1–2 hours after the chequered flag.
          </p>
          <a href="/" className="inline-block px-4 py-2 bg-f1-red text-white font-bold text-sm rounded hover:bg-red-700 transition-colors">
            Back to session picker
          </a>
        </div>
      </div>
    );
  }

  const trackPoints = trackData?.track_points || [];
  const rotation = trackData?.rotation || 0;
  const drivers = replay.frame?.drivers || [];
  const trackStatus = replay.frame?.status || "green";
  const redFlagEnd = replay.frame?.red_flag_end ?? null;
  const redFlagCountdown = redFlagEnd !== null && replay.frame
    ? Math.max(0, redFlagEnd - replay.frame.timestamp)
    : null;
  const weather = replay.frame?.weather;
  const isQualifying = sessionType === "Q" || sessionType === "SQ";
  const isPractice = sessionType === "FP1" || sessionType === "FP2" || sessionType === "FP3";
  const hasSectors = isQualifying || isPractice;

  // Turn off showAllPanels when user manually closes any panel
  function closePanel(closeFn: () => void) {
    closeFn();
    if (settings.showAllPanels) updateSetting("showAllPanels", false);
  }

  // For practice sessions, cap the total time at the official session duration (60 min)
  // so the "remaining" timer is accurate rather than including post-session telemetry
  const PRACTICE_DURATION = 3600; // 60 minutes
  const effectiveTotalTime = isPractice ? Math.min(replay.totalTime, PRACTICE_DURATION) : replay.totalTime;

  // Compute sector overlay for track map
  const SECTOR_HEX: Record<string, string> = { purple: "#A855F7", green: "#22C55E", yellow: "#EAB308" };
  const DEFAULT_SECTOR = "#3A3A4A";
  const sectorOverlay: SectorOverlay | null = (() => {
    if (!hasSectors || !showSectorOverlay || !trackData?.sector_boundaries) return null;
    const target = sectorFocusDriver && selectedDrivers.includes(sectorFocusDriver)
      ? sectorFocusDriver
      : null;
    if (!target) return null;
    const drv = drivers.find((d) => d.abbr === target);
    const sectors = drv?.sectors;
    return {
      boundaries: trackData.sector_boundaries,
      colors: {
        s1: SECTOR_HEX[sectors?.find((s) => s.num === 1)?.color ?? ""] ?? DEFAULT_SECTOR,
        s2: SECTOR_HEX[sectors?.find((s) => s.num === 2)?.color ?? ""] ?? DEFAULT_SECTOR,
        s3: SECTOR_HEX[sectors?.find((s) => s.num === 3)?.color ?? ""] ?? DEFAULT_SECTOR,
      },
    };
  })();

  // Calculate leaderboard width based on active columns
  const leaderboardWidthFull = (() => {
    let w = 106; // base: position(24) + team bar(12) + driver(30) + flags(16) + padding(16) + right padding(8)
    if (settings.showTeamAbbr) w += 28;
    if (!isRace) w += 18; // pit indicator (P box + margin)
    if (isRace && settings.showGridChange) w += 24;
    if (!isRace && settings.showBestLapTime) w += 60; // best lap time column
    if (settings.showLastLapTime) w += 60; // last lap time column
    if (settings.showGapToLeader) w += 56 + (!isRace ? 8 : 0); // extra margin between lap time and gap in practice/qualifying
    if (hasSectors && settings.showSectors) w += 36; // sector indicators (28 + 8 margin)
    if (isRace && settings.showPitStops) w += 24;
    if (isRace && settings.showTyreHistory) w += 36;
    if (settings.showTyreType) w += 24;
    if (settings.showTyreAge) w += 20;
    if (isRace && settings.showPitPrediction) w += 40; // pit prediction
    if (isRace && settings.showPitPrediction && settings.showPitFreeAir) w += 36; // pit gaps (ahead/behind)
    return w;
  })();

  // On mobile, auto-hide team abbreviation if columns overflow the screen
  const mobileTeamAbbrHidden = isMobile && settings.showTeamAbbr && leaderboardWidthFull > (typeof window !== "undefined" ? window.innerWidth : 400);
  const leaderboardWidth = mobileTeamAbbrHidden ? leaderboardWidthFull - 28 : leaderboardWidthFull;
  const compareActive = q3CompareActive;
  const controlsUseQ3 = q3CompareActive;
  const replayTrackDrivers = drivers.filter((d) => !d.retired && !d.no_timing && !d.finished && (d.x !== 0 || d.y !== 0) && d.x > -0.5 && d.x < 1.5 && d.y > -0.5 && d.y < 1.5).map((d) => ({
    abbr: d.abbr,
    x: d.x,
    y: d.y,
    color: d.color,
    position: d.position,
  }));

  return (
    <div className="h-dvh flex flex-col bg-f1-dark overflow-hidden" style={{ paddingTop: "env(safe-area-inset-top)" }}>
      {/* Banner */}
      {!fullscreen && sessionData && (
        <SessionBanner
          eventName={sessionData.event_name}
          circuit={sessionData.circuit}
          country={sessionData.country}
          sessionType={sessionType}
          year={year}
          settings={settings}
          onSettingChange={updateSetting}
          weather={weather}
          mobileTeamAbbrHidden={mobileTeamAbbrHidden}
        />
      )}

      {/* Main content */}
      <div className="flex-1 flex flex-col sm:flex-row min-h-0 overflow-y-auto sm:overflow-hidden pb-20 sm:pb-0">
        {/* Race Control section - mobile only, above track map */}
        <div className="sm:hidden">
          <button
            onClick={() => setMobileRcOpen(!mobileRcOpen)}
            className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
          >
            <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Race Control</span>
            <svg className={`w-4 h-4 text-f1-muted transition-transform ${mobileRcOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          {mobileRcOpen && (() => {
            const latest = (replay.frame?.rc_messages || [])[0];
            if (!latest) return <p className="text-f1-muted text-xs px-3 py-2">No messages yet</p>;
            const upper = latest.message.toUpperCase();
            const isPenalty = upper.includes("PENALTY") && !upper.includes("NO FURTHER");
            const isInvestigation = upper.includes("INVESTIGATION") || upper.includes("NOTED");
            const isCleared = upper.includes("NO FURTHER") || upper.includes("NO INVESTIGATION");
            return (
              <div className="px-3 py-2 bg-f1-card border-b border-f1-border">
                <div className="flex items-start gap-2">
                  <div className={`w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0 ${
                    isPenalty ? "bg-red-500" : isInvestigation ? "bg-orange-400" : isCleared ? "bg-green-500" : "bg-f1-muted"
                  }`} />
                  <div className="min-w-0">
                    <p className="text-[11px] text-white leading-tight">{latest.message}</p>
                    {latest.lap && <span className="text-[9px] text-f1-muted">Lap {latest.lap}</span>}
                  </div>
                </div>
              </div>
            );
          })()}
        </div>

        {/* Track section */}
        <div className={`sm:flex-1 min-w-0 ${!isMobile && showTelemetry && (selectedDrivers.length > 2 || settings.showAllPanels || rcPinned) ? `flex ${effectiveTelemetryPosition === "left" ? "flex-row" : "flex-col"} min-h-0` : "relative"}`}>
          {/* Mobile section header */}
          {isMobile && (
            <button
              onClick={() => setMobileTrackOpen(!mobileTrackOpen)}
              className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
            >
              <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Track Map</span>
              <svg className={`w-4 h-4 text-f1-muted transition-transform ${mobileTrackOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
              </svg>
            </button>
          )}

          {(!isMobile || mobileTrackOpen) && (
            <div className={`h-[42vh] sm:h-full relative ${!isMobile && showTelemetry && (selectedDrivers.length > 2 || settings.showAllPanels || rcPinned) ? "flex-1 min-w-0 min-h-0" : ""}`}>
              {/* Flag badge */}
              {trackStatus !== "green" && (
                <div className="absolute top-3 left-1/2 -translate-x-1/2 z-10">
                  <div
                    className={`px-3 py-1 rounded text-xs font-extrabold uppercase ${
                      trackStatus === "red"
                        ? "bg-red-600 text-white"
                        : trackStatus === "sc"
                        ? "bg-yellow-500 text-black"
                        : trackStatus === "vsc"
                        ? "bg-yellow-500/80 text-black"
                        : "bg-yellow-400 text-black"
                    }`}
                  >
                    {trackStatus === "red" ? (
                      <div className="flex flex-col items-center gap-1">
                        <span>Red Flag</span>
                        {redFlagCountdown !== null && redFlagCountdown > 0 && (
                          <>
                            <span className="text-[10px] font-bold opacity-80 tabular-nums normal-case">
                              Resumes in {Math.floor(redFlagCountdown / 60)}:{String(Math.floor(redFlagCountdown % 60)).padStart(2, "0")}
                            </span>
                            <button
                              onClick={() => { if (redFlagEnd !== null) replay.seek(redFlagEnd); }}
                              className="px-2 py-0.5 text-[10px] font-bold bg-white/20 hover:bg-white/30 rounded transition-colors normal-case"
                            >
                              Skip to restart
                            </button>
                          </>
                        )}
                      </div>
                    ) : trackStatus === "sc"
                      ? "Safety Car"
                      : trackStatus === "vsc"
                      ? "Virtual Safety Car"
                      : "Yellow Flag"}
                  </div>
                </div>
              )}

              {/* Race Control toggle - desktop only, mobile has its own section */}
              <div className="absolute top-3 right-3 z-10 hidden sm:block">
                <button
                  onClick={() => {
                    if (rcPinned) {
                      setRcPinned(false);
                    } else {
                      setRcPanelOpen(!rcPanelOpen);
                    }
                  }}
                  className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-bold transition-colors ${
                    rcPanelOpen || rcPinned
                      ? "bg-orange-500 text-white"
                      : "bg-f1-card/90 border border-f1-border text-f1-muted hover:text-white backdrop-blur-sm"
                  }`}
                  title="Race Control Messages"
                >
                  <svg className="w-3.5 h-3.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M3 21v-4m0 0V5a2 2 0 012-2h6.5l1 1H21l-3 6 3 6h-8.5l-1-1H5a2 2 0 00-2 2z" />
                  </svg>
                  RC
                </button>
              </div>

              {/* Race Control Messages panel (floating, hidden when pinned) */}
              {rcPanelOpen && !rcPinned && (
                <div
                  ref={rcPanelRef}
                  className={`z-20 w-80 bg-f1-card/95 border border-f1-border rounded-lg shadow-xl backdrop-blur-sm overflow-hidden flex flex-col ${
                    rcPanelSize === "sm" ? "max-h-[25%]" : rcPanelSize === "md" ? "max-h-[50%]" : "max-h-[85%]"
                  }`}
                  style={rcPosition
                    ? { position: "fixed", left: rcPosition.x, top: rcPosition.y }
                    : { position: "absolute", top: 48, right: 12 }
                  }
                >
                  <div
                    className="flex items-center justify-between px-3 py-2 border-b border-f1-border flex-shrink-0 cursor-grab active:cursor-grabbing"
                    style={{ touchAction: "none" }}
                    onMouseDown={onRcDragStart}
                    onTouchStart={onRcDragStart}
                  >
                    <span className="text-[10px] font-bold text-f1-muted uppercase tracking-wider">Race Control</span>
                    <div className="flex items-center gap-1">
                      {(["sm", "md", "lg"] as const).map((size) => (
                        <button
                          key={size}
                          onClick={() => setRcPanelSize(size)}
                          className={`w-5 h-4 flex items-center justify-center rounded text-[8px] font-bold transition-colors ${
                            rcPanelSize === size ? "bg-f1-muted/30 text-white" : "text-f1-muted hover:text-white"
                          }`}
                          title={size === "sm" ? "Compact" : size === "md" ? "Medium" : "Expanded"}
                        >
                          {size === "sm" ? (
                            <svg className="w-3 h-3" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth={1.5}><rect x="1" y="6" width="10" height="5" rx="1" /></svg>
                          ) : size === "md" ? (
                            <svg className="w-3 h-3" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth={1.5}><rect x="1" y="3" width="10" height="8" rx="1" /></svg>
                          ) : (
                            <svg className="w-3 h-3" viewBox="0 0 12 12" fill="none" stroke="currentColor" strokeWidth={1.5}><rect x="1" y="1" width="10" height="10" rx="1" /></svg>
                          )}
                        </button>
                      ))}
                      {rcPosition && (
                        <button onClick={() => setRcPosition(null)} className="text-f1-muted hover:text-white ml-1" title="Reset position">
                          <ArrowUpRight className="w-3.5 h-3.5" />
                        </button>
                      )}
                      <button onClick={() => setRcPanelOpen(false)} className="text-f1-muted hover:text-white ml-1">
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  </div>
                  <div className="flex-1 overflow-y-auto divide-y divide-f1-border/50">
                    {(() => {
                      const allMsgs = replay.frame?.rc_messages || [];
                      const msgs = rcPanelSize === "sm" ? allMsgs.slice(0, 1) : allMsgs;
                      if (allMsgs.length === 0) return <p className="text-f1-muted text-xs p-3 text-center">No race control messages yet</p>;
                      return msgs.map((rc, i) => {
                        const upper = rc.message.toUpperCase();
                        const isInvestigation = upper.includes("INVESTIGATION") || upper.includes("NOTED");
                        const isPenalty = upper.includes("PENALTY") && !upper.includes("NO FURTHER");
                        const isCleared = upper.includes("NO FURTHER") || upper.includes("NO INVESTIGATION");
                        return (
                          <div key={i} className="px-3 py-2">
                            <div className="flex items-start gap-2">
                              <div className={`w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0 ${
                                isPenalty ? "bg-red-500" : isInvestigation ? "bg-orange-400" : isCleared ? "bg-green-500" : "bg-f1-muted"
                              }`} />
                              <div className="min-w-0">
                                <p className="text-[11px] text-white leading-tight">{rc.message}</p>
                                {rc.lap && <span className="text-[9px] text-f1-muted">Lap {rc.lap}</span>}
                              </div>
                            </div>
                          </div>
                        );
                      });
                    })()}
                  </div>
                </div>
              )}

              <TrackCanvas
                trackPoints={trackPoints}
                rotation={rotation}
                trackStatus={trackStatus}
                drivers={compareActive ? [] : replayTrackDrivers}
                highlightedDrivers={compareActive ? [] : selectedDrivers}
                playbackSpeed={replay.speed}
                showDriverNames={settings.showDriverNames}
                sectorOverlay={sectorOverlay}
                corners={settings.showCorners ? trackData?.corners : null}
                marshalSectors={trackData?.marshal_sectors}
                sectorFlags={replay.frame?.sector_flags}
                q3CompareLines={compareActive ? q3CompareLines : null}
                q3CompareElapsedSeconds={compareActive ? q3PlaybackTime : 0}
              />

              {isQualifyingSession && (q3LinesResponse?.drivers?.length || 0) > 0 && (
                <div className={`absolute top-3 left-3 z-20 backdrop-blur-sm transition-all ${
                  q3CompareMode
                    ? "bg-f1-card/92 border border-f1-border rounded-lg shadow-lg px-2.5 py-2 w-[min(420px,calc(100vw-1.5rem))]"
                    : "bg-f1-card/72 border border-f1-border/70 rounded-lg px-1.5 py-1.5"
                }`}>
                  <div className={`flex items-center ${q3CompareMode ? "gap-2" : "gap-1.5"}`}>
                    <button
                      onClick={() => setQ3CompareMode((v) => !v)}
                      disabled={(q3LinesResponse?.drivers?.length || 0) < 2}
                      className={`px-2.5 py-1.5 rounded-md text-[10px] font-extrabold uppercase tracking-wide border transition-colors ${
                        q3CompareMode
                          ? "bg-f1-red border-f1-red text-white shadow-[0_0_0_1px_rgba(255,255,255,0.08)_inset]"
                          : "bg-black/20 border-f1-border text-f1-muted hover:text-white hover:bg-black/35"
                      } disabled:opacity-50 disabled:cursor-not-allowed`}
                    >
                      Lap Compare
                    </button>
                    {!q3CompareMode && (
                      <span className="text-[10px] font-semibold text-f1-muted">Q3 best laps</span>
                    )}
                    {q3CompareMode && q3LiveDelta && (
                      <span className="text-[10px] font-bold text-white tabular-nums">LIVE DELTA: {q3LiveDelta.text}</span>
                    )}
                  </div>
                  {q3CompareMode && (
                    <div className="mt-1.5 space-y-1">
                      <div className="flex items-center gap-1">
                        <select
                          value={q3SelectedDrivers[0] || ""}
                          onChange={(e) => setQ3SelectedDrivers((prev) => [e.target.value || null, prev[1]])}
                          className="flex-1 bg-f1-dark border border-f1-border rounded px-1.5 py-1 text-[10px] text-white"
                        >
                          <option value="" disabled>Select Driver 1</option>
                          {(q3LinesResponse?.drivers || []).map((d) => (
                            <option key={d.abbr} value={d.abbr} disabled={d.abbr === q3SelectedDrivers[1]}>
                              {d.abbr} ({d.lap_time})
                            </option>
                          ))}
                        </select>
                      </div>
                      <div className="flex items-center gap-1">
                        <select
                          value={q3SelectedDrivers[1] || ""}
                          onChange={(e) => setQ3SelectedDrivers((prev) => [prev[0], e.target.value || null])}
                          className="flex-1 bg-f1-dark border border-f1-border rounded px-1.5 py-1 text-[10px] text-white"
                        >
                          <option value="" disabled>Select Driver 2</option>
                          {(q3LinesResponse?.drivers || []).map((d) => (
                            <option key={d.abbr} value={d.abbr} disabled={d.abbr === q3SelectedDrivers[0]}>
                              {d.abbr} ({d.lap_time})
                            </option>
                          ))}
                        </select>
                      </div>
                      {q3CompareLines.length === 2 && (
                        <div className="pt-1">
                          <div className="overflow-hidden rounded border border-f1-border/70 bg-black/35">
                            <div className="grid grid-cols-[minmax(90px,1.2fr)_0.85fr_0.85fr_0.85fr_1fr] gap-x-2 bg-black/45 px-2 py-1 text-[8px] font-semibold uppercase tracking-wide text-f1-muted">
                              <span>Best Lap</span>
                              <span className="text-center">Sector 1</span>
                              <span className="text-center">Sector 2</span>
                              <span className="text-center">Sector 3</span>
                              <span className="text-right">Lap Time</span>
                            </div>
                            {q3CompareLines.map((line, idx) => {
                              const lineMeta = q3DriverMap.get(line.abbr);
                              const sectors = q3SectorReveal.get(line.abbr);
                              const cells: Array<Q3SectorCell | undefined> = [sectors?.s1, sectors?.s2, sectors?.s3];
                              const sectorTextClass = (cell: Q3SectorCell | undefined) => {
                                if (!cell || !cell.raw || !cell.revealed) return "text-f1-muted";
                                if (cell.tone === "purple") return "text-purple-300";
                                if (cell.tone === "green") return "text-green-300";
                                if (cell.tone === "yellow") return "text-yellow-300";
                                return "text-f1-muted";
                              };
                              const sectorValue = (cell: Q3SectorCell | undefined) => {
                                if (!cell || !cell.raw) return "—";
                                if (!cell.revealed) return "…";
                                return cell.raw;
                              };
                              return (
                                <div
                                  key={line.abbr}
                                  className={`grid grid-cols-[minmax(90px,1.2fr)_0.85fr_0.85fr_0.85fr_1fr] gap-x-2 px-2 py-1 text-[10px] tabular-nums ${
                                    idx > 0 ? "border-t border-f1-border/35" : ""
                                  }`}
                                >
                                  <div className="flex items-center gap-1.5 min-w-0">
                                    <span className="text-[9px] text-f1-muted">{idx + 1}</span>
                                    <span
                                      className="inline-block w-4 border-t-2"
                                      style={{
                                        borderTopColor: line.color,
                                        borderTopStyle: line.lineDash?.length ? "dashed" : "solid",
                                      }}
                                    />
                                    <Q3LegendCar color={line.color} outlined={line.markerStyle === "outlined"} />
                                    <span className="font-bold text-white truncate">{line.abbr}</span>
                                  </div>
                                  {cells.map((cell, i) => (
                                    <span key={`${line.abbr}-s${i + 1}`} className={`text-center ${sectorTextClass(cell)}`}>
                                      {sectorValue(cell)}
                                    </span>
                                  ))}
                                  <span className="text-right text-white">{lineMeta?.lap_time || "—"}</span>
                                </div>
                              );
                            })}
                            <div className="grid grid-cols-[minmax(90px,1.2fr)_0.85fr_0.85fr_0.85fr_1fr] gap-x-2 border-t border-f1-border/45 bg-black/25 px-2 py-1 text-[9px] tabular-nums">
                              <span className="font-semibold text-f1-muted uppercase">Δ (D1-D2)</span>
                              <span className={`text-center ${
                                !q3SectorDelta?.s1?.ready
                                  ? "text-f1-muted"
                                  : Math.abs(q3SectorDelta.s1.signed ?? 0) < 5e-4
                                  ? "text-white"
                                  : (q3SectorDelta.s1.signed ?? 0) < 0
                                  ? "text-green-300"
                                  : "text-yellow-300"
                              }`}>
                                {q3SectorDelta?.s1?.ready ? `${q3SectorDelta.s1.text}s` : "…"}
                              </span>
                              <span className={`text-center ${
                                !q3SectorDelta?.s2?.ready
                                  ? "text-f1-muted"
                                  : Math.abs(q3SectorDelta.s2.signed ?? 0) < 5e-4
                                  ? "text-white"
                                  : (q3SectorDelta.s2.signed ?? 0) < 0
                                  ? "text-green-300"
                                  : "text-yellow-300"
                              }`}>
                                {q3SectorDelta?.s2?.ready ? `${q3SectorDelta.s2.text}s` : "…"}
                              </span>
                              <span className={`text-center ${
                                !q3SectorDelta?.s3?.ready
                                  ? "text-f1-muted"
                                  : Math.abs(q3SectorDelta.s3.signed ?? 0) < 5e-4
                                  ? "text-white"
                                  : (q3SectorDelta.s3.signed ?? 0) < 0
                                  ? "text-green-300"
                                  : "text-yellow-300"
                              }`}>
                                {q3SectorDelta?.s3?.ready ? `${q3SectorDelta.s3.text}s` : "…"}
                              </span>
                              <span className={`text-right ${
                                !q3LapDelta
                                  ? "text-f1-muted"
                                  : Math.abs(q3LapDelta.signed) < 5e-4
                                  ? "text-white"
                                  : q3LapDelta.signed < 0
                                  ? "text-green-300"
                                  : "text-yellow-300"
                              }`}>
                                {q3LapDelta ? `${q3LapDelta.text}s` : "…"}
                              </span>
                            </div>
                          </div>
                          <p className="mt-1 text-[8px] text-f1-muted">negative = Driver 1 faster</p>
                        </div>
                      )}
                      {q3CompareLines.length !== 2 && (
                        <p className="text-[9px] text-f1-muted">Select two Q3 drivers to start the comparison.</p>
                      )}
                    </div>
                  )}
                </div>
              )}

              {/* Telemetry now in bottom drawer */}

              {/* Sector overlay toggle - desktop qualifying only */}
              {!isMobile && hasSectors && trackData?.sector_boundaries && (
                <div className="absolute bottom-2 right-36 z-20 flex items-center gap-1">
                  {showSectorOverlay && selectedDrivers.length === 0 && (
                    <span className="text-[10px] text-f1-muted mr-1">Select a driver to view sectors</span>
                  )}
                  {showSectorOverlay && selectedDrivers.length > 0 && (
                    selectedDrivers.map((abbr) => {
                      const drv = drivers.find((d) => d.abbr === abbr);
                      const isActive = sectorFocusDriver === abbr;
                      return (
                        <button
                          key={abbr}
                          onClick={() => setSectorFocusDriver(isActive ? null : abbr)}
                          className={`px-1.5 py-1 border rounded text-[10px] font-bold transition-colors ${
                            isActive
                              ? "bg-purple-500/20 border-purple-500/50 text-purple-300"
                              : "bg-f1-card border-f1-border text-f1-muted hover:text-white"
                          }`}
                        >
                          <span className="inline-block w-1.5 h-1.5 rounded-full mr-1" style={{ backgroundColor: drv?.color }} />
                          {abbr}
                        </button>
                      );
                    })
                  )}
                  <button
                    onClick={() => setShowSectorOverlay(!showSectorOverlay)}
                    className={`px-2 py-1 border rounded text-[10px] font-bold transition-colors ${
                      showSectorOverlay
                        ? "bg-purple-500/20 border-purple-500/50 text-purple-300 hover:text-purple-200"
                        : "bg-f1-card border-f1-border text-f1-muted hover:text-white"
                    }`}
                  >
                    {showSectorOverlay ? "Hide" : "Show"} Sectors
                  </button>
                </div>
              )}

              {/* Sector overlay controls - mobile qualifying only */}
              {isMobile && hasSectors && trackData?.sector_boundaries && (
                <div className="absolute bottom-2 left-2 right-2 z-20 flex items-center gap-1">
                  {showSectorOverlay && selectedDrivers.length > 0 && (
                    <div className="flex items-center gap-1 overflow-x-auto">
                      {selectedDrivers.map((abbr) => {
                        const drv = drivers.find((d) => d.abbr === abbr);
                        const isActive = sectorFocusDriver === abbr;
                        return (
                          <button
                            key={abbr}
                            onClick={() => setSectorFocusDriver(isActive ? null : abbr)}
                            className={`flex-shrink-0 px-1.5 py-1 border rounded text-[10px] font-bold transition-colors ${
                              isActive
                                ? "bg-purple-500/20 border-purple-500/50 text-purple-300"
                                : "bg-f1-card/90 border-f1-border text-f1-muted backdrop-blur-sm"
                            }`}
                          >
                            <span className="inline-block w-1.5 h-1.5 rounded-full mr-1" style={{ backgroundColor: drv?.color }} />
                            {abbr}
                          </button>
                        );
                      })}
                    </div>
                  )}
                  {showSectorOverlay && selectedDrivers.length === 0 && (
                    <span className="text-[10px] text-f1-muted">Select a driver to view sectors</span>
                  )}
                  <button
                    onClick={() => setShowSectorOverlay(!showSectorOverlay)}
                    className={`flex-shrink-0 ml-auto px-2 py-1 border rounded text-[10px] font-bold transition-colors ${
                      showSectorOverlay
                        ? "bg-purple-500/20 border-purple-500/50 text-purple-300"
                        : "bg-f1-card/90 border-f1-border text-f1-muted backdrop-blur-sm"
                    }`}
                  >
                    Sectors
                  </button>
                </div>
              )}

              {/* Fullscreen toggle moved to PlaybackControls */}

              {/* Telemetry overlay - desktop only, bottom-left (1-2 drivers) */}
              {!isMobile && showTelemetry && selectedDrivers.length <= 2 && !settings.showAllPanels && !rcPinned && (
                <div className="absolute bottom-2 left-3 z-10 flex flex-col gap-1">
                  <button
                    onClick={() => setShowTelemetry(false)}
                    className="self-start px-2 py-0.5 bg-f1-card/90 border border-f1-border rounded text-[9px] font-bold text-f1-muted hover:text-white transition-colors backdrop-blur-sm mb-0.5"
                  >
                    Hide Telemetry
                  </button>
                  {selectedDrivers.length > 0 ? (
                    selectedDrivers.map((abbr) => {
                      const drv = drivers.find((d) => d.abbr === abbr) || null;
                      return <TelemetryChart key={abbr} visible driver={drv} year={year} isQualifying={isQualifying} useImperial={settings.useImperial} />;
                    })
                  ) : (
                    <TelemetryChart visible driver={null} year={year} useImperial={settings.useImperial} />
                  )}
                </div>
              )}

              {/* Telemetry toggle - desktop only, bottom-left */}
              {!isMobile && !showTelemetry && !settings.showAllPanels && (
                <button
                  onClick={() => setShowTelemetry(true)}
                  className="absolute bottom-2 left-3 z-20 px-2 py-1 bg-f1-card/90 border border-f1-border rounded text-[10px] font-bold text-f1-muted hover:text-white transition-colors backdrop-blur-sm"
                >
                  Show Telemetry
                </button>
              )}

              {/* Lap Analysis floating button - desktop only, bottom-right */}
              {!isMobile && isRace && lapsResponse?.laps && (
                <button
                  onClick={() => setLapAnalysisOpen(!lapAnalysisOpen)}
                  className={`absolute bottom-2 right-3 z-20 flex items-center gap-1 px-2 py-1 rounded text-xs font-bold transition-colors ${
                    lapAnalysisOpen
                      ? "bg-f1-red text-white"
                      : "bg-f1-card/90 border border-f1-border text-f1-muted hover:text-white backdrop-blur-sm"
                  }`}
                >
                  <svg className="w-3.5 h-3.5" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                  </svg>
                  Laps
                </button>
              )}
            </div>
          )}

          {/* Telemetry panel - desktop only (3+ drivers) */}
          {!isMobile && showTelemetry && (selectedDrivers.length > 2 || settings.showAllPanels || rcPinned) && (
            <div
              className={`flex-shrink-0 ${
                effectiveTelemetryPosition === "left"
                  ? "h-full bg-f1-card border-r border-f1-border order-first px-3 py-2 overflow-y-auto overflow-x-hidden"
                  : `border-t border-f1-border py-1 flex ${lapAnalysisOpen && isMediumScreen ? "flex-col overflow-y-auto" : "overflow-hidden"}`
              }`}
              style={effectiveTelemetryPosition === "left" && rcPinned && telemetryWidth > 0 ? { width: telemetryWidth + 24 } : undefined}
            >
              <div ref={telemetryPanelRef} className={effectiveTelemetryPosition === "bottom" ? "inline-block bg-f1-card px-3 pt-1 flex-shrink-0" : ""}>
                <div className="flex items-center gap-2 mb-1">
                  <span className="text-[10px] font-bold text-f1-muted uppercase">Telemetry</span>
                  {lapAnalysisOpen ? (
                    <span className="text-[9px] text-f1-muted italic">{forceBottomTelemetry ? "Shown at bottom (all panels open)" : "Shown at bottom while Lap Analysis is open"}</span>
                  ) : (
                    <button
                      onClick={() => setTelemetryPosition(telemetryPosition === "left" ? "bottom" : "left")}
                      className="px-1.5 py-0.5 text-[9px] font-bold text-f1-muted hover:text-white border border-f1-border rounded transition-colors"
                      title={telemetryPosition === "left" ? "Move to bottom" : "Move to left"}
                    >
                      {telemetryPosition === "left" ? "Move to bottom" : "Move to left"}
                    </button>
                  )}
                  <button
                    onClick={() => closePanel(() => setShowTelemetry(false))}
                    className="px-1.5 py-0.5 text-[9px] font-bold text-f1-muted hover:text-white border border-f1-border rounded transition-colors ml-auto"
                  >
                    Hide
                  </button>
                </div>
                <div className="flex flex-col gap-1">
                  {selectedDrivers.length > 0 ? (
                    selectedDrivers.map((abbr) => {
                      const drv = drivers.find((d) => d.abbr === abbr) || null;
                      return <TelemetryChart key={abbr} visible driver={drv} year={year} isQualifying={isQualifying} useImperial={settings.useImperial} />;
                    })
                  ) : (
                    <p className="text-[10px] text-f1-muted py-2">Select drivers on the leaderboard to view telemetry</p>
                  )}
                </div>
              </div>

              {/* Race Control in panel: show button or pinned messages */}
              {!rcPinned && (
                <div className={`flex items-center justify-center ${
                  effectiveTelemetryPosition === "bottom" && !(lapAnalysisOpen && isMediumScreen)
                    ? "border-l border-f1-border px-4"
                    : "border-t border-f1-border py-2 mt-2"
                }`}>
                  <button
                    onClick={() => { setRcPinned(true); setRcPanelOpen(false); setRcPosition(null); }}
                    className="px-2 py-1 text-[9px] font-bold text-f1-muted hover:text-white border border-f1-border rounded transition-colors"
                  >
                    Show Race Control
                  </button>
                </div>
              )}
              {rcPinned && (
                <div
                  className={`bg-f1-card ${
                    effectiveTelemetryPosition === "bottom" && !(lapAnalysisOpen && isMediumScreen)
                      ? "border-l border-f1-border px-3 pt-1 flex-1 overflow-hidden flex flex-col"
                    : "border-t border-f1-border px-3 py-2 mt-2"
                  }`}
                  style={effectiveTelemetryPosition === "bottom" && telemetryHeight > 0 ? { maxHeight: telemetryHeight } : undefined}
                >
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-[10px] font-bold text-f1-muted uppercase">Race Control</span>
                    <button
                      onClick={() => closePanel(() => setRcPinned(false))}
                      className="px-1.5 py-0.5 text-[9px] font-bold text-f1-muted hover:text-white border border-f1-border rounded transition-colors"
                    >
                      Hide
                    </button>
                  </div>
                  <div className="divide-y divide-f1-border/50 flex-1 overflow-y-auto">
                    {(() => {
                      const allMsgs = replay.frame?.rc_messages || [];
                      if (allMsgs.length === 0) return <p className="text-f1-muted text-xs py-2 text-center">No messages yet</p>;
                      return allMsgs.map((rc, i) => {
                        const upper = rc.message.toUpperCase();
                        const isInvestigation = upper.includes("INVESTIGATION") || upper.includes("NOTED");
                        const isPenalty = upper.includes("PENALTY") && !upper.includes("NO FURTHER");
                        const isCleared = upper.includes("NO FURTHER") || upper.includes("NO INVESTIGATION");
                        return (
                          <div key={i} className="py-1.5">
                            <div className="flex items-start gap-2">
                              <div className={`w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0 ${
                                isPenalty ? "bg-red-500" : isInvestigation ? "bg-orange-400" : isCleared ? "bg-green-500" : "bg-f1-muted"
                              }`} />
                              <div className="min-w-0">
                                <p className="text-[11px] text-white leading-tight">{rc.message}</p>
                                {rc.lap && <span className="text-[9px] text-f1-muted">Lap {rc.lap}</span>}
                              </div>
                            </div>
                          </div>
                        );
                      });
                    })()}
                  </div>
                </div>
              )}
            </div>
          )}
        </div>

        {/* Telemetry section - mobile only, collapsible like leaderboard */}
        <div className="sm:hidden">
          <button
            onClick={() => setMobileTelemetryOpen(!mobileTelemetryOpen)}
            className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
          >
            <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Telemetry</span>
            <svg className={`w-4 h-4 text-f1-muted transition-transform ${mobileTelemetryOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
              <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
            </svg>
          </button>
          {mobileTelemetryOpen && (
            <div className="bg-f1-card px-3 py-2 space-y-1">
              {selectedDrivers.length > 0 ? (
                selectedDrivers.map((abbr) => {
                  const drv = drivers.find((d) => d.abbr === abbr) || null;
                  return <TelemetryChart key={abbr} visible driver={drv} year={year} isQualifying={isQualifying} useImperial={settings.useImperial} />;
                })
              ) : (
                <TelemetryChart visible driver={null} year={year} useImperial={settings.useImperial} />
              )}
            </div>
          )}
        </div>

        {/* Leaderboard section (with optional lap analysis panel on desktop) */}
        {settings.showLeaderboard && (
          <div className={`${lapAnalysisOpen ? "flex-shrink" : "flex-shrink-0"} flex ${isMobile ? "" : "border-l"} border-f1-border overflow-hidden`} style={{ width: isMobile ? "100%" : undefined }}>
            {/* Lap Analysis Panel - desktop only, left of leaderboard */}
            {!isMobile && isRace && lapAnalysisOpen && lapsResponse?.laps && (
              <div className="w-[280px] h-full border-r border-f1-border overflow-hidden flex-shrink-0">
                <LapAnalysisPanel laps={lapsResponse.laps} drivers={drivers} currentLap={Math.max(0, (replay.frame?.lap || 0) - 1)} onClose={() => closePanel(() => setLapAnalysisOpen(false))} />
              </div>
            )}

            <div style={{ width: isMobile ? "100%" : Math.ceil(leaderboardWidth * leaderboardScale) }}>
              {/* Mobile section header */}
              {isMobile && (
                <button
                  onClick={() => setMobileLeaderboardOpen(!mobileLeaderboardOpen)}
                  className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
                >
                  <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Leaderboard</span>
                  <svg className={`w-4 h-4 text-f1-muted transition-transform ${mobileLeaderboardOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                  </svg>
                </button>
              )}

              {(!isMobile || mobileLeaderboardOpen) && (
                <Leaderboard
                  drivers={drivers}
                  highlightedDrivers={selectedDrivers}
                  onDriverClick={handleDriverClick}
                  settings={settings}
                  currentTime={replay.frame?.timestamp || 0}
                  isRace={isRace}
                  isQualifying={isQualifying}
                  onScaleChange={setLeaderboardScale}
                  lapData={lapData}
                  currentLap={replay.frame?.lap || 0}
                  mobileTeamAbbrHidden={mobileTeamAbbrHidden}
                />
              )}
            </div>
          </div>
        )}

        {/* Lap Analysis section - mobile only, below leaderboard */}
        {isMobile && isRace && lapsResponse?.laps && (
          <div className="sm:hidden border-t border-f1-border" ref={(el) => {
            if (el && mobileLapAnalysisOpen) {
              setTimeout(() => el.scrollIntoView({ behavior: "smooth", block: "start" }), 100);
            }
          }}>
            <button
              onClick={() => setMobileLapAnalysisOpen(!mobileLapAnalysisOpen)}
              className="w-full flex items-center justify-between px-3 py-3 bg-f1-card border-b border-f1-border min-h-[44px]"
            >
              <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Lap Analysis</span>
              <svg className={`w-4 h-4 text-f1-muted transition-transform ${mobileLapAnalysisOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
              </svg>
            </button>
            {mobileLapAnalysisOpen && (
              <div className="bg-f1-card max-h-[60vh] overflow-y-auto">
                <LapAnalysisPanel laps={lapsResponse.laps} drivers={drivers} currentLap={Math.max(0, (replay.frame?.lap || 0) - 1)} />
              </div>
            )}
          </div>
        )}
      </div>

      {/* Playback controls */}
      <PlaybackControls
        playing={controlsUseQ3 ? q3PlaybackPlaying : replay.playing}
        speed={controlsUseQ3 ? q3PlaybackSpeed : replay.speed}
        currentTime={controlsUseQ3 ? q3PlaybackTime : (replay.frame?.timestamp || 0)}
        totalTime={controlsUseQ3 ? q3EffectiveTotalTime : effectiveTotalTime}
        currentLap={controlsUseQ3 ? 0 : (replay.frame?.lap || 0)}
        totalLaps={controlsUseQ3 ? 0 : replay.totalLaps}
        finished={controlsUseQ3 ? q3PlaybackFinished : replay.finished}
        showSessionTime={settings.showSessionTime}
        onPlay={controlsUseQ3 ? q3Play : replay.play}
        onPause={controlsUseQ3 ? q3Pause : replay.pause}
        onSpeedChange={controlsUseQ3 ? q3SetSpeed : replay.setSpeed}
        onSeek={controlsUseQ3 ? q3Seek : replay.seek}
        onSeekToLap={controlsUseQ3 ? undefined : replay.seekToLap}
        onReset={controlsUseQ3 ? q3Reset : replay.reset}
        isRace={controlsUseQ3 ? false : isRace}
        onPiP={!isMobile && !isIOS ? () => setPipActive(true) : undefined}
        pipActive={pipActive}
        onFullscreen={!isMobile ? () => {
          const next = !fullscreen;
          setFullscreen(next);
          if (next && document.documentElement.requestFullscreen) {
            document.documentElement.requestFullscreen();
          } else if (!next && document.fullscreenElement) {
            document.exitFullscreen();
          }
        } : undefined}
        fullscreen={fullscreen}
        qualiPhase={controlsUseQ3 ? null : replay.frame?.quali_phase}
        qualiPhases={controlsUseQ3 ? [] : replay.qualiPhases}
      />

      {/* Document PiP window — visible across tabs */}
      {pipActive && !isMobile && !isIOS && (
        <PiPWindow onClose={() => setPipActive(false)} width={400} height={780}>
          <div className="flex flex-col h-full bg-f1-dark overflow-hidden">
            {/* PiP Track Map */}
            <div className="flex-shrink-0">
              <button
                onClick={() => setPipTrackOpen(!pipTrackOpen)}
                className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
              >
                <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Track Map</span>
                <svg className={`w-4 h-4 text-f1-muted transition-transform ${pipTrackOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              {pipTrackOpen && (
                <div className="relative" style={{ height: "40vh" }}>
                  {trackStatus !== "green" && (
                    <div className="absolute top-2 left-1/2 -translate-x-1/2 z-10">
                      <div
                        className={`px-2 py-0.5 rounded text-[10px] font-extrabold uppercase ${
                          trackStatus === "red"
                            ? "bg-red-600 text-white"
                            : trackStatus === "sc"
                            ? "bg-yellow-500 text-black"
                            : trackStatus === "vsc"
                            ? "bg-yellow-500/80 text-black"
                            : "bg-yellow-400 text-black"
                        }`}
                      >
                        {trackStatus === "red"
                          ? "Red Flag"
                          : trackStatus === "sc"
                          ? "Safety Car"
                          : trackStatus === "vsc"
                          ? "Virtual Safety Car"
                          : "Yellow Flag"}
                      </div>
                    </div>
                  )}
                  <TrackCanvas
                    trackPoints={trackPoints}
                    rotation={rotation}
                    trackStatus={trackStatus}
                    drivers={compareActive ? [] : replayTrackDrivers}
                    highlightedDrivers={compareActive ? [] : selectedDrivers}
                    playbackSpeed={replay.speed}
                    showDriverNames={settings.showDriverNames}
                    sectorOverlay={sectorOverlay}
                    corners={settings.showCorners ? trackData?.corners : null}
                    marshalSectors={trackData?.marshal_sectors}
                    sectorFlags={replay.frame?.sector_flags}
                    q3CompareLines={compareActive ? q3CompareLines : null}
                    q3CompareElapsedSeconds={compareActive ? q3PlaybackTime : 0}
                  />
                </div>
              )}
            </div>

            {/* PiP Race Control */}
            <div className="flex-shrink-0 border-t border-f1-border">
              <button
                onClick={() => setPipRcOpen(!pipRcOpen)}
                className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
              >
                <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Race Control</span>
                <svg className={`w-4 h-4 text-f1-muted transition-transform ${pipRcOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              {pipRcOpen && (() => {
                const latest = (replay.frame?.rc_messages || [])[0];
                if (!latest) return <p className="text-f1-muted text-xs px-3 py-2">No messages yet</p>;
                const upper = latest.message.toUpperCase();
                const isPenalty = upper.includes("PENALTY") && !upper.includes("NO FURTHER");
                const isInvestigation = upper.includes("INVESTIGATION") || upper.includes("NOTED");
                const isCleared = upper.includes("NO FURTHER") || upper.includes("NO INVESTIGATION");
                return (
                  <div className="px-3 py-2">
                    <div className="flex items-start gap-2">
                      <div className={`w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0 ${
                        isPenalty ? "bg-red-500" : isInvestigation ? "bg-orange-400" : isCleared ? "bg-green-500" : "bg-f1-muted"
                      }`} />
                      <div className="min-w-0">
                        <p className="text-[11px] text-white leading-tight">{latest.message}</p>
                        {latest.lap && <span className="text-[9px] text-f1-muted">Lap {latest.lap}</span>}
                      </div>
                    </div>
                  </div>
                );
              })()}
            </div>

            {/* PiP Telemetry */}
            <div className="flex-shrink-0 border-t border-f1-border">
              <button
                onClick={() => setPipTelemetryOpen(!pipTelemetryOpen)}
                className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border"
              >
                <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Telemetry</span>
                <svg className={`w-4 h-4 text-f1-muted transition-transform ${pipTelemetryOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              {pipTelemetryOpen && (
                <div className="bg-f1-card px-3 py-2 space-y-1">
                  {selectedDrivers.length > 0 ? (
                    selectedDrivers.map((abbr) => {
                      const drv = drivers.find((d) => d.abbr === abbr) || null;
                      return <TelemetryChart key={abbr} visible driver={drv} year={year} isQualifying={isQualifying} useImperial={settings.useImperial} />;
                    })
                  ) : (
                    <TelemetryChart visible driver={null} year={year} useImperial={settings.useImperial} />
                  )}
                </div>
              )}
            </div>

            {/* PiP Leaderboard */}
            <div className="flex-1 min-h-0 flex flex-col border-t border-f1-border overflow-hidden">
              <button
                onClick={() => setPipLeaderboardOpen(!pipLeaderboardOpen)}
                className="w-full flex items-center justify-between px-3 py-2 bg-f1-card border-b border-f1-border flex-shrink-0"
              >
                <span className="text-[11px] font-bold text-f1-muted uppercase tracking-wider">Leaderboard</span>
                <svg className={`w-4 h-4 text-f1-muted transition-transform ${pipLeaderboardOpen ? "rotate-180" : ""}`} fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                  <path strokeLinecap="round" strokeLinejoin="round" d="M19 9l-7 7-7-7" />
                </svg>
              </button>
              {pipLeaderboardOpen && (
                <div className="flex-1 min-h-0 overflow-y-auto pb-[80px]">
                  <Leaderboard
                    drivers={drivers}
                    highlightedDrivers={selectedDrivers}
                    onDriverClick={handleDriverClick}
                    settings={settings}
                    currentTime={replay.frame?.timestamp || 0}
                    isRace={isRace}
                    isQualifying={isQualifying}
                    compact
                    lapData={lapData}
                    currentLap={replay.frame?.lap || 0}
                      />
                </div>
              )}
            </div>

            {/* PiP Playback Controls */}
            <div className="flex-shrink-0">
              <PlaybackControls
                playing={controlsUseQ3 ? q3PlaybackPlaying : replay.playing}
                speed={controlsUseQ3 ? q3PlaybackSpeed : replay.speed}
                currentTime={controlsUseQ3 ? q3PlaybackTime : (replay.frame?.timestamp || 0)}
                totalTime={controlsUseQ3 ? q3EffectiveTotalTime : effectiveTotalTime}
                currentLap={controlsUseQ3 ? 0 : (replay.frame?.lap || 0)}
                totalLaps={controlsUseQ3 ? 0 : replay.totalLaps}
                finished={controlsUseQ3 ? q3PlaybackFinished : replay.finished}
                showSessionTime={settings.showSessionTime}
                onPlay={controlsUseQ3 ? q3Play : replay.play}
                onPause={controlsUseQ3 ? q3Pause : replay.pause}
                onSpeedChange={controlsUseQ3 ? q3SetSpeed : replay.setSpeed}
                onSeek={controlsUseQ3 ? q3Seek : replay.seek}
                onSeekToLap={controlsUseQ3 ? undefined : replay.seekToLap}
                onReset={controlsUseQ3 ? q3Reset : replay.reset}
                isRace={controlsUseQ3 ? false : isRace}
                qualiPhase={controlsUseQ3 ? null : replay.frame?.quali_phase}
                qualiPhases={controlsUseQ3 ? [] : replay.qualiPhases}
              />
            </div>
          </div>
        </PiPWindow>
      )}

    </div>
  );
}
