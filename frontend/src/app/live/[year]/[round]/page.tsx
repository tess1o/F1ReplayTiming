"use client";

import { useState, useEffect } from "react";
import { useParams, useSearchParams } from "next/navigation";
import { useApi } from "@/hooks/useApi";
import { useLiveSocket } from "@/hooks/useLiveSocket";
import { useSettings } from "@/hooks/useSettings";
import { apiFetch } from "@/lib/api";
import SessionBanner from "@/components/SessionBanner";
import TrackCanvas from "@/components/TrackCanvas";
import Leaderboard from "@/components/Leaderboard";
import PiPWindow from "@/components/PiPWindow";

interface TrackData {
  track_points: { x: number; y: number }[];
  rotation: number;
  circuit_name: string;
  sector_boundaries?: { s1_end: number; s2_end: number; total: number } | null;
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

export default function LivePage() {
  const params = useParams();
  const searchParams = useSearchParams();
  const year = Number(params.year);
  const round = Number(params.round);
  const sessionType = searchParams.get("type") || "R";
  const speed = Number(searchParams.get("speed") || "10");
  const devMode = searchParams.get("dev") === "1";

  const [selectedDrivers, setSelectedDrivers] = useState<string[]>([]);
  const [isMobile, setIsMobile] = useState(false);
  const [mobileTrackOpen, setMobileTrackOpen] = useState(true);
  const [mobileLeaderboardOpen, setMobileLeaderboardOpen] = useState(true);
  const [leaderboardScale, setLeaderboardScale] = useState(1);
  const [delayOffset, setDelayOffset] = useState(0);
  const [showDelaySlider, setShowDelaySlider] = useState(false);
  const [checkingReplay, setCheckingReplay] = useState(false);
  const [replayCheckResult, setReplayCheckResult] = useState<string | null>(null);
  const [pipActive, setPipActive] = useState(false);
  const [pipTrackOpen, setPipTrackOpen] = useState(true);
  const [pipRcOpen, setPipRcOpen] = useState(true);
  const [pipLeaderboardOpen, setPipLeaderboardOpen] = useState(true);
  const [rcPanelOpen, setRcPanelOpen] = useState(false);
  const [rcPanelSize, setRcPanelSize] = useState<"sm" | "md" | "lg">("md");

  useEffect(() => {
    function check() { setIsMobile(window.innerWidth < 640); }
    check();
    window.addEventListener("resize", check);
    return () => window.removeEventListener("resize", check);
  }, []);

  function handleDriverClick(abbr: string) {
    setSelectedDrivers((prev) => {
      if (prev.includes(abbr)) {
        return prev.filter((d) => d !== abbr);
      }
      if (prev.length >= 2) {
        return [prev[1], abbr];
      }
      return [...prev, abbr];
    });
  }

  const { settings, update: updateSetting } = useSettings();

  const { data: sessionData, loading: sessionLoading, error: sessionError } = useApi<SessionData>(
    `/api/sessions/${year}/${round}?type=${sessionType}`,
  );

  const { data: trackData, loading: trackLoading, error: trackError } = useApi<TrackData>(
    `/api/sessions/${year}/${round}/track?type=${sessionType}`,
  );

  const live = useLiveSocket(year, round, sessionType, speed, delayOffset);

  const isLoading = sessionLoading || trackLoading;
  const dataError = sessionError || trackError;

  const isRace = sessionType === "R" || sessionType === "S";
  const isQualifying = sessionType === "Q" || sessionType === "SQ";

  // Show loading until live socket is ready
  if (isLoading || (!dataError && live.loading)) {
    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center">
          <div className="inline-block w-12 h-12 border-3 border-f1-muted border-t-f1-red rounded-full animate-spin mb-6" />
          <p className="text-f1-muted text-lg">Connecting to live timing...</p>
        </div>
      </div>
    );
  }

  if (live.error) {
    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center max-w-md">
          <p className="text-red-400 text-lg font-bold mb-2">Live Timing Unavailable</p>
          <p className="text-f1-muted mb-6 text-sm">{live.error}</p>
          <a href="/" className="inline-block px-4 py-2 bg-f1-red text-white font-bold text-sm rounded hover:bg-red-700 transition-colors">
            Back to session picker
          </a>
        </div>
      </div>
    );
  }

  if (dataError) {
    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center max-w-md">
          <p className="text-red-400 text-lg font-bold mb-2">Session Unavailable</p>
          <p className="text-f1-muted mb-6">Data for this session is not available.</p>
          <a href="/" className="inline-block px-4 py-2 bg-f1-red text-white font-bold text-sm rounded hover:bg-red-700 transition-colors">
            Back to session picker
          </a>
        </div>
      </div>
    );
  }

  // Session ended state
  if (live.sessionEnded) {
    async function checkReplayAvailable() {
      setCheckingReplay(true);
      setReplayCheckResult(null);
      try {
        await apiFetch(`/api/sessions/${year}/${round}?type=${sessionType}`);
        // If the fetch succeeds, replay data is available — navigate
        window.location.href = `/replay/${year}/${round}?type=${sessionType}`;
      } catch {
        setReplayCheckResult("Not available yet — data typically takes 15\u201330 minutes after session end.");
        setCheckingReplay(false);
      }
    }

    return (
      <div className="min-h-screen bg-f1-dark flex items-center justify-center">
        <div className="text-center max-w-md">
          <div className="inline-block px-3 py-1 bg-f1-card border border-f1-border rounded text-xs font-bold text-f1-muted uppercase mb-4">
            Session Ended
          </div>
          <p className="text-white text-lg font-bold mb-2">
            {sessionData?.event_name || "Session"} — {sessionType}
          </p>
          <p className="text-f1-muted mb-6 text-sm">
            Full replay with track positions and telemetry will be available shortly.
          </p>
          {replayCheckResult && (
            <p className="text-yellow-400 text-sm mb-4">{replayCheckResult}</p>
          )}
          <div className="flex gap-3 justify-center">
            <a href="/" className="inline-block px-4 py-2 bg-f1-card border border-f1-border text-white font-bold text-sm rounded hover:bg-f1-border transition-colors">
              Back to sessions
            </a>
            <button
              onClick={checkReplayAvailable}
              disabled={checkingReplay}
              className="inline-flex items-center gap-2 px-4 py-2 bg-f1-red text-white font-bold text-sm rounded hover:bg-red-700 transition-colors disabled:opacity-50"
            >
              {checkingReplay && (
                <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              )}
              Check for replay data
            </button>
          </div>
        </div>
      </div>
    );
  }

  const trackPoints = trackData?.track_points || [];
  const rotation = trackData?.rotation || 0;
  const drivers = live.frame?.drivers || [];
  const trackStatus = live.frame?.status || "green";
  const weather = live.frame?.weather;

  // Check if we have any position data for the track map
  const hasPositionData = drivers.some((d) => d.x !== 0 || d.y !== 0);

  // Calculate leaderboard width
  const leaderboardWidth = (() => {
    let w = 106;
    if (settings.showTeamAbbr) w += 28;
    if (!isRace) w += 18;
    if (isRace && settings.showGridChange) w += 24;
    if (settings.showGapToLeader) w += 56;
    if (isQualifying && settings.showSectors) w += 36;
    if (isRace && settings.showPitStops) w += 24;
    if (isRace && settings.showTyreHistory) w += 36;
    if (settings.showTyreType) w += 24;
    if (settings.showTyreAge) w += 20;
    if (isRace && settings.showPitPrediction) w += 40;
    if (isRace && settings.showPitPrediction && settings.showPitFreeAir) w += 36;
    return w;
  })();

  return (
    <div className="h-screen flex flex-col bg-f1-dark overflow-hidden">
      {/* Banner */}
      {sessionData && (
        <SessionBanner
          eventName={sessionData.event_name}
          circuit={sessionData.circuit}
          country={sessionData.country}
          sessionType={sessionType}
          year={year}
          settings={settings}
          onSettingChange={updateSetting}
          weather={weather}
          extraActions={
            <a
              href="/"
              className="px-2 py-1 sm:px-3 rounded text-[10px] sm:text-xs font-bold text-f1-muted hover:text-white hover:bg-white/10 transition-colors"
            >
              Exit
            </a>
          }
        />
      )}

      {/* Main content */}
      <div className="flex-1 flex flex-col sm:flex-row min-h-0 overflow-y-auto sm:overflow-hidden">
        {/* Track section */}
        <div className="sm:flex-1 relative">
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
            <div className="h-[42vh] sm:h-full relative">
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

              {/* LIVE badge + Race Control toggle */}
              <div className="absolute top-3 right-3 z-10 flex items-center gap-2">
                <button
                  onClick={() => setRcPanelOpen(!rcPanelOpen)}
                  className={`flex items-center gap-1 px-2 py-1 rounded text-xs font-bold transition-colors ${
                    rcPanelOpen
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
                <div className="flex items-center gap-1.5 px-2 py-1 bg-red-600 rounded text-xs font-extrabold text-white uppercase">
                  <span className="w-2 h-2 bg-white rounded-full animate-pulse" />
                  LIVE
                </div>
              </div>

              {/* Race Control Messages panel */}
              {rcPanelOpen && (
                <div className={`absolute top-12 right-3 z-20 w-80 bg-f1-card/95 border border-f1-border rounded-lg shadow-xl backdrop-blur-sm overflow-hidden flex flex-col ${
                  rcPanelSize === "sm" ? "max-h-[25%]" : rcPanelSize === "md" ? "max-h-[50%]" : "max-h-[85%]"
                }`}>
                  <div className="flex items-center justify-between px-3 py-2 border-b border-f1-border flex-shrink-0">
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
                      <button onClick={() => setRcPanelOpen(false)} className="text-f1-muted hover:text-white ml-1">
                        <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={2}>
                          <path strokeLinecap="round" strokeLinejoin="round" d="M6 18L18 6M6 6l12 12" />
                        </svg>
                      </button>
                    </div>
                  </div>
                  <div className="flex-1 overflow-y-auto divide-y divide-f1-border/50">
                    {(live.rcMessages || []).length === 0 ? (
                      <p className="text-f1-muted text-xs p-3 text-center">No race control messages yet</p>
                    ) : (
                      (rcPanelSize === "sm" ? (live.rcMessages || []).slice(0, 1) : (live.rcMessages || [])).map((rc, i) => {
                        const isInvestigation = rc.message.toUpperCase().includes("INVESTIGATION") || rc.message.toUpperCase().includes("NOTED");
                        const isPenalty = rc.message.toUpperCase().includes("PENALTY") && !rc.message.toUpperCase().includes("NO FURTHER");
                        const isCleared = rc.message.toUpperCase().includes("NO FURTHER") || rc.message.toUpperCase().includes("NO INVESTIGATION");
                        return (
                          <div key={i} className="px-3 py-2">
                            <div className="flex items-start gap-2">
                              <div className={`w-1.5 h-1.5 rounded-full mt-1.5 flex-shrink-0 ${
                                isPenalty ? "bg-red-500" : isInvestigation ? "bg-orange-400" : isCleared ? "bg-green-500" : "bg-f1-muted"
                              }`} />
                              <div className="min-w-0">
                                <p className="text-[11px] text-white leading-tight">{rc.message}</p>
                                {rc.lap && (
                                  <span className="text-[9px] text-f1-muted">Lap {rc.lap}</span>
                                )}
                              </div>
                            </div>
                          </div>
                        );
                      })
                    )}
                  </div>
                </div>
              )}

              {hasPositionData ? (
                <TrackCanvas
                  trackPoints={trackPoints}
                  rotation={rotation}
                  trackStatus={trackStatus}
                  drivers={drivers.filter((d) => !d.retired && !d.no_timing && (d.x !== 0 || d.y !== 0)).map((d) => ({
                    abbr: d.abbr,
                    x: d.x,
                    y: d.y,
                    color: d.color,
                    position: d.position,
                  }))}
                  highlightedDrivers={selectedDrivers}
                  playbackSpeed={1}
                  showDriverNames={settings.showDriverNames}
                />
              ) : (
                <div className="h-full flex items-center justify-center">
                  {trackPoints.length > 0 ? (
                    <div className="relative w-full h-full">
                      <TrackCanvas
                        trackPoints={trackPoints}
                        rotation={rotation}
                        trackStatus={trackStatus}
                        drivers={[]}
                        highlightedDrivers={[]}
                        playbackSpeed={1}
                        showDriverNames={false}
                      />
                      <div className="absolute inset-0 flex items-center justify-center">
                        <div className="bg-f1-card/90 border border-f1-border rounded-lg px-6 py-3 backdrop-blur-sm">
                          <p className="text-f1-muted text-sm text-center">
                            Track positions available in replay after session
                          </p>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <p className="text-f1-muted text-sm">Track data not available</p>
                  )}
                </div>
              )}
            </div>
          )}
        </div>

        {/* Leaderboard section */}
        {settings.showLeaderboard && (
          <div className={`flex-shrink-0 ${isMobile ? "" : "border-l"} border-f1-border`} style={{ width: isMobile ? "100%" : Math.ceil(leaderboardWidth * leaderboardScale) }}>
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
                currentTime={live.frame?.timestamp || 0}
                isRace={isRace}
                isQualifying={isQualifying}
                onScaleChange={setLeaderboardScale}
              />
            )}
          </div>
        )}
      </div>

      {/* Live info bar (replaces playback controls) */}
      <div className="flex-shrink-0 bg-f1-card border-t border-f1-border px-4 py-2 flex items-center justify-between">
        <div className="flex items-center gap-4">
          {/* Lap counter */}
          {isRace && live.frame && (
            <div className="text-sm">
              <span className="text-f1-muted">Lap </span>
              <span className="text-white font-bold">{live.frame.lap}</span>
              {live.frame.total_laps > 0 && (
                <span className="text-f1-muted">/{live.frame.total_laps}</span>
              )}
            </div>
          )}

          {/* Dev controls (test replayer only — add ?dev=1 to URL) */}
          {devMode && live.frame && (
            <div className="flex items-center gap-2">
              <span className="text-[10px] font-mono text-f1-muted">
                {(() => {
                  const t = live.frame.timestamp;
                  const h = Math.floor(t / 3600);
                  const m = Math.floor((t % 3600) / 60);
                  const s = Math.floor(t % 60);
                  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
                })()}
              </span>
              <div className="flex items-center gap-1">
                {[60, 300, 600, 1800].map((s) => (
                  <button
                    key={s}
                    onClick={() => live.send(JSON.stringify({ command: "skip", seconds: s }))}
                    className="px-1.5 py-0.5 bg-f1-dark border border-f1-border rounded text-[10px] font-bold text-f1-muted hover:text-white hover:border-f1-muted transition-colors"
                  >
                    +{s >= 60 ? `${s / 60}m` : `${s}s`}
                  </button>
                ))}
              </div>
            </div>
          )}

          {/* Qualifying phase */}
          {isQualifying && live.frame?.quali_phase && (
            <div className="text-sm">
              <span className="text-white font-bold">{live.frame.quali_phase.phase}</span>
              {live.frame.quali_phase.remaining > 0 && (
                <span className="text-f1-muted ml-2">
                  {Math.floor(live.frame.quali_phase.remaining / 60)}:
                  {String(Math.floor(live.frame.quali_phase.remaining % 60)).padStart(2, "0")}
                </span>
              )}
            </div>
          )}
        </div>

        <div className="flex items-center gap-3">
          {/* Broadcast delay */}
          <div className="relative">
            <button
              onClick={() => setShowDelaySlider(!showDelaySlider)}
              className={`px-2 py-1 border rounded text-[10px] font-bold transition-colors ${
                delayOffset !== 0
                  ? "bg-blue-500/20 border-blue-500/50 text-blue-300"
                  : "bg-f1-dark border-f1-border text-f1-muted hover:text-white"
              }`}
            >
              Delay: {delayOffset > 0 ? "+" : ""}{delayOffset}s
            </button>
            {showDelaySlider && (
              <div className="absolute bottom-full right-0 mb-2 bg-f1-card border border-f1-border rounded-lg p-3 shadow-xl z-50 w-56">
                <div className="flex items-center justify-between mb-2">
                  <span className="text-[10px] font-bold text-f1-muted uppercase">Broadcast Delay</span>
                  <button
                    onClick={() => { setDelayOffset(0); }}
                    className="text-[10px] text-f1-muted hover:text-white"
                  >
                    Reset
                  </button>
                </div>
                <input
                  type="range"
                  min={-10}
                  max={10}
                  step={0.5}
                  value={delayOffset}
                  onChange={(e) => setDelayOffset(Number(e.target.value))}
                  className="w-full h-1 bg-f1-border rounded-lg appearance-none cursor-pointer accent-blue-500"
                />
                <div className="flex justify-between text-[9px] text-f1-muted mt-1">
                  <span>-10s</span>
                  <span>0</span>
                  <span>+10s</span>
                </div>
                <p className="text-[9px] text-f1-muted mt-2">
                  Adjust to sync with your broadcast feed
                </p>
              </div>
            )}
          </div>

          {/* Live indicator + PiP */}
          <div className="flex items-center gap-2">
            <div className="flex items-center gap-1.5">
              <span className="w-2 h-2 bg-red-500 rounded-full animate-pulse" />
              <span className="text-xs font-bold text-red-400 uppercase">Live</span>
            </div>
            {!isMobile && (
              <button
                onClick={() => setPipActive(!pipActive)}
                className={`px-3 py-1.5 rounded border transition-colors text-xs font-bold ${
                  pipActive
                    ? "border-f1-red text-f1-red hover:bg-f1-red/10"
                    : "border-f1-border text-f1-muted hover:text-white hover:bg-white/10"
                }`}
                title="Picture-in-Picture"
              >
                <svg className="w-4 h-4 inline-block mr-1 -mt-0.5" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth={2} strokeLinecap="round" strokeLinejoin="round">
                  <rect x="2" y="3" width="20" height="14" rx="2" />
                  <rect x="12" y="10" width="10" height="10" rx="1" fill="currentColor" opacity="0.3" />
                </svg>
                PiP
              </button>
            )}
          </div>
        </div>
      </div>

      {/* PiP window */}
      {pipActive && !isMobile && (
        <PiPWindow onClose={() => setPipActive(false)} width={400} height={720}>
          <div className="flex flex-col h-full bg-f1-dark">
            {/* PiP Track Map */}
            <div>
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
                    drivers={[]}
                    highlightedDrivers={selectedDrivers}
                    showDriverNames={settings.showDriverNames}
                  />
                </div>
              )}
            </div>

            {/* PiP Race Control */}
            <div className="border-t border-f1-border">
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
                const latest = (live.rcMessages || [])[0];
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

            {/* PiP Leaderboard */}
            <div className="flex-1 min-h-0 flex flex-col border-t border-f1-border">
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
                <div className="flex-1 min-h-0 overflow-y-auto">
                  <Leaderboard
                    drivers={drivers}
                    highlightedDrivers={selectedDrivers}
                    onDriverClick={handleDriverClick}
                    settings={settings}
                    currentTime={live.frame?.timestamp || 0}
                    isRace={isRace}
                    isQualifying={isQualifying}
                    compact
                  />
                </div>
              )}
            </div>
          </div>
        </PiPWindow>
      )}
    </div>
  );
}
