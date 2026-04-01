"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import { useApi } from "@/hooks/useApi";
import { apiFetch, apiRequest } from "@/lib/api";

interface SeasonsResponse {
  seasons: number[];
}

interface SessionEntry {
  name: string;
  date_utc: string | null;
  available: boolean;
  session_type?: string;
  download_state?: "not_downloaded" | "queued" | "processing" | "downloaded" | "failed";
  downloaded?: boolean;
  last_error?: string;
}

interface Event {
  round_number: number;
  country: string;
  event_name: string;
  location: string;
  event_date: string;
  sessions: SessionEntry[];
  status: "latest" | "available" | "future";
}

interface EventsResponse {
  year: number;
  events: Event[];
}

interface DownloadJob {
  id: string;
  year: number;
  round: number;
  session_type: string;
  state: string;
  attempt: number;
  max_attempts: number;
  created_at: string;
  updated_at: string;
  started_at?: string;
  finished_at?: string;
  last_error?: string;
  message?: string;
  source?: string;
}

interface QueueSummary {
  queued: number;
  processing: number;
  recent_failed: number;
  recent_done: number;
}

interface QueueResponse {
  active_job: DownloadJob | null;
  queued_jobs: DownloadJob[];
  recent_jobs: DownloadJob[];
  summary: QueueSummary;
}

const SESSION_LABELS: Record<string, string> = {
  Race: "R",
  Qualifying: "Q",
  Sprint: "S",
  "Sprint Qualifying": "SQ",
  "Sprint Shootout": "SQ",
  "Practice 1": "FP1",
  "Practice 2": "FP2",
  "Practice 3": "FP3",
};

function formatDateTime(iso?: string): string {
  if (!iso) return "";
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "";
  return d.toLocaleString([], {
    month: "short",
    day: "numeric",
    hour: "2-digit",
    minute: "2-digit",
  });
}

function toSessionCode(value?: string): string {
  const raw = (value || "").trim();
  if (!raw) return "";
  return SESSION_LABELS[raw] || raw.toUpperCase();
}

export default function DownloadsPage() {
  const currentYear = new Date().getFullYear();
  const [year, setYear] = useState(currentYear);
  const [queueData, setQueueData] = useState<QueueResponse | null>(null);
  const [eventsData, setEventsData] = useState<EventsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [busyAction, setBusyAction] = useState<string | null>(null);

  const { data: seasonsData } = useApi<SeasonsResponse>("/api/seasons");
  const seasons = useMemo(() => (seasonsData?.seasons || []).filter((s) => s <= currentYear), [seasonsData, currentYear]);

  const refresh = useCallback(async () => {
    const [queueRes, eventsRes] = await Promise.all([
      apiFetch<QueueResponse>("/api/downloads/queue"),
      apiFetch<EventsResponse>(`/api/seasons/${year}/events`),
    ]);
    setQueueData(queueRes);
    setEventsData(eventsRes);
    setError(null);
  }, [year]);

  useEffect(() => {
    let cancelled = false;
    setLoading(true);
    refresh()
      .catch((err) => {
        if (!cancelled) setError(err.message || "Failed to load downloads");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });

    const timer = setInterval(() => {
      refresh().catch((err) => {
        if (!cancelled) setError(err.message || "Failed to refresh downloads");
      });
    }, 3000);

    return () => {
      cancelled = true;
      clearInterval(timer);
    };
  }, [refresh]);

  const enqueue = useCallback(async (mode: string, round?: number, sessionType?: string) => {
    const key = `${mode}:${round || ""}:${sessionType || ""}`;
    setBusyAction(key);
    try {
      await apiRequest("/api/downloads/enqueue", {
        method: "POST",
        body: JSON.stringify({
          mode,
          year,
          round,
          session_type: sessionType,
        }),
      });
      await refresh();
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to enqueue downloads");
    } finally {
      setBusyAction(null);
    }
  }, [refresh, year]);

  const retryFailed = useCallback(async () => {
    setBusyAction("retry_failed");
    try {
      await apiRequest("/api/downloads/retry-failed", {
        method: "POST",
        body: JSON.stringify({ year }),
      });
      await refresh();
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to retry failed jobs");
    } finally {
      setBusyAction(null);
    }
  }, [refresh, year]);

  const events = eventsData?.events || [];

  return (
    <div className="min-h-screen bg-f1-dark text-f1-text">
      <div className="bg-f1-card border-b border-f1-border">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 py-5 sm:py-6 flex items-center justify-between gap-4">
          <div>
            <h1 className="text-xl sm:text-2xl font-bold text-white">Downloads Manager</h1>
            <p className="text-f1-muted text-xs sm:text-sm">Queue and manage session downloads from UI</p>
          </div>
          <a
            href="/"
            className="px-4 py-2 bg-f1-border text-f1-muted text-sm font-bold rounded hover:text-white transition-colors"
          >
            Back Home
          </a>
        </div>
      </div>

      <div className="max-w-7xl mx-auto px-4 sm:px-6 py-6 space-y-6">
        {error && (
          <div className="px-4 py-3 rounded border border-red-500/40 bg-red-500/10 text-red-300 text-sm">
            {error}
          </div>
        )}

        <div className="flex gap-2 flex-wrap">
          {seasons.map((s) => (
            <button
              key={s}
              onClick={() => setYear(s)}
              className={`px-4 py-2 rounded-lg text-sm font-bold transition-colors ${
                year === s
                  ? "bg-f1-red text-white"
                  : "bg-f1-card text-f1-muted hover:text-white border border-f1-border"
              }`}
            >
              {s}
            </button>
          ))}
        </div>

        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3">
          <div className="bg-f1-card border border-f1-border rounded-lg p-4">
            <p className="text-f1-muted text-xs uppercase">Processing</p>
            <p className="text-white text-2xl font-bold">{queueData?.summary.processing || 0}</p>
          </div>
          <div className="bg-f1-card border border-f1-border rounded-lg p-4">
            <p className="text-f1-muted text-xs uppercase">Queued</p>
            <p className="text-white text-2xl font-bold">{queueData?.summary.queued || 0}</p>
          </div>
          <div className="bg-f1-card border border-f1-border rounded-lg p-4">
            <p className="text-f1-muted text-xs uppercase">Recent Done</p>
            <p className="text-white text-2xl font-bold">{queueData?.summary.recent_done || 0}</p>
          </div>
          <div className="bg-f1-card border border-f1-border rounded-lg p-4">
            <p className="text-f1-muted text-xs uppercase">Recent Failed</p>
            <p className="text-white text-2xl font-bold">{queueData?.summary.recent_failed || 0}</p>
          </div>
        </div>

        <div className="bg-f1-card border border-f1-border rounded-lg p-4 space-y-3">
          <p className="text-sm font-bold text-white">Bulk Actions ({year})</p>
          <div className="flex flex-wrap gap-2">
            <button
              onClick={() => void enqueue("season_all")}
              disabled={busyAction !== null}
              className="px-3 py-2 bg-blue-600 text-white text-xs font-bold rounded hover:bg-blue-500 disabled:opacity-50"
            >
              Download All Sessions
            </button>
            <button
              onClick={() => void enqueue("season_races")}
              disabled={busyAction !== null}
              className="px-3 py-2 bg-blue-600 text-white text-xs font-bold rounded hover:bg-blue-500 disabled:opacity-50"
            >
              Download All Races (R+S)
            </button>
            <button
              onClick={() => void enqueue("season_races_quali")}
              disabled={busyAction !== null}
              className="px-3 py-2 bg-blue-600 text-white text-xs font-bold rounded hover:bg-blue-500 disabled:opacity-50"
            >
              Download Races + Qualifying
            </button>
            <button
              onClick={() => void retryFailed()}
              disabled={busyAction !== null}
              className="px-3 py-2 bg-red-600 text-white text-xs font-bold rounded hover:bg-red-500 disabled:opacity-50"
            >
              Retry Failed
            </button>
          </div>
        </div>

        <div className="grid lg:grid-cols-3 gap-4">
          <div className="bg-f1-card border border-f1-border rounded-lg p-4 lg:col-span-2">
            <p className="text-sm font-bold text-white mb-3">Queue</p>
            {loading ? (
              <p className="text-f1-muted text-sm">Loading queue...</p>
            ) : (
              <div className="space-y-2">
                {queueData?.active_job ? (
                  <div className="p-3 rounded border border-yellow-500/40 bg-yellow-500/10">
                    <p className="text-yellow-200 text-sm font-bold">
                      Active: {queueData.active_job.year} R{queueData.active_job.round} {queueData.active_job.session_type}
                    </p>
                    <p className="text-f1-muted text-xs mt-1">
                      {queueData.active_job.message || "Processing..."}
                    </p>
                    <p className="text-f1-muted text-xs mt-1">
                      Attempt {queueData.active_job.attempt || 1} / {queueData.active_job.max_attempts || 1}
                    </p>
                  </div>
                ) : (
                  <p className="text-f1-muted text-sm">No active job</p>
                )}

                {(queueData?.queued_jobs || []).slice(0, 20).map((job, idx) => (
                  <div key={job.id} className="p-2 rounded border border-f1-border text-sm text-f1-muted">
                    #{idx + 1} {job.year} R{job.round} {job.session_type}
                  </div>
                ))}

                {(queueData?.queued_jobs || []).length === 0 && (
                  <p className="text-f1-muted text-sm">Queue is empty</p>
                )}
              </div>
            )}
          </div>

          <div className="bg-f1-card border border-f1-border rounded-lg p-4">
            <p className="text-sm font-bold text-white mb-3">Recent Jobs</p>
            <div className="space-y-2 max-h-[420px] overflow-y-auto">
              {(queueData?.recent_jobs || []).slice(0, 30).map((job) => (
                <div key={job.id} className="p-2 rounded border border-f1-border text-xs">
                  <p className="text-white font-bold">
                    {job.year} R{job.round} {toSessionCode(job.session_type)}
                  </p>
                  <p className={job.state === "failed" ? "text-red-300" : "text-green-300"}>{job.state}</p>
                  {job.message && <p className="text-f1-muted">{job.message}</p>}
                  {job.last_error && <p className="text-red-300">{job.last_error}</p>}
                  <p className="text-f1-muted">{formatDateTime(job.updated_at)}</p>
                  {(job.state === "downloaded" || job.state === "done" || job.state === "completed") && toSessionCode(job.session_type) && (
                    <a
                      href={`/replay/${job.year}/${job.round}?type=${toSessionCode(job.session_type)}`}
                      className="inline-block mt-2 px-2 py-1 bg-f1-border text-white rounded hover:bg-f1-red"
                    >
                      Open Replay
                    </a>
                  )}
                </div>
              ))}
              {(queueData?.recent_jobs || []).length === 0 && (
                <p className="text-f1-muted text-sm">No recent jobs yet</p>
              )}
            </div>
          </div>
        </div>

        <div className="bg-f1-card border border-f1-border rounded-lg p-4">
          <p className="text-sm font-bold text-white mb-4">Per-session Status ({year})</p>
          <div className="space-y-4">
            {events.map((evt) => (
              <div key={evt.round_number} className="border border-f1-border rounded-lg p-3">
                <div className="flex items-center justify-between gap-3 mb-3">
                  <p className="text-white font-bold">R{evt.round_number} {evt.event_name}</p>
                  <button
                    onClick={() => void enqueue("weekend", evt.round_number)}
                    disabled={busyAction !== null}
                    className="px-2 py-1 bg-f1-border text-f1-muted text-xs font-bold rounded hover:text-white disabled:opacity-50"
                  >
                    Download Weekend
                  </button>
                </div>
                <div className="flex flex-wrap gap-2">
                  {evt.sessions.map((s) => {
                    const code = s.session_type || SESSION_LABELS[s.name] || "";
                    if (!code) return null;
                    const state = s.download_state || (s.downloaded ? "downloaded" : "not_downloaded");
                    const stateCls =
                      state === "downloaded"
                        ? "bg-green-500/20 border-green-500/40 text-green-200"
                        : state === "processing"
                          ? "bg-yellow-500/20 border-yellow-500/40 text-yellow-200"
                          : state === "queued"
                            ? "bg-yellow-500/10 border-yellow-500/30 text-yellow-200"
                            : state === "failed"
                              ? "bg-red-500/20 border-red-500/40 text-red-200"
                              : "bg-f1-border/30 border-f1-border text-f1-muted";

                    return (
                      <div key={s.name} className={`px-2 py-2 rounded border text-xs min-w-[150px] ${stateCls}`}>
                        <p className="font-bold">{s.name}</p>
                        <p className="capitalize">{state.replaceAll("_", " ")}</p>
                        {state === "failed" && s.last_error && <p className="mt-1">{s.last_error}</p>}
                        <div className="mt-2">
                          {s.available ? (
                            state === "downloaded" ? (
                              <a
                                href={`/replay/${year}/${evt.round_number}?type=${code}`}
                                className="inline-block px-2 py-1 bg-f1-border text-white rounded hover:bg-f1-red"
                              >
                                Open Replay
                              </a>
                            ) : state === "queued" || state === "processing" ? (
                              <span className="inline-block px-2 py-1 bg-f1-border text-f1-muted rounded">In Queue</span>
                            ) : (
                              <button
                                onClick={() => void enqueue("session", evt.round_number, code)}
                                disabled={busyAction !== null}
                                className="px-2 py-1 bg-blue-600 text-white rounded hover:bg-blue-500 disabled:opacity-50"
                              >
                                {state === "failed" ? "Retry" : "Download"}
                              </button>
                            )
                          ) : (
                            <span className="inline-block px-2 py-1 bg-f1-border/30 text-f1-muted rounded">Not available yet</span>
                          )}
                        </div>
                      </div>
                    );
                  })}
                </div>
              </div>
            ))}
            {events.length === 0 && <p className="text-f1-muted text-sm">No season data available</p>}
          </div>
        </div>
      </div>
    </div>
  );
}
