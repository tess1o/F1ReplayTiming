import type { Q3CompareLine, Q3DominanceOverlay } from "@/lib/trackRenderer";

export interface Q3LineSample {
  x: number;
  y: number;
  t: number;
  p: number;
}

export interface Q3LineDriver {
  abbr: string;
  driver_number: string;
  team: string;
  color: string;
  lap_number: number;
  lap_time: string;
  lap_time_seconds: number;
  sector1?: string | null;
  sector2?: string | null;
  sector3?: string | null;
  sector_colors?: {
    s1?: "purple" | "green" | "yellow" | null;
    s2?: "purple" | "green" | "yellow" | null;
    s3?: "purple" | "green" | "yellow" | null;
  } | null;
  phase: "Q3";
  samples: Q3LineSample[];
}

export interface Q3LinesData {
  phase: "Q3";
  generated_at: string;
  default_pair: string[];
  drivers: Q3LineDriver[];
}

export interface Q3SectorCell {
  raw: string;
  seconds: number | null;
  split: number | null;
  revealed: boolean;
  tone: "purple" | "green" | "yellow" | null;
}

export interface Q3SectorRow {
  s1: Q3SectorCell;
  s2: Q3SectorCell;
  s3: Q3SectorCell;
}

interface Q3SectorDeltaCell {
  ready: boolean;
  text: string;
  signed: number | null;
}

export interface Q3SectorDelta {
  s1: Q3SectorDeltaCell;
  s2: Q3SectorDeltaCell;
  s3: Q3SectorDeltaCell;
}

export interface Q3LapDelta {
  signed: number;
  text: string;
}

export interface Q3LiveDelta {
  deltaSeconds: number;
  text: string;
}

export interface Q3DominanceBuildOptions {
  microSectorCount?: number;
  tieThresholdSeconds?: number;
  speedTieThresholdKph?: number;
  tieColor?: string;
  speedByDriver?: Record<string, { speed?: number[] | null } | null | undefined> | null;
}

function normalizeHexColor(input: string): string {
  const raw = (input || "").trim();
  if (!raw) return "#FFFFFF";
  const withHash = raw.startsWith("#") ? raw : `#${raw}`;
  if (withHash.length === 4) {
    const r = withHash[1];
    const g = withHash[2];
    const b = withHash[3];
    return `#${r}${r}${g}${g}${b}${b}`.toUpperCase();
  }
  if (withHash.length === 7) return withHash.toUpperCase();
  return "#FFFFFF";
}

function hexToRgb(input: string): { r: number; g: number; b: number } {
  const hex = normalizeHexColor(input);
  return {
    r: Number.parseInt(hex.slice(1, 3), 16),
    g: Number.parseInt(hex.slice(3, 5), 16),
    b: Number.parseInt(hex.slice(5, 7), 16),
  };
}

function pickHighContrastColor(base: string): string {
  const candidates = ["#F59E0B", "#22D3EE", "#A3E635", "#F472B6", "#38BDF8"];
  const a = hexToRgb(base);
  let best = candidates[0];
  let bestScore = -1;
  for (const candidate of candidates) {
    const b = hexToRgb(candidate);
    const score = Math.hypot(a.r - b.r, a.g - b.g, a.b - b.b);
    if (score > bestScore) {
      best = candidate;
      bestScore = score;
    }
  }
  return best;
}

function parseTimingValueToSeconds(raw: string | null | undefined): number | null {
  const value = (raw || "").trim();
  if (!value) return null;
  const parts = value.split(":");
  if (parts.length === 1) {
    const sec = Number.parseFloat(parts[0]);
    return Number.isFinite(sec) && sec >= 0 ? sec : null;
  }
  if (parts.length === 2) {
    const mins = Number.parseInt(parts[0], 10);
    const sec = Number.parseFloat(parts[1]);
    if (!Number.isFinite(mins) || !Number.isFinite(sec) || mins < 0 || sec < 0) return null;
    return mins * 60 + sec;
  }
  return null;
}

function normalizeQ3SamplesForPlayback(samples: Q3LineSample[], lapTimeSeconds: number): Q3LineSample[] {
  if (!samples || samples.length === 0 || !Number.isFinite(lapTimeSeconds) || lapTimeSeconds <= 0) return samples || [];
  const ordered = [...samples];
  if (ordered.length === 1) {
    return [{ ...ordered[0], p: 0, t: 0 }];
  }

  const cumulative: number[] = new Array(ordered.length).fill(0);
  let totalDist = 0;
  for (let i = 1; i < ordered.length; i++) {
    const dx = ordered[i].x - ordered[i - 1].x;
    const dy = ordered[i].y - ordered[i - 1].y;
    totalDist += Math.hypot(dx, dy);
    cumulative[i] = totalDist;
  }
  const fallbackDen = Math.max(ordered.length - 1, 1);
  let prevT = 0;
  return ordered.map((sample, idx) => {
    const rawProgress = totalDist > 1e-6 ? cumulative[idx] / totalDist : idx / fallbackDen;
    const p = Math.max(0, Math.min(1, rawProgress));
    let t = p * lapTimeSeconds;
    if (idx > 0 && t < prevT) {
      t = prevT;
    }
    prevT = t;
    return { ...sample, p, t };
  });
}

function interpolateQ3StateAtSampleTime(samples: Q3LineSample[], sampleTime: number): { p: number; t: number } | null {
  if (!samples || samples.length === 0) return null;
  if (samples.length === 1) return { p: samples[0].p, t: samples[0].t };
  if (sampleTime <= samples[0].t) return { p: samples[0].p, t: samples[0].t };
  const last = samples[samples.length - 1];
  if (sampleTime >= last.t) return { p: last.p, t: last.t };

  for (let i = 1; i < samples.length; i++) {
    const curr = samples[i];
    if (sampleTime > curr.t) continue;
    const prev = samples[i - 1];
    const span = Math.max(curr.t - prev.t, 1e-6);
    const ratio = (sampleTime - prev.t) / span;
    return {
      p: prev.p + (curr.p - prev.p) * ratio,
      t: sampleTime,
    };
  }

  return { p: last.p, t: last.t };
}

function mapReplayToSampleTime(samples: Q3LineSample[], lapTimeSeconds: number, replayTime: number): number | null {
  if (!samples || samples.length === 0) return null;
  const first = samples[0].t;
  const last = samples[samples.length - 1].t;
  const span = Math.max(last - first, 1e-6);
  const lapSpan = Math.max(lapTimeSeconds, 1e-6);
  const clampedReplay = Math.max(0, Math.min(lapTimeSeconds, replayTime));
  const normalized = clampedReplay / lapSpan;
  return first + normalized * span;
}

function mapSampleToReplayTime(samples: Q3LineSample[], lapTimeSeconds: number, sampleTime: number): number | null {
  if (!samples || samples.length === 0) return null;
  const first = samples[0].t;
  const last = samples[samples.length - 1].t;
  const span = Math.max(last - first, 1e-6);
  const normalized = Math.max(0, Math.min(1, (sampleTime - first) / span));
  return normalized * Math.max(lapTimeSeconds, 0);
}

function normalizeSampleProgress(samples: Q3LineSample[], progress: number): number {
  if (!samples || samples.length === 0) return 0;
  const first = samples[0].p;
  const last = samples[samples.length - 1].p;
  const span = Math.max(last - first, 1e-6);
  return Math.max(0, Math.min(1, (progress - first) / span));
}

function denormalizeSampleProgress(samples: Q3LineSample[], normalizedProgress: number): number {
  if (!samples || samples.length === 0) return 0;
  const first = samples[0].p;
  const last = samples[samples.length - 1].p;
  const span = Math.max(last - first, 1e-6);
  const clamped = Math.max(0, Math.min(1, normalizedProgress));
  return first + clamped * span;
}

function timeAtProgress(samples: Q3LineSample[], targetProgress: number, lapTimeSeconds: number): number | null {
  if (!samples || samples.length === 0) return null;
  if (samples.length === 1) return mapSampleToReplayTime(samples, lapTimeSeconds, samples[0].t);

  const rawTargetProgress = denormalizeSampleProgress(samples, targetProgress);
  if (rawTargetProgress <= samples[0].p) return mapSampleToReplayTime(samples, lapTimeSeconds, samples[0].t);
  const last = samples[samples.length - 1];
  if (rawTargetProgress >= last.p) return mapSampleToReplayTime(samples, lapTimeSeconds, last.t);

  for (let i = 1; i < samples.length; i++) {
    const curr = samples[i];
    if (rawTargetProgress > curr.p) continue;
    const prev = samples[i - 1];
    const span = Math.max(curr.p - prev.p, 1e-6);
    const ratio = (rawTargetProgress - prev.p) / span;
    const sampleTime = prev.t + (curr.t - prev.t) * ratio;
    return mapSampleToReplayTime(samples, lapTimeSeconds, sampleTime);
  }

  return mapSampleToReplayTime(samples, lapTimeSeconds, last.t);
}

export function buildQ3CompareLines(
  enabled: boolean,
  selected: [string | null, string | null],
  driverMap: Map<string, Q3LineDriver>,
): Q3CompareLine[] {
  if (!enabled) return [];
  const [a, b] = selected;
  if (!a || !b || a === b) return [];
  const d1 = driverMap.get(a);
  const d2 = driverMap.get(b);
  if (!d1 || !d2) return [];

  const c1 = normalizeHexColor(d1.color);
  const c2 = normalizeHexColor(d2.color);
  const sameColor = c1 === c2;
  const c2Adjusted = sameColor ? pickHighContrastColor(c2) : c2;

  return [
    {
      abbr: d1.abbr,
      color: c1,
      lapTimeSeconds: d1.lap_time_seconds,
      samples: normalizeQ3SamplesForPlayback(d1.samples || [], d1.lap_time_seconds),
    },
    {
      abbr: d2.abbr,
      color: c2Adjusted,
      lapTimeSeconds: d2.lap_time_seconds,
      samples: normalizeQ3SamplesForPlayback(d2.samples || [], d2.lap_time_seconds),
      lineDash: sameColor ? [8, 6] : undefined,
      markerStyle: sameColor ? "outlined" : "solid",
    },
  ];
}

function clampMicroSectorCount(value: number | undefined): number {
  if (!Number.isFinite(value)) return 300;
  return Math.max(20, Math.min(2000, Math.floor(value || 300)));
}

export function buildQ3DominanceOverlay(
  enabled: boolean,
  lines: Q3CompareLine[],
  options: Q3DominanceBuildOptions = {},
): Q3DominanceOverlay | null {
  if (!enabled || lines.length !== 2) return null;
  const [d1, d2] = lines;
  if (!d1.samples?.length || !d2.samples?.length || d1.lapTimeSeconds <= 0 || d2.lapTimeSeconds <= 0) {
    return null;
  }

  const microSectorCount = clampMicroSectorCount(options.microSectorCount);
  const tieThreshold = Number.isFinite(options.tieThresholdSeconds) ? Math.max(0, options.tieThresholdSeconds || 0) : 5e-4;
  const tieColor = options.tieColor || "#9CA3AF";
  const speedByDriver = options.speedByDriver || null;
  const d1Speed = speedByDriver?.[d1.abbr]?.speed || null;
  const d2Speed = speedByDriver?.[d2.abbr]?.speed || null;

  const buildTelemetryProgressTimeProfile = (speed: number[] | null, lapTimeSeconds: number): Array<{ p: number; t: number; v: number }> | null => {
    if (!speed || speed.length < 2 || !Number.isFinite(lapTimeSeconds) || lapTimeSeconds <= 0) return null;
    const n = speed.length;
    const dt = lapTimeSeconds / (n - 1);
    if (!Number.isFinite(dt) || dt <= 0) return null;

    const cumulativeDistance = new Array<number>(n).fill(0);
    let totalDistance = 0;
    const toMps = (vKmh: number) => (Number.isFinite(vKmh) ? Math.max(0, vKmh) / 3.6 : 0);

    for (let i = 1; i < n; i++) {
      const v0 = toMps(speed[i - 1]);
      const v1 = toMps(speed[i]);
      const stepDistance = ((v0 + v1) * 0.5) * dt;
      totalDistance += stepDistance;
      cumulativeDistance[i] = totalDistance;
    }

    if (!Number.isFinite(totalDistance) || totalDistance <= 1e-6) return null;

    const profile: Array<{ p: number; t: number; v: number }> = new Array(n);
    for (let i = 0; i < n; i++) {
      const v = Number.isFinite(speed[i]) ? Math.max(0, speed[i]) : 0;
      profile[i] = {
        p: Math.max(0, Math.min(1, cumulativeDistance[i] / totalDistance)),
        t: i * dt,
        v,
      };
    }
    profile[0].p = 0;
    profile[n - 1].p = 1;
    return profile;
  };

  const timeAtTelemetryProgress = (
    profile: Array<{ p: number; t: number; v: number }>,
    targetProgress: number,
  ): number | null => {
    if (!profile.length) return null;
    const target = Math.max(0, Math.min(1, targetProgress));
    if (target <= profile[0].p) return profile[0].t;

    const last = profile[profile.length - 1];
    if (target >= last.p) return last.t;

    for (let i = 1; i < profile.length; i++) {
      const curr = profile[i];
      if (target > curr.p) continue;
      const prev = profile[i - 1];
      const span = curr.p - prev.p;
      if (span <= 1e-9) return curr.t;
      const ratio = (target - prev.p) / span;
      return prev.t + (curr.t - prev.t) * ratio;
    }

    return last.t;
  };

  const speedAtTelemetryProgress = (
    profile: Array<{ p: number; t: number; v: number }>,
    targetProgress: number,
  ): number | null => {
    if (!profile.length) return null;
    const target = Math.max(0, Math.min(1, targetProgress));
    if (target <= profile[0].p) return profile[0].v;

    const last = profile[profile.length - 1];
    if (target >= last.p) return last.v;

    for (let i = 1; i < profile.length; i++) {
      const curr = profile[i];
      if (target > curr.p) continue;
      const prev = profile[i - 1];
      const span = curr.p - prev.p;
      if (span <= 1e-9) return curr.v;
      const ratio = (target - prev.p) / span;
      return prev.v + (curr.v - prev.v) * ratio;
    }

    return last.v;
  };

  const d1Profile = buildTelemetryProgressTimeProfile(d1Speed, d1.lapTimeSeconds);
  const d2Profile = buildTelemetryProgressTimeProfile(d2Speed, d2.lapTimeSeconds);
  if (!d1Profile || !d2Profile) return null;
  const speedTieThreshold = Number.isFinite(options.speedTieThresholdKph)
    ? Math.max(0, options.speedTieThresholdKph || 0)
    : 2.0;

  const segments: Q3DominanceOverlay["segments"] = [];
  let aWins = 0;
  let bWins = 0;
  let ties = 0;

  for (let i = 0; i < microSectorCount; i++) {
    const startProgress = i / microSectorCount;
    const endProgress = (i + 1) / microSectorCount;

    const d1Start = timeAtTelemetryProgress(d1Profile, startProgress);
    const d1End = timeAtTelemetryProgress(d1Profile, endProgress);
    const d2Start = timeAtTelemetryProgress(d2Profile, startProgress);
    const d2End = timeAtTelemetryProgress(d2Profile, endProgress);
    if (d1Start == null || d1End == null || d2Start == null || d2End == null) continue;

    const dt1 = d1End - d1Start;
    const dt2 = d2End - d2Start;
    if (!Number.isFinite(dt1) || !Number.isFinite(dt2)) continue;

    const centerProgress = (startProgress + endProgress) * 0.5;
    const v1 = speedAtTelemetryProgress(d1Profile, centerProgress);
    const v2 = speedAtTelemetryProgress(d2Profile, centerProgress);

    let winner: "a" | "b" | "tie";
    if (v1 != null && v2 != null && Number.isFinite(v1) && Number.isFinite(v2)) {
      const dv = v1 - v2;
      if (Math.abs(dv) <= speedTieThreshold) {
        const delta = dt1 - dt2;
        winner = Math.abs(delta) <= tieThreshold ? "tie" : delta < 0 ? "a" : "b";
      } else {
        winner = dv > 0 ? "a" : "b";
      }
    } else {
      const delta = dt1 - dt2;
      winner = Math.abs(delta) <= tieThreshold ? "tie" : delta < 0 ? "a" : "b";
    }
    const color = winner === "a" ? d1.color : winner === "b" ? d2.color : tieColor;

    if (winner === "a") aWins++;
    else if (winner === "b") bWins++;
    else ties++;

    const prev = segments[segments.length - 1];
    if (prev && prev.winner === winner) {
      prev.endProgress = endProgress;
    } else {
      segments.push({
        startProgress,
        endProgress,
        winner,
        color,
      });
    }
  }

  const total = aWins + bWins + ties;
  if (segments.length === 0 || total === 0) return null;

  return {
    segments,
    summary: {
      aAbbr: d1.abbr,
      bAbbr: d2.abbr,
      aWins,
      bWins,
      ties,
      total,
    },
  };
}

export function computeQ3LiveDelta(lines: Q3CompareLine[], playbackTime: number): Q3LiveDelta | null {
  if (lines.length !== 2) return null;
  const d1 = lines[0];
  const d2 = lines[1];

  const d1Now = Math.min(playbackTime, d1.lapTimeSeconds);
  const d2Now = Math.min(playbackTime, d2.lapTimeSeconds);
  const d1SampleTime = mapReplayToSampleTime(d1.samples, d1.lapTimeSeconds, d1Now);
  const d2SampleTime = mapReplayToSampleTime(d2.samples, d2.lapTimeSeconds, d2Now);
  if (d1SampleTime == null || d2SampleTime == null) return null;

  const d1State = interpolateQ3StateAtSampleTime(d1.samples, d1SampleTime);
  const d2State = interpolateQ3StateAtSampleTime(d2.samples, d2SampleTime);
  if (!d1State || !d2State) return null;

  const d1Progress = normalizeSampleProgress(d1.samples, d1State.p);
  const d2Progress = normalizeSampleProgress(d2.samples, d2State.p);
  const compareProgress = Math.min(d1Progress, d2Progress);
  const d1AtCompare = timeAtProgress(d1.samples, compareProgress, d1.lapTimeSeconds);
  const d2AtCompare = timeAtProgress(d2.samples, compareProgress, d2.lapTimeSeconds);
  const signed = (d1AtCompare ?? d1Now) - (d2AtCompare ?? d2Now);

  const leader =
    Math.abs(signed) < 5e-4
      ? "LEVEL"
      : signed < 0
      ? `${d1.abbr} ahead`
      : `${d2.abbr} ahead`;

  return {
    deltaSeconds: signed,
    text: `${signed >= 0 ? "+" : ""}${signed.toFixed(3)}s • ${leader}`,
  };
}

export function buildQ3SectorReveal(
  lines: Q3CompareLine[],
  driverMap: Map<string, Q3LineDriver>,
  playbackTime: number,
): Map<string, Q3SectorRow> {
  const reveal = new Map<string, Q3SectorRow>();
  for (const line of lines) {
    const meta = driverMap.get(line.abbr);
    if (!meta) continue;
    const s1Raw = (meta.sector1 || "").trim();
    const s2Raw = (meta.sector2 || "").trim();
    const s3Raw = (meta.sector3 || "").trim();
    const s1 = parseTimingValueToSeconds(s1Raw);
    const s2 = parseTimingValueToSeconds(s2Raw);
    const s3 = parseTimingValueToSeconds(s3Raw);
    const split1 = s1;
    const split2 = s1 != null && s2 != null ? s1 + s2 : null;
    const split3 = s1 != null && s2 != null && s3 != null ? s1 + s2 + s3 : null;
    const elapsed = Math.min(playbackTime, line.lapTimeSeconds);

    const colors = meta.sector_colors || {};
    const buildCell = (
      raw: string,
      seconds: number | null,
      split: number | null,
      tone: "purple" | "green" | "yellow" | null,
    ): Q3SectorCell => ({
      raw,
      seconds,
      split,
      revealed: split != null && elapsed+1e-3 >= split,
      tone,
    });

    reveal.set(line.abbr, {
      s1: buildCell(s1Raw, s1, split1, colors.s1 || null),
      s2: buildCell(s2Raw, s2, split2, colors.s2 || null),
      s3: buildCell(s3Raw, s3, split3, colors.s3 || null),
    });
  }
  return reveal;
}

export function computeQ3SectorDelta(
  lines: Q3CompareLine[],
  sectorReveal: Map<string, Q3SectorRow>,
): Q3SectorDelta | null {
  if (lines.length !== 2) return null;
  const d1 = sectorReveal.get(lines[0].abbr);
  const d2 = sectorReveal.get(lines[1].abbr);
  if (!d1 || !d2) return null;

  const compare = (a: Q3SectorCell, b: Q3SectorCell): Q3SectorDeltaCell => {
    if (!a.revealed || !b.revealed || a.seconds == null || b.seconds == null) {
      return { ready: false, text: "…", signed: null };
    }
    const signed = a.seconds - b.seconds;
    if (Math.abs(signed) < 5e-4) {
      return { ready: true, text: "0.000", signed: 0 };
    }
    return { ready: true, text: `${signed >= 0 ? "+" : ""}${signed.toFixed(3)}`, signed };
  };

  return {
    s1: compare(d1.s1, d2.s1),
    s2: compare(d1.s2, d2.s2),
    s3: compare(d1.s3, d2.s3),
  };
}

export function computeQ3LapDelta(lines: Q3CompareLine[]): Q3LapDelta | null {
  if (lines.length !== 2) return null;
  const signed = lines[0].lapTimeSeconds - lines[1].lapTimeSeconds;
  return {
    signed,
    text: `${signed >= 0 ? "+" : ""}${signed.toFixed(3)}`,
  };
}
