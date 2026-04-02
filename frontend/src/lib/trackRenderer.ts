export interface TrackPoint {
  x: number;
  y: number;
}

export interface DriverMarker {
  abbr: string;
  x: number;
  y: number;
  color: string;
  position: number | null;
}

export interface Corner {
  x: number;
  y: number;
  number: number;
  letter: string;
  angle: number;
}

export interface MarshalSector {
  x: number;
  y: number;
  number: number;
}

export interface SectorFlag {
  sector: number;
  flag: string;
  driver: string;
}

export interface SectorOverlay {
  boundaries: { s1_end: number; s2_end: number; total: number };
  colors: { s1: string; s2: string; s3: string };
}

export interface Q3LineSample {
  x: number;
  y: number;
  t: number;
  p: number;
}

export interface Q3CompareLine {
  abbr: string;
  color: string;
  lapTimeSeconds: number;
  samples: Q3LineSample[];
  lineDash?: number[];
  markerStyle?: "solid" | "outlined";
}

const TRACK_STATUS_COLORS: Record<string, string> = {
  green: "#3A3A4A",
  yellow: "#F5C518",
  sc: "#F5C518",
  vsc: "#F5C518",
  red: "#E10600",
};

export function drawTrack(
  ctx: CanvasRenderingContext2D,
  points: TrackPoint[],
  width: number,
  height: number,
  rotation: number,
  trackStatus: string = "green",
  sectorOverlay?: SectorOverlay | null,
  corners?: Corner[] | null,
  marshalSectors?: MarshalSector[] | null,
  sectorFlags?: SectorFlag[] | null,
) {
  if (points.length === 0) return;

  const padX = 40;
  const padTop = 60;
  const padBottom = 90;
  const w = width - padX * 2;
  const h = height - padTop - padBottom;

  // Rotation is pre-applied in the backend; keep for any future manual override
  const rad = (rotation * Math.PI) / 180;
  const cos = Math.cos(rad);
  const sin = Math.sin(rad);

  // Center of the normalized track
  const cx = 0.5;
  const cy = 0.5;

  const rotated = points.map((p) => {
    const dx = p.x - cx;
    const dy = p.y - cy;
    return {
      x: dx * cos - dy * sin + cx,
      y: dx * sin + dy * cos + cy,
    };
  });

  // Find bounds after rotation
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const p of rotated) {
    minX = Math.min(minX, p.x);
    maxX = Math.max(maxX, p.x);
    minY = Math.min(minY, p.y);
    maxY = Math.max(maxY, p.y);
  }

  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;
  const scale = Math.min(w / rangeX, h / rangeY);

  const offsetX = padX + (w - rangeX * scale) / 2;
  const offsetY = padTop + (h - rangeY * scale) / 2;

  function toScreen(p: TrackPoint): [number, number] {
    return [
      offsetX + (p.x - minX) * scale,
      offsetY + (maxY - p.y) * scale, // Flip Y: data Y-up → screen Y-down
    ];
  }

  // Draw track outline (optionally colored by sector)
  if (sectorOverlay) {
    const { boundaries, colors } = sectorOverlay;
    const segments = [
      { start: 0, end: boundaries.s1_end, color: colors.s1 },
      { start: boundaries.s1_end, end: boundaries.s2_end, color: colors.s2 },
      { start: boundaries.s2_end, end: rotated.length - 1, color: colors.s3 },
    ];
    // Draw base track first (so gaps between segments aren't visible)
    ctx.beginPath();
    ctx.strokeStyle = "#3A3A4A";
    ctx.lineWidth = 12;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";
    const [bx, by] = toScreen(rotated[0]);
    ctx.moveTo(bx, by);
    for (let i = 1; i < rotated.length; i++) {
      const [px, py] = toScreen(rotated[i]);
      ctx.lineTo(px, py);
    }
    ctx.closePath();
    ctx.stroke();

    // Draw colored sector segments on top
    for (const seg of segments) {
      ctx.beginPath();
      ctx.strokeStyle = seg.color;
      ctx.lineWidth = 12;
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      const [sx2, sy2] = toScreen(rotated[seg.start]);
      ctx.moveTo(sx2, sy2);
      for (let i = seg.start + 1; i <= seg.end && i < rotated.length; i++) {
        const [px, py] = toScreen(rotated[i]);
        ctx.lineTo(px, py);
      }
      ctx.stroke();
    }
  } else {
    ctx.beginPath();
    const effectiveStatus = (sectorFlags && sectorFlags.length > 0 && (trackStatus === "yellow")) ? "green" : trackStatus;
    ctx.strokeStyle = TRACK_STATUS_COLORS[effectiveStatus] || "#3A3A4A";
    ctx.lineWidth = 12;
    ctx.lineCap = "round";
    ctx.lineJoin = "round";

    const [sx, sy] = toScreen(rotated[0]);
    ctx.moveTo(sx, sy);
    for (let i = 1; i < rotated.length; i++) {
      const [px, py] = toScreen(rotated[i]);
      ctx.lineTo(px, py);
    }
    ctx.closePath();
    ctx.stroke();
  }

  // Draw track center line
  ctx.beginPath();
  ctx.strokeStyle = "#4A4A5A";
  ctx.lineWidth = 2;
  const [sx, sy] = toScreen(rotated[0]);
  ctx.moveTo(sx, sy);
  for (let i = 1; i < rotated.length; i++) {
    const [px, py] = toScreen(rotated[i]);
    ctx.lineTo(px, py);
  }
  ctx.closePath();
  ctx.stroke();

  // Start/finish marker  - drawn perpendicular to track direction
  const [fx, fy] = toScreen(rotated[0]);
  const [nx, ny] = toScreen(rotated[1]);
  const trackAngle = Math.atan2(ny - fy, nx - fx);
  const perpAngle = trackAngle + Math.PI / 2;
  const markerLen = 8;
  ctx.beginPath();
  ctx.moveTo(fx - Math.cos(perpAngle) * markerLen, fy - Math.sin(perpAngle) * markerLen);
  ctx.lineTo(fx + Math.cos(perpAngle) * markerLen, fy + Math.sin(perpAngle) * markerLen);
  ctx.strokeStyle = "#FFFFFF";
  ctx.lineWidth = 3;
  ctx.lineCap = "round";
  ctx.stroke();

  // Corner labels
  if (corners && corners.length > 0) {
    ctx.font = "bold 10px sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";

    for (const c of corners) {
      // Apply same rotation as track points
      const dx = c.x - cx;
      const dy = c.y - cy;
      const rx = dx * cos - dy * sin + cx;
      const ry = dx * sin + dy * cos + cy;
      const [screenX, screenY] = toScreen({ x: rx, y: ry });

      // Offset label away from track using the angle
      const labelRad = ((c.angle + rotation) * Math.PI) / 180;
      const labelOffset = 18;
      const lx = screenX + Math.cos(labelRad) * labelOffset;
      const ly = screenY - Math.sin(labelRad) * labelOffset;

      const label = c.letter ? `${c.number}${c.letter}` : `${c.number}`;

      ctx.fillStyle = "rgba(255, 255, 255, 0.4)";
      ctx.fillText(label, lx, ly);
    }
  }

  // Marshal sector flag indicators
  if (marshalSectors && sectorFlags && sectorFlags.length > 0) {
    const flagLookup = new Map<number, SectorFlag>();
    for (const sf of sectorFlags) {
      flagLookup.set(sf.sector, sf);
    }

    ctx.font = "bold 9px sans-serif";
    ctx.textAlign = "center";
    ctx.textBaseline = "middle";

    for (const ms of marshalSectors) {
      const sf = flagLookup.get(ms.number);
      if (!sf) continue;

      const dx = ms.x - cx;
      const dy = ms.y - cy;
      const rx = dx * cos - dy * sin + cx;
      const ry = dx * sin + dy * cos + cy;
      const [screenX, screenY] = toScreen({ x: rx, y: ry });

      // Flag colour
      const isDouble = sf.flag === "DOUBLE YELLOW";
      const flagColor = sf.flag === "RED" ? "#FF0000" : "#FFD700";
      const radius = 8;

      // Dark outline for contrast
      ctx.beginPath();
      ctx.arc(screenX, screenY, radius + 2, 0, Math.PI * 2);
      ctx.fillStyle = "#000000";
      ctx.globalAlpha = 0.6;
      ctx.fill();
      ctx.globalAlpha = 1.0;

      // Inner circle
      ctx.beginPath();
      ctx.arc(screenX, screenY, radius, 0, Math.PI * 2);
      ctx.fillStyle = flagColor;
      ctx.fill();

      // Double yellow — outer ring
      if (isDouble) {
        ctx.beginPath();
        ctx.arc(screenX, screenY, radius + 4, 0, Math.PI * 2);
        ctx.strokeStyle = "#B8960F";
        ctx.lineWidth = 2;
        ctx.stroke();
      }

      // Draw driver abbreviation if present
      if (sf.driver) {
        ctx.fillStyle = "#FFFFFF";
        ctx.fillText(sf.driver, screenX, screenY + radius + 10);
      }
    }
  }
}

export function drawDrivers(
  ctx: CanvasRenderingContext2D,
  drivers: DriverMarker[],
  trackPoints: TrackPoint[],
  width: number,
  height: number,
  rotation: number,
  highlightedDrivers: string[],
  showNames: boolean = true,
) {
  if (trackPoints.length === 0) return;

  const padX = 40;
  const padTop = 60;
  const padBottom = 90;
  const w = width - padX * 2;
  const h = height - padTop - padBottom;

  const rad = (rotation * Math.PI) / 180;
  const cos = Math.cos(rad);
  const sin = Math.sin(rad);
  const cx = 0.5;
  const cy = 0.5;

  const rotatedTrack = trackPoints.map((p) => {
    const dx = p.x - cx;
    const dy = p.y - cy;
    return { x: dx * cos - dy * sin + cx, y: dx * sin + dy * cos + cy };
  });

  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const p of rotatedTrack) {
    minX = Math.min(minX, p.x);
    maxX = Math.max(maxX, p.x);
    minY = Math.min(minY, p.y);
    maxY = Math.max(maxY, p.y);
  }

  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;
  const scale = Math.min(w / rangeX, h / rangeY);
  const offsetX = padX + (w - rangeX * scale) / 2;
  const offsetY = padTop + (h - rangeY * scale) / 2;

  for (const drv of drivers) {
    // Rotate driver position
    const dx = drv.x - cx;
    const dy = drv.y - cy;
    const rx = dx * cos - dy * sin + cx;
    const ry = dx * sin + dy * cos + cy;

    const sx = offsetX + (rx - minX) * scale;
    const sy = offsetY + (maxY - ry) * scale; // Flip Y: data Y-up → screen Y-down

    const isHighlighted = highlightedDrivers.includes(drv.abbr);
    const radius = isHighlighted ? 8 : 5;

    ctx.save();

    // Glow effect for highlighted
    if (isHighlighted) {
      ctx.beginPath();
      ctx.arc(sx, sy, 14, 0, Math.PI * 2);
      ctx.fillStyle = drv.color + "40";
      ctx.fill();
    }

    // Driver dot
    ctx.beginPath();
    ctx.arc(sx, sy, radius, 0, Math.PI * 2);
    ctx.fillStyle = drv.color;
    ctx.strokeStyle = drv.color;
    ctx.lineWidth = 1;
    ctx.fill();
    ctx.stroke();

    ctx.restore();

    // Driver label
    if (showNames) {
      ctx.font = isHighlighted ? "800 12px system-ui, -apple-system, sans-serif" : "800 10px system-ui, -apple-system, sans-serif";
      ctx.fillStyle = "#FFFFFF";
      ctx.textAlign = "center";
      ctx.fillText(drv.abbr, sx, sy - radius - 4);
    }
  }
}

function interpolateSamplePosition(
  samples: Q3LineSample[],
  elapsedSeconds: number,
): { x: number; y: number; p: number; headingX: number; headingY: number } | null {
  if (!samples || samples.length === 0) return null;
  if (samples.length === 1) {
    return { x: samples[0].x, y: samples[0].y, p: samples[0].p, headingX: 1, headingY: 0 };
  }

  if (elapsedSeconds <= samples[0].t) {
    const nx = samples[1].x - samples[0].x;
    const ny = samples[1].y - samples[0].y;
    const mag = Math.hypot(nx, ny) || 1;
    return { x: samples[0].x, y: samples[0].y, p: samples[0].p, headingX: nx / mag, headingY: ny / mag };
  }
  const last = samples[samples.length - 1];
  if (elapsedSeconds >= last.t) {
    const prev = samples[samples.length - 2];
    const nx = last.x - prev.x;
    const ny = last.y - prev.y;
    const mag = Math.hypot(nx, ny) || 1;
    return { x: last.x, y: last.y, p: last.p, headingX: nx / mag, headingY: ny / mag };
  }

  for (let i = 1; i < samples.length; i++) {
    const curr = samples[i];
    if (elapsedSeconds > curr.t) continue;
    const prev = samples[i - 1];
    const span = Math.max(curr.t - prev.t, 1e-6);
    const ratio = (elapsedSeconds - prev.t) / span;
    const nx = curr.x - prev.x;
    const ny = curr.y - prev.y;
    const mag = Math.hypot(nx, ny) || 1;
    return {
      x: prev.x + (curr.x - prev.x) * ratio,
      y: prev.y + (curr.y - prev.y) * ratio,
      p: prev.p + (curr.p - prev.p) * ratio,
      headingX: nx / mag,
      headingY: ny / mag,
    };
  }

  const prev = samples[samples.length - 2];
  const nx = last.x - prev.x;
  const ny = last.y - prev.y;
  const mag = Math.hypot(nx, ny) || 1;
  return { x: last.x, y: last.y, p: last.p, headingX: nx / mag, headingY: ny / mag };
}

function mapReplayElapsedToSampleTime(
  samples: Q3LineSample[],
  lapTimeSeconds: number,
  replayElapsedSeconds: number,
): number {
  if (!samples || samples.length === 0) return replayElapsedSeconds;
  const firstT = samples[0].t;
  const lastT = samples[samples.length - 1].t;
  const span = Math.max(lastT - firstT, 1e-6);
  const lapSpan = Math.max(lapTimeSeconds, 1e-6);
  const clampedReplay = Math.max(0, Math.min(lapTimeSeconds, replayElapsedSeconds));
  const normalized = clampedReplay / lapSpan;
  return firstT + normalized * span;
}

export function drawQ3CompareOverlay(
  ctx: CanvasRenderingContext2D,
  lines: Q3CompareLine[],
  elapsedSeconds: number,
  width: number,
  height: number,
  rotation: number,
) {
  if (!lines || lines.length === 0) return;

  const padX = 40;
  const padTop = 60;
  const padBottom = 90;
  const w = width - padX * 2;
  const h = height - padTop - padBottom;

  const rad = (rotation * Math.PI) / 180;
  const cos = Math.cos(rad);
  const sin = Math.sin(rad);
  const cx = 0.5;
  const cy = 0.5;

  const allPoints: TrackPoint[] = [];
  for (const line of lines) {
    for (const s of line.samples) allPoints.push({ x: s.x, y: s.y });
  }
  if (allPoints.length === 0) return;

  const rotatedBounds = allPoints.map((p) => {
    const dx = p.x - cx;
    const dy = p.y - cy;
    return { x: dx * cos - dy * sin + cx, y: dx * sin + dy * cos + cy };
  });

  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (const p of rotatedBounds) {
    minX = Math.min(minX, p.x);
    maxX = Math.max(maxX, p.x);
    minY = Math.min(minY, p.y);
    maxY = Math.max(maxY, p.y);
  }

  const rangeX = maxX - minX || 1;
  const rangeY = maxY - minY || 1;
  const scale = Math.min(w / rangeX, h / rangeY);
  const offsetX = padX + (w - rangeX * scale) / 2;
  const offsetY = padTop + (h - rangeY * scale) / 2;

  function toScreen(p: TrackPoint): [number, number] {
    const dx = p.x - cx;
    const dy = p.y - cy;
    const rx = dx * cos - dy * sin + cx;
    const ry = dx * sin + dy * cos + cy;
    return [
      offsetX + (rx - minX) * scale,
      offsetY + (maxY - ry) * scale,
    ];
  }

  function drawRevealedLine(samples: Q3LineSample[], elapsedForDriver: number, color: string, lineDash?: number[]) {
    if (!samples || samples.length < 2) return;

    const firstT = samples[0].t;
    const lastT = samples[samples.length - 1].t;
    const clampedElapsed = Math.max(firstT, Math.min(elapsedForDriver, lastT));
    if (clampedElapsed <= firstT + 1e-6) return;

    ctx.save();
    ctx.globalAlpha = 0.9;
    ctx.beginPath();
    const [sx, sy] = toScreen({ x: samples[0].x, y: samples[0].y });
    ctx.moveTo(sx, sy);

    let drewAny = false;
    for (let i = 1; i < samples.length; i++) {
      const curr = samples[i];
      if (curr.t <= clampedElapsed) {
        const [px, py] = toScreen({ x: curr.x, y: curr.y });
        ctx.lineTo(px, py);
        drewAny = true;
        continue;
      }

      const prev = samples[i - 1];
      const span = Math.max(curr.t - prev.t, 1e-6);
      const ratio = Math.max(0, Math.min(1, (clampedElapsed - prev.t) / span));
      const ix = prev.x + (curr.x - prev.x) * ratio;
      const iy = prev.y + (curr.y - prev.y) * ratio;
      const [px, py] = toScreen({ x: ix, y: iy });
      ctx.lineTo(px, py);
      drewAny = true;
      break;
    }

    if (drewAny) {
      ctx.strokeStyle = color;
      ctx.lineWidth = 4;
      ctx.lineCap = "round";
      ctx.lineJoin = "round";
      ctx.setLineDash(lineDash || []);
      ctx.stroke();
    }
    ctx.restore();
  }

  function drawCarMarker(
    centerX: number,
    centerY: number,
    headingAngle: number,
    color: string,
    style: "solid" | "outlined" = "solid",
  ) {
    ctx.save();
    ctx.translate(centerX, centerY);
    ctx.rotate(headingAngle);

    const bodyColor = style === "outlined" ? "#111827" : color;
    const borderColor = style === "outlined" ? color : "#FFFFFF";
    const borderWidth = style === "outlined" ? 2.5 : 1.7;

    // Rear wing
    ctx.fillStyle = bodyColor;
    ctx.strokeStyle = borderColor;
    ctx.lineWidth = borderWidth;
    ctx.beginPath();
    ctx.roundRect(-9.5, -4, 2.8, 8, 1);
    ctx.fill();
    ctx.stroke();

    // Main body
    ctx.beginPath();
    ctx.roundRect(-6.8, -3.2, 10.8, 6.4, 2.2);
    ctx.fill();
    ctx.stroke();

    // Nose cone
    ctx.beginPath();
    ctx.moveTo(4, -2.3);
    ctx.lineTo(10.3, 0);
    ctx.lineTo(4, 2.3);
    ctx.closePath();
    ctx.fill();
    ctx.stroke();

    // Front wing
    ctx.beginPath();
    ctx.roundRect(9.8, -3.8, 2.4, 7.6, 1);
    ctx.fill();
    ctx.stroke();

    // Wheels
    ctx.fillStyle = "#0A0A0A";
    ctx.beginPath();
    ctx.arc(-3.8, -4.5, 1.6, 0, Math.PI * 2);
    ctx.arc(-3.8, 4.5, 1.6, 0, Math.PI * 2);
    ctx.arc(2.6, -4.5, 1.6, 0, Math.PI * 2);
    ctx.arc(2.6, 4.5, 1.6, 0, Math.PI * 2);
    ctx.fill();

    ctx.restore();
  }

  const cars: Array<{
    abbr: string;
    color: string;
    x: number;
    y: number;
    headingX: number;
    headingY: number;
    markerStyle: "solid" | "outlined";
  }> = [];
  for (const line of lines) {
    if (!line.samples || line.samples.length === 0) continue;
    const replayElapsedForDriver = Math.min(elapsedSeconds, Math.max(line.lapTimeSeconds, 0));
    const sampleElapsedForDriver = mapReplayElapsedToSampleTime(
      line.samples,
      line.lapTimeSeconds,
      replayElapsedForDriver,
    );
    drawRevealedLine(line.samples, sampleElapsedForDriver, line.color, line.lineDash);
    const pos = interpolateSamplePosition(line.samples, sampleElapsedForDriver);
    if (!pos) continue;
    cars.push({
      abbr: line.abbr,
      color: line.color,
      x: pos.x,
      y: pos.y,
      headingX: pos.headingX,
      headingY: pos.headingY,
      markerStyle: line.markerStyle || "solid",
    });
  }

  for (const car of cars) {
    const [sx, sy] = toScreen({ x: car.x, y: car.y });
    const [hx, hy] = toScreen({
      x: car.x + car.headingX * 0.01,
      y: car.y + car.headingY * 0.01,
    });
    const headingAngle = Math.atan2(hy - sy, hx - sx);
    ctx.save();
    drawCarMarker(sx, sy, headingAngle, car.color, car.markerStyle);
    ctx.font = "800 11px system-ui, -apple-system, sans-serif";
    ctx.fillStyle = "#FFFFFF";
    ctx.textAlign = "center";
    ctx.fillText(car.abbr, sx, sy - 15);
    ctx.restore();
  }
}
