CREATE TABLE IF NOT EXISTS schema_migrations (
  version INTEGER PRIMARY KEY,
  checksum TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS json_artifacts (
  path TEXT PRIMARY KEY,
  body BLOB NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS seasons (
  year INTEGER PRIMARY KEY,
  payload_json BLOB NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  year INTEGER NOT NULL,
  round_number INTEGER NOT NULL,
  country TEXT,
  event_name TEXT,
  location TEXT,
  event_date TEXT,
  status TEXT,
  UNIQUE(year, round_number)
);

CREATE TABLE IF NOT EXISTS event_sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id INTEGER NOT NULL,
  name TEXT,
  session_type TEXT,
  date_utc TEXT,
  available INTEGER NOT NULL DEFAULT 0,
  download_state TEXT,
  downloaded INTEGER NOT NULL DEFAULT 0,
  last_error TEXT,
  updated_at TEXT,
  FOREIGN KEY(event_id) REFERENCES events(id) ON DELETE CASCADE,
  UNIQUE(event_id, session_type, name)
);

CREATE TABLE IF NOT EXISTS sessions (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  year INTEGER NOT NULL,
  round INTEGER NOT NULL,
  session_type TEXT NOT NULL,
  event_name TEXT,
  circuit TEXT,
  country TEXT,
  ready INTEGER NOT NULL DEFAULT 0,
  total_laps INTEGER NOT NULL DEFAULT 0,
  total_time REAL NOT NULL DEFAULT 0,
  replay_schema_version INTEGER NOT NULL DEFAULT 1,
  quali_phases_json BLOB,
  updated_at TEXT NOT NULL,
  UNIQUE(year, round, session_type)
);

CREATE INDEX IF NOT EXISTS idx_sessions_lookup ON sessions(year, round, session_type);

CREATE TABLE IF NOT EXISTS drivers (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  abbreviation TEXT NOT NULL,
  driver_number TEXT,
  full_name TEXT,
  team_name TEXT,
  team_color TEXT,
  UNIQUE(session_id, abbreviation),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS laps (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  driver TEXT NOT NULL,
  lap_number INTEGER NOT NULL,
  position INTEGER,
  lap_time TEXT,
  time_seconds REAL,
  sector1 TEXT,
  sector2 TEXT,
  sector3 TEXT,
  compound TEXT,
  tyre_life INTEGER,
  pit_in INTEGER NOT NULL DEFAULT 0,
  pit_out INTEGER NOT NULL DEFAULT 0,
  UNIQUE(session_id, driver, lap_number),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_laps_session_driver_lap ON laps(session_id, driver, lap_number);

CREATE TABLE IF NOT EXISTS results (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  position INTEGER,
  driver TEXT,
  abbreviation TEXT,
  team TEXT,
  team_color TEXT,
  grid_position INTEGER,
  status TEXT,
  points REAL,
  fastest_lap TEXT,
  gap_to_leader TEXT,
  UNIQUE(session_id, abbreviation),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS track (
  session_id INTEGER PRIMARY KEY,
  rotation REAL,
  circuit_name TEXT,
  x_min REAL,
  y_min REAL,
  scale REAL,
  track_points_json BLOB,
  sector_boundaries_json BLOB,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS track_corners (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  corner_order INTEGER NOT NULL,
  x REAL,
  y REAL,
  number INTEGER,
  letter TEXT,
  angle REAL,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_track_corners_session ON track_corners(session_id, corner_order);

CREATE TABLE IF NOT EXISTS track_marshal_sectors (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  sector_order INTEGER NOT NULL,
  x REAL,
  y REAL,
  number INTEGER,
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_track_marshal_session ON track_marshal_sectors(session_id, sector_order);

CREATE TABLE IF NOT EXISTS replay_chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  chunk_seq INTEGER NOT NULL,
  start_ts_ms INTEGER NOT NULL,
  end_ts_ms INTEGER NOT NULL,
  frame_count INTEGER NOT NULL,
  codec TEXT NOT NULL,
  payload_blob BLOB NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(session_id, chunk_seq),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS replay_frame_index (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  frame_seq INTEGER NOT NULL,
  ts_ms INTEGER NOT NULL,
  lap INTEGER NOT NULL,
  chunk_seq INTEGER NOT NULL,
  frame_in_chunk INTEGER NOT NULL,
  UNIQUE(session_id, frame_seq),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_replay_frame_ts ON replay_frame_index(session_id, ts_ms);
CREATE INDEX IF NOT EXISTS idx_replay_frame_lap ON replay_frame_index(session_id, lap, frame_seq);

CREATE TABLE IF NOT EXISTS telemetry_chunks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  session_id INTEGER NOT NULL,
  driver_abbr TEXT NOT NULL,
  lap INTEGER NOT NULL,
  chunk_seq INTEGER NOT NULL,
  codec TEXT NOT NULL,
  payload_blob BLOB NOT NULL,
  created_at TEXT NOT NULL,
  UNIQUE(session_id, driver_abbr, lap, chunk_seq),
  FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_telemetry_lookup ON telemetry_chunks(session_id, driver_abbr, lap, chunk_seq);

CREATE TABLE IF NOT EXISTS download_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  state_json BLOB NOT NULL,
  updated_at TEXT NOT NULL
);
