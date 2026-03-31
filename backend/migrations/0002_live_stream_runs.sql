CREATE TABLE IF NOT EXISTS live_stream_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  year INTEGER NOT NULL,
  round INTEGER NOT NULL,
  session_type TEXT NOT NULL,
  status TEXT NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT,
  last_error TEXT
);

CREATE INDEX IF NOT EXISTS idx_live_stream_runs_lookup
  ON live_stream_runs(year, round, session_type, started_at);
