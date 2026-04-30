-- Clear cached season schedules so they are regenerated with the latest logic.
-- This avoids requiring users to delete the entire persisted SQLite volume.
DELETE FROM json_artifacts
WHERE path LIKE 'seasons/%/schedule.json';

-- Keep normalized schedule tables in sync with regenerated artifacts.
DELETE FROM event_sessions;
DELETE FROM events;
DELETE FROM seasons;
