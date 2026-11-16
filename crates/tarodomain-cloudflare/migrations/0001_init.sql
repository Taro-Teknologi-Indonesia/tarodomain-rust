-- TaroDomain schema, version 1.0.0 (spec §27.1, following §27.2).
-- Timestamps are ISO 8601 text with milliseconds, JSON is text, versions are
-- integers. Every statement ends with a semicolon at the end of a line.

-- 1 + 3. The append-only event log, folded together with the stream mapping:
-- `position` is the global append order (D1 runs one write at a time, so
-- insert order is commit order), and (stream_id, stream_version) is unique,
-- which is how a stale writer is detected.
CREATE TABLE IF NOT EXISTS tarodomain_events (
  position INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id TEXT NOT NULL UNIQUE,
  stream_id TEXT NOT NULL,
  stream_category TEXT NOT NULL,
  stream_version INTEGER NOT NULL,
  event_type TEXT NOT NULL,
  event_version TEXT NOT NULL,
  causation_id TEXT,
  correlation_id TEXT NOT NULL,
  command_causation_id TEXT,
  data TEXT NOT NULL,
  metadata TEXT NOT NULL,
  occurred_at TEXT NOT NULL,
  UNIQUE (stream_id, stream_version)
);
CREATE INDEX IF NOT EXISTS tarodomain_events_category ON tarodomain_events (stream_category, position);
CREATE INDEX IF NOT EXISTS tarodomain_events_correlation ON tarodomain_events (correlation_id, position);

-- 2. Stream metadata.
CREATE TABLE IF NOT EXISTS tarodomain_streams (
  stream_id TEXT PRIMARY KEY,
  version INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
) WITHOUT ROWID;

-- 4. Aggregate snapshots.
CREATE TABLE IF NOT EXISTS tarodomain_snapshots (
  source_uuid TEXT NOT NULL,
  source_version INTEGER NOT NULL,
  source_type TEXT NOT NULL,
  data TEXT NOT NULL,
  metadata TEXT NOT NULL,
  created_at TEXT NOT NULL,
  PRIMARY KEY (source_uuid, source_version)
) WITHOUT ROWID;

-- 5. Persistent subscription cursors. `last_position` is what catch-up
-- reads by; `last_seen` is the event ID the contract reports.
CREATE TABLE IF NOT EXISTS tarodomain_subscriptions (
  subscription_name TEXT PRIMARY KEY,
  stream_id TEXT,
  last_seen TEXT,
  last_position INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  failed_event_id TEXT,
  failed_attempts INTEGER NOT NULL DEFAULT 0
) WITHOUT ROWID;

-- 6. Projection and saga idempotency. `handler_instance` is '' for none.
CREATE TABLE IF NOT EXISTS tarodomain_processed_events (
  handler_name TEXT NOT NULL,
  handler_instance TEXT NOT NULL DEFAULT '',
  event_id TEXT NOT NULL,
  processed_at TEXT NOT NULL,
  PRIMARY KEY (handler_name, handler_instance, event_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS tarodomain_processed_events_at ON tarodomain_processed_events (processed_at);

-- 7. Process manager state.
CREATE TABLE IF NOT EXISTS tarodomain_sagas (
  saga_type TEXT NOT NULL,
  saga_id TEXT NOT NULL,
  correlation_id TEXT NOT NULL,
  state TEXT NOT NULL,
  status TEXT NOT NULL,
  current_step TEXT NOT NULL,
  failure_reason TEXT,
  started_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  completed_at TEXT,
  PRIMARY KEY (saga_type, saga_id)
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS tarodomain_sagas_correlation ON tarodomain_sagas (correlation_id);
CREATE INDEX IF NOT EXISTS tarodomain_sagas_status ON tarodomain_sagas (status, updated_at);

-- 8. Failed message quarantine. IDs are never reused.
CREATE TABLE IF NOT EXISTS tarodomain_dead_letters (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  handler_name TEXT NOT NULL,
  event_id TEXT NOT NULL,
  event_type TEXT NOT NULL,
  payload TEXT NOT NULL,
  error_code TEXT NOT NULL,
  error_message TEXT NOT NULL,
  attempts INTEGER NOT NULL,
  created_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS tarodomain_dead_letters_event ON tarodomain_dead_letters (event_id, handler_name);

-- 9. Command deduplication.
CREATE TABLE IF NOT EXISTS tarodomain_processed_commands (
  command_id TEXT PRIMARY KEY,
  command_type TEXT NOT NULL,
  handler_name TEXT NOT NULL,
  processed_at TEXT NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS tarodomain_processed_commands_at ON tarodomain_processed_commands (processed_at);

-- Job leases for the scheduled jobs (Step 10.7). `expires_at` is epoch ms.
CREATE TABLE IF NOT EXISTS tarodomain_locks (
  name TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  expires_at INTEGER NOT NULL
) WITHOUT ROWID;
