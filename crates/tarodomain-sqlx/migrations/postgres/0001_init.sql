-- TaroDomain schema for PostgreSQL (spec §27.1): uuid, jsonb and timestamptz.

-- 1 + 3. The append-only event log, folded together with the stream mapping.
-- `seq` is insert order. `position` is the global append order and stays NULL
-- until the Sequencer numbers the event after its transaction commits (plan
-- Risks: global ordering).
CREATE TABLE tarodomain_events (
  seq BIGSERIAL PRIMARY KEY,
  position BIGINT UNIQUE,
  event_id UUID NOT NULL UNIQUE,
  stream_id TEXT NOT NULL,
  stream_category TEXT NOT NULL,
  stream_version BIGINT NOT NULL,
  event_type TEXT NOT NULL,
  event_version TEXT NOT NULL,
  causation_id UUID,
  correlation_id UUID NOT NULL,
  command_causation_id UUID,
  data JSONB NOT NULL,
  metadata JSONB NOT NULL,
  occurred_at TIMESTAMPTZ NOT NULL,
  UNIQUE (stream_id, stream_version)
);
CREATE INDEX tarodomain_events_category ON tarodomain_events (stream_category, position);
CREATE INDEX tarodomain_events_correlation ON tarodomain_events (correlation_id, seq);
CREATE INDEX tarodomain_events_unpositioned ON tarodomain_events (seq) WHERE position IS NULL;

-- 2. Stream metadata.
CREATE TABLE tarodomain_streams (
  stream_id TEXT PRIMARY KEY,
  version BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL
);

-- 4. Aggregate snapshots.
CREATE TABLE tarodomain_snapshots (
  source_uuid UUID NOT NULL,
  source_version BIGINT NOT NULL,
  source_type TEXT NOT NULL,
  data JSONB NOT NULL,
  metadata JSONB NOT NULL,
  created_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (source_uuid, source_version)
);

-- 5. Persistent subscription cursors. `last_position` is what catch-up
-- reads by; `last_seen` is the event ID the contract reports.
CREATE TABLE tarodomain_subscriptions (
  subscription_name TEXT PRIMARY KEY,
  stream_id TEXT,
  last_seen UUID,
  last_position BIGINT NOT NULL DEFAULT 0,
  created_at TIMESTAMPTZ NOT NULL,
  failed_event_id UUID,
  failed_attempts BIGINT NOT NULL DEFAULT 0
);

-- 6. Projection and saga idempotency. `handler_instance` is '' for none.
CREATE TABLE tarodomain_processed_events (
  handler_name TEXT NOT NULL,
  handler_instance TEXT NOT NULL DEFAULT '',
  event_id UUID NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL,
  PRIMARY KEY (handler_name, handler_instance, event_id)
);
CREATE INDEX tarodomain_processed_events_at ON tarodomain_processed_events (processed_at);

-- 7. Process manager state.
CREATE TABLE tarodomain_sagas (
  saga_type TEXT NOT NULL,
  saga_id UUID NOT NULL,
  correlation_id UUID NOT NULL,
  state JSONB NOT NULL,
  status TEXT NOT NULL,
  current_step TEXT NOT NULL,
  failure_reason TEXT,
  started_at TIMESTAMPTZ NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL,
  completed_at TIMESTAMPTZ,
  PRIMARY KEY (saga_type, saga_id)
);
CREATE INDEX tarodomain_sagas_correlation ON tarodomain_sagas (correlation_id);
CREATE INDEX tarodomain_sagas_status ON tarodomain_sagas (status, updated_at);

-- 8. Failed message quarantine. IDs are never reused.
CREATE TABLE tarodomain_dead_letters (
  id BIGSERIAL PRIMARY KEY,
  handler_name TEXT NOT NULL,
  event_id UUID NOT NULL,
  event_type TEXT NOT NULL,
  payload JSONB NOT NULL,
  error_code TEXT NOT NULL,
  error_message TEXT NOT NULL,
  attempts BIGINT NOT NULL,
  created_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX tarodomain_dead_letters_event ON tarodomain_dead_letters (event_id, handler_name);

-- 9. Command deduplication.
CREATE TABLE tarodomain_processed_commands (
  command_id UUID PRIMARY KEY,
  command_type TEXT NOT NULL,
  handler_name TEXT NOT NULL,
  processed_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX tarodomain_processed_commands_at ON tarodomain_processed_commands (processed_at);

-- Named counters: `event-position` is the Sequencer's.
CREATE TABLE tarodomain_counters (
  name TEXT PRIMARY KEY,
  value BIGINT NOT NULL
);

-- Job leases for the scheduled jobs (Step 11.6). `expires_at` is epoch ms.
CREATE TABLE tarodomain_locks (
  name TEXT PRIMARY KEY,
  holder TEXT NOT NULL,
  expires_at BIGINT NOT NULL
);
