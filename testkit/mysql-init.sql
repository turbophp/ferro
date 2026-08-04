-- testkit/mysql-init.sql
-- Shared init for the MySQL 8 + MariaDB 11 testkit backends (M1-S6). Mounted at
-- /docker-entrypoint-initdb.d/init.sql on BOTH services, so it must stay MySQL/MariaDB-compatible.
--
-- Three jobs:
--   1. Re-assert the OK-packet SESSION TRACKERS (belt-and-braces with the docker-compose `command:`
--      flags, which are the authoritative source). A CURATED `session_track_system_variables` list —
--      deliberately NOT `'*'`: the Task-1 spike found `'*'` fires benign trackers (e.g. statement_id)
--      that would taint every pooled connection. The list INCLUDES `sort_buffer_size` so the
--      `p_set_session()` fixture below actually emits a SystemVariables tracker.
--   2. A deterministic seed table for smoke/integration reads — SIGNED `bigint` PK (NOT
--      `BIGINT UNSIGNED`: the unsigned-64 type policy is deferred, SPEC §9).
--   3. `p_set_session()` — the §7.1 hard-gate fixture: a session mutation performed INSIDE a stored
--      program, which the assist lexer is BLIND to but the OK-packet tracker still reports. This is
--      the property that proves the MySQL pin engine sees what PG's protocol byte + a lexer cannot.

-- 1. Session trackers (mirror the `command:` flags so this file also works if sourced by hand
--    against a server started without them). SET GLOBAL affects connections opened AFTER it — the
--    real test connections are all opened post-init, so they inherit these.
SET GLOBAL session_track_state_change = ON;
SET GLOBAL session_track_transaction_info = 'STATE';
SET GLOBAL session_track_system_variables =
    'autocommit,sql_mode,time_zone,sort_buffer_size,foreign_key_checks,unique_checks';

USE ferro;

-- 2. Seed table — signed bigint PK (avoids the deferred BIGINT UNSIGNED / unsigned-64 policy).
CREATE TABLE ferro_smoke (
    id   BIGINT PRIMARY KEY,
    note VARCHAR(255) NOT NULL
);
INSERT INTO ferro_smoke (id, note) VALUES (1, 'hello'), (2, 'world');

-- 3. The §7.1 hard-gate fixture: a `SET SESSION` buried in a proc body. `CALL p_set_session()` shows
--    the assist lexer only `CALL p_set_session()` (no visible SET), yet the OK-packet
--    `session_state_info` will still report the `sort_buffer_size` mutation — the M1-S6 raison d'être.
DELIMITER $$
CREATE PROCEDURE p_set_session()
BEGIN
    SET SESSION sort_buffer_size = 262144;
END $$
DELIMITER ;
