-- testkit/postgres/init.sql
-- Minimal seed so integration/smoke tests have deterministic rows to read.
CREATE TABLE ferro_smoke (
    id   integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO ferro_smoke (id, note) VALUES (1, 'hello'), (2, 'world');

-- M1-S8b: the upstream Doctrine DBAL functional suite gets its OWN database. NEVER point it at the
-- shared `ferro` one: it creates and abandons ~40 tables, 8+ sequences, several schemas, a domain
-- type and views, and nothing would ever clean them out of the database every other live suite in
-- this repo uses. Ferro's patched TestUtil does NOT drop/create it (PHP holds no credentials —
-- SPEC §12 / D8); `testkit/dbal-suite.sh` resets it container-side before every recorded run.
SELECT 'CREATE DATABASE doctrine_tests OWNER ferro'
 WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'doctrine_tests') \gexec
