-- testkit/postgres/init.sql
-- Minimal seed so integration/smoke tests have deterministic rows to read.
CREATE TABLE ferro_smoke (
    id   integer PRIMARY KEY,
    note text NOT NULL
);
INSERT INTO ferro_smoke (id, note) VALUES (1, 'hello'), (2, 'world');
