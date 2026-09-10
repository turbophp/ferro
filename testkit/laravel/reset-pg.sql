-- M2 / C2: the Illuminate integration suite's ONLY reset, container-side.
--
-- Same shape and same reason as testkit/dbal/reset-pg.sql: PHP holds no credentials (SPEC §12/D8),
-- so it cannot drop and recreate its own database the way a framework suite normally would. Without
-- a reset the recorded number is not reproducible — the DBAL suite MEASURED that against a
-- known-good driver, where consecutive runs of the same command gave 23 then 33 errors.
--
-- It enumerates schemas from pg_namespace rather than naming them, and that is not defensive
-- styling: the sibling suite's hand-written list measurably rotted. Two schemas the plan never named
-- were left behind by the suite itself, drifting the error count 71 -> 72 between a virgin database
-- and every run after it. An enumeration cannot rot; a list of names always can.
--
-- CASCADE is required for the same reason it is there: a plain DROP TABLE leaves dependent objects
-- behind, and a framework's migration tables carry constraints and sequences.
--
-- NOTE this is deliberately NOT parameterised by suite. It runs against the `laravel_tests`
-- database (testkit/postgres/init.sql), never the shared `ferro` one, and the runner passes the
-- database explicitly rather than relying on a default.
DO $$
DECLARE s text;
BEGIN
    FOR s IN
        SELECT nspname FROM pg_namespace
         WHERE nspname NOT IN ('pg_catalog', 'information_schema', 'pg_toast')
           AND nspname NOT LIKE 'pg_temp%'
           AND nspname NOT LIKE 'pg_toast_temp%'
    LOOP
        EXECUTE format('DROP SCHEMA %I CASCADE', s);
    END LOOP;
END $$;
CREATE SCHEMA public AUTHORIZATION ferro;
GRANT ALL ON SCHEMA public TO ferro;
