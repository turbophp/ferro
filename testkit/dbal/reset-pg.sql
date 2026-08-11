-- M1-S8b: the upstream functional suite's ONLY reset. Upstream gets idempotence from
-- TestUtil::initializeDatabase()'s dropDatabase/createDatabase, which Ferro structurally cannot do
-- (PHP holds no credentials, SPEC §12/D8), so it happens HERE, container-side, with no PHP
-- credentials involved — the same shape the MySQL grant in testkit/mysql-init.sql already uses.
--
-- Without it the recorded number is not reproducible: measured against a KNOWN-GOOD driver, the same
-- command gave 23 then 33 errors on consecutive runs (plan hazard 85). CASCADE is required —
-- upstream's own dropTableIfExists issues a plain DROP TABLE and leaves dependent objects behind.
--
-- It drops EVERY non-system schema rather than a hand-written list, because a hand-written list is
-- exactly what silently rots: the plan's draft named testschema/nested/another, and the FIRST
-- measured run of this suite also left `001_test` and `test_create_schema` behind (from
-- Schema/SchemaManagerFunctionalTestCase), which showed up as a 71 -> 72 error drift between the
-- first run on a virgin database and every run after it. Enumerating from pg_namespace cannot rot.
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
