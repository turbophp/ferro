-- M1-S9: the ORM suite's fail-closed reset. Runs container-side as the `ferro` user against the
-- MAINTENANCE database (`postgres`) — never `doctrine_orm_tests` itself, because you cannot drop
-- the database you are connected to.
--
-- `WITH (FORCE)` severs any backend still pinning it. That clause is not defensive styling: the
-- measured failure mode (research-orm step 6b) was a stale `ferrod` holding 2 pooled connections,
-- the DROP silently refused, and 48 non-passing becoming 227 with a triage that blamed the driver
-- (96 ToolsException + 34 dup-key + 21 count drift + 7 NonUniqueResult — every one of which reads
-- like a Ferro defect and none of which was).
--
-- PHP holds no credentials (SPEC §12 / D8), so upstream's `TestUtil::initializeDatabase()`
-- dropDatabase/createDatabase pair cannot exist here; this file IS that reset, and
-- `testkit/orm/TestUtil.ferro.php`'s no-op is only sound because it runs.
DROP DATABASE IF EXISTS doctrine_orm_tests WITH (FORCE);
CREATE DATABASE doctrine_orm_tests OWNER ferro;
