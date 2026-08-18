-- M1-S9: the ORM suite's fail-closed reset, MySQL/MariaDB arm. Runs as ROOT container-side: the
-- `ferro` user deliberately has no CREATE DATABASE (testkit/mysql-init.sql grants per-database
-- privileges only), so the GRANT below is what makes the freshly created database usable by the
-- pool's DSN user. Same contract as the PG arm: PHP holds no credentials (SPEC §12 / D8) and
-- testkit/orm/TestUtil.ferro.php's initializeDatabase() is a no-op that depends on this file.
DROP DATABASE IF EXISTS doctrine_orm_tests;
CREATE DATABASE doctrine_orm_tests;
GRANT ALL PRIVILEGES ON doctrine_orm_tests.* TO 'ferro'@'%';
FLUSH PRIVILEGES;
