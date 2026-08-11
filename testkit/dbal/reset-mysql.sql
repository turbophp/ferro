-- M1-S8b: see reset-pg.sql. MySQL has no schema/database distinction to work around, so the whole
-- database is recreated. Run as root, container-side — the `ferro` user deliberately has no
-- CREATE DATABASE privilege.
DROP DATABASE IF EXISTS doctrine_tests;
CREATE DATABASE doctrine_tests;
GRANT ALL PRIVILEGES ON doctrine_tests.* TO 'ferro'@'%';
FLUSH PRIVILEGES;
