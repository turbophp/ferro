-- testkit/orm/reset-mysql.sql — the ORM suite's database, dropped and recreated (run as root).
-- Its OWN database, never the shared `ferro` one: the suite creates and abandons ~500 tables.
DROP DATABASE IF EXISTS orm_tests;
CREATE DATABASE orm_tests CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
GRANT ALL PRIVILEGES ON orm_tests.* TO 'ferro'@'%';
