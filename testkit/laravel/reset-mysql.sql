-- testkit/laravel/reset-mysql.sql — the MySQL-family reset for testkit/laravel-suite.sh (M2-C1f).
--
-- Drop and recreate the suite's OWN database (never the shared `ferro` one), so every run starts
-- from the same empty schema — the precondition of recording a number. No GRANT here, on purpose:
-- MySQL and MariaDB store a database-level grant by NAME (`mysql.db`), so the one
-- testkit/mysql-init.sql made survives the DROP, and the suite's own user — which holds ALL on
-- `laravel_tests`.* and nothing more — can run this file as well as root can. That is what lets the
-- runner's `mysql-local` arm reset with the DSN's credentials.
--
-- The character set is EXPLICIT because the server defaults differ (MySQL 8.4 and MariaDB 11.8
-- default to utf8mb4, MariaDB 10.11 to latin1), and a column whose default collation depended on
-- which server ran it would make the families incomparable. utf8mb4_unicode_ci is Laravel's own
-- default `collation`.
DROP DATABASE IF EXISTS laravel_tests;
CREATE DATABASE laravel_tests CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
