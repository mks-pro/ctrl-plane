-- Runs only when the Postgres volume is first initialized.
-- The runtime database is created by POSTGRES_DB; this script adds the
-- service schema and the disposable database used by integration tests.
CREATE SCHEMA IF NOT EXISTS app;

SELECT 'CREATE DATABASE saas_task_test OWNER ctrlplane'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'saas_task_test')
\gexec
