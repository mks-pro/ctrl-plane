-- 0001_init.down.sql — reverse dependency order.
DROP TABLE IF EXISTS processed_updates;
DROP TABLE IF EXISTS outbox_events;
DROP TABLE IF EXISTS tasks;
DROP TABLE IF EXISTS tenants;
