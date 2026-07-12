-- Removes the self-derived order-diff volume estimator, superseded by real ESI region
-- history (see app/config.py's history_region_id) for every currently tracked structure.
--
-- Only needed on a database created before commit 5044981 ("Remove the order-diff volume
-- estimator entirely") - app/db.py's init_db() only ever adds schema, never removes it, so
-- any database bootstrapped before that commit still has these tables/columns lying around
-- unused. Safe to re-run (every statement is guarded with IF EXISTS).
--
-- Usage:
--   docker-compose exec -T postgres psql -U <POSTGRES_USER> -d <POSTGRES_DB> < migrations/2026-07-12_remove_order_diff_estimator.sql

BEGIN;

DROP TABLE IF EXISTS order_snapshots;
DROP TABLE IF EXISTS fill_events;
ALTER TABLE item_stats DROP COLUMN IF EXISTS volume_7d_min;
ALTER TABLE item_stats DROP COLUMN IF EXISTS volume_7d_max;

COMMIT;
