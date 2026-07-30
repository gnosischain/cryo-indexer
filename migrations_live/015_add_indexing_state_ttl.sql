-- Bound execution_live.indexing_state growth.
--
-- The live data tables all expire after two days (002/003/004/013 carry
-- `TTL ... + INTERVAL 2 DAY`), but indexing_state had no TTL at all. State rows
-- therefore accumulated forever while the data they describe was deliberately
-- expired, leaving stale 'failed' markers reaching back to block 40. The
-- cryo-failed-ranges alert counts `latest_status = 'failed'` per range with no time
-- bound, so it reported 477 "missing" ranges on 2026-07-30 for data that was gone by
-- design -- a false positive that grew monotonically and could never clear.
--
-- 7 days is deliberately LONGER than the 2-day data TTL. State must outlive the data
-- it describes: if a 'completed' marker expired while its rows were still present, the
-- range would look unindexed and be re-extracted for nothing.
--
-- Only migrations_live/ gets this. execution and celo_execution are full-history
-- databases whose indexing_state IS the completeness record -- never TTL those.
--
-- Idempotent: MODIFY TTL can be re-applied safely. Note ClickHouse defaults
-- materialize_ttl_after_modify = 1, so existing parts are rewritten and rows already
-- past the horizon are dropped by the resulting mutation.

ALTER TABLE {{database}}.indexing_state
    MODIFY TTL created_at + INTERVAL 7 DAY;
