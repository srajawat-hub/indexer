-- This file should undo anything in `up.sql`
-- Step 1: Drop the triggers from pools and ammswap
DROP TRIGGER IF EXISTS trg_increment_pool_count ON pools;
DROP TRIGGER IF EXISTS trg_update_volume_and_traders ON ammswap;

-- Step 2: Drop the trigger functions
DROP FUNCTION IF EXISTS increment_pool_count();
DROP FUNCTION IF EXISTS update_volume_and_traders();

-- Step 3: Drop the launchpad_traders table (if used)
DROP TABLE IF EXISTS launchpad_traders;

-- Step 4: Drop the summary table
DROP TABLE IF EXISTS launchpad_metrics_summary;
