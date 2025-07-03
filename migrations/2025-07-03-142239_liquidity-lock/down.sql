-- This file should undo anything in `up.sql`
ALTER TABLE pools DROP COLUMN liquidity_lock_end_timestamp;
ALTER TABLE liquidity DROP COLUMN token_id;