-- Your SQL goes here
ALTER TABLE pools ADD COLUMN liquidity_lock_end_timestamp TIMESTAMP;
ALTER TABLE liquidity ADD COLUMN token_id TEXT;