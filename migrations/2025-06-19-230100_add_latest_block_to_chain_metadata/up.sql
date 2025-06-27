-- Your SQL goes here

-- Add latest_block column to chain_metadata table
ALTER TABLE chain_metadata 
ADD COLUMN latest_block BIGINT DEFAULT 0 NOT NULL;
