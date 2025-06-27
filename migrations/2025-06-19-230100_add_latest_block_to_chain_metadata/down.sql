-- This file should undo anything in `up.sql`

-- Remove latest_block column from chain_metadata table
ALTER TABLE chain_metadata 
DROP COLUMN latest_block;
