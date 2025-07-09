-- This file should undo anything in `up.sql`
ALTER TABLE chain_metadata ADD COLUMN network_name TEXT NOT NULL DEFAULT '';