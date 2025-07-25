-- Drop indexes first
DROP INDEX IF EXISTS idx_mayan_vaas_sequence_order_hash_chain;
DROP INDEX IF EXISTS idx_mayan_vaas_chain_id;
DROP INDEX IF EXISTS idx_mayan_vaas_timestamp;
DROP INDEX IF EXISTS idx_mayan_vaas_order_hash;
DROP INDEX IF EXISTS idx_mayan_vaas_sequence;

-- Drop the table
DROP TABLE IF EXISTS mayan_vaas;
