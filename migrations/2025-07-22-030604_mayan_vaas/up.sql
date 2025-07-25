-- Create mayan_vaas table to store VAA sequence and order hash data
CREATE TABLE IF NOT EXISTS public.mayan_vaas (
    id bigserial NOT NULL PRIMARY KEY,
    sequence bigint NOT NULL,
    order_hash text NOT NULL,
    vaa_action smallint NOT NULL, -- 0=None, 1=Fulfill, 2=Unlock, 3=Cancel, 4=UnlockBatch
    timestamp timestamp without time zone NOT NULL,
    vaa_data text, -- base64 encoded VAA data (optional for debugging)
    chain_id smallint NOT NULL, -- Chain ID (2=Ethereum, 30=Base)
    solana_order_hash text, -- SHA-256 hash of LocalOrderProcessingData for Solana queries
    created_at timestamp without time zone DEFAULT NOW() NOT NULL
);

-- Create index on sequence for faster lookups
CREATE INDEX IF NOT EXISTS idx_mayan_vaas_sequence ON public.mayan_vaas(sequence);
-- Create index on order_hash for faster order lookups
CREATE INDEX IF NOT EXISTS idx_mayan_vaas_order_hash ON public.mayan_vaas(order_hash);
-- Create index on solana_order_hash for faster Solana-style hash lookups
CREATE INDEX IF NOT EXISTS idx_mayan_vaas_solana_order_hash ON public.mayan_vaas(solana_order_hash);
-- Create index on timestamp for time-based queries
CREATE INDEX IF NOT EXISTS idx_mayan_vaas_timestamp ON public.mayan_vaas(timestamp);
-- Create index on chain_id for faster chain-specific lookups
CREATE INDEX IF NOT EXISTS idx_mayan_vaas_chain_id ON public.mayan_vaas(chain_id);
-- Create unique constraint on (sequence, order_hash, chain_id) combination to prevent exact duplicates
CREATE UNIQUE INDEX IF NOT EXISTS idx_mayan_vaas_sequence_order_hash_chain ON public.mayan_vaas(sequence, order_hash, chain_id);
