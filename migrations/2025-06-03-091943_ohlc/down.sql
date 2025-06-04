-- This file should undo anything in `up.sql`
-- Drop the stored procedure
DROP PROCEDURE IF EXISTS public.refresh_ohlc(TEXT);

-- Drop the unique constraint, if it wasn't dropped automatically with the table
ALTER TABLE IF EXISTS public.ohlc_price_tables
    DROP CONSTRAINT IF EXISTS ohlc_unique;

-- Drop the table
DROP TABLE IF EXISTS public.ohlc_price_tables;