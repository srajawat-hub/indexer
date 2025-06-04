-- Revert AmmSwap first (has foreign key on pool)
DROP TABLE IF EXISTS AmmSwap;

-- Then revert Liquidity (also depends on pool)
DROP TABLE IF EXISTS liquidity;

-- Finally, drop the Pool table
DROP TABLE IF EXISTS pools;