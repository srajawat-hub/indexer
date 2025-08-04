-- Your SQL goes here
CREATE TABLE launchpad_metrics_summary (
    id BOOLEAN PRIMARY KEY DEFAULT TRUE, -- Singleton row
    total_launchpad_pools BIGINT DEFAULT 0,
    total_unique_traders BIGINT DEFAULT 0,
    total_traded_volume NUMERIC DEFAULT 0,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

-- Insert initial row
INSERT INTO launchpad_metrics_summary (id) VALUES (TRUE)
ON CONFLICT DO NOTHING;

-- Step 2: Create a table to track unique traders
CREATE TABLE launchpad_traders (
    user_address TEXT PRIMARY KEY
);

-- Step 3: Create trigger function for pools table
CREATE OR REPLACE FUNCTION increment_pool_count()
RETURNS TRIGGER AS $$
BEGIN
    UPDATE launchpad_metrics_summary
    SET total_launchpad_pools = total_launchpad_pools + 1,
        updated_at = NOW()
    WHERE id = TRUE;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Step 4: Attach trigger to pools table
CREATE TRIGGER trg_increment_pool_count
AFTER INSERT ON pools
FOR EACH ROW
EXECUTE FUNCTION increment_pool_count();

-- Step 5: Create trigger function for ammswap table
CREATE OR REPLACE FUNCTION update_volume_and_traders()
RETURNS TRIGGER AS $$
DECLARE
    new_user TEXT := LOWER(NEW.initiator_user_address);
    is_new_trader BOOLEAN := FALSE;
BEGIN
    -- Insert into unique trader table
    BEGIN
        INSERT INTO launchpad_traders (user_address)
        VALUES (new_user);
        is_new_trader := TRUE;
    EXCEPTION WHEN unique_violation THEN
        is_new_trader := FALSE;
    END;

    -- Update the metrics summary
    UPDATE launchpad_metrics_summary
    SET total_traded_volume = total_traded_volume + COALESCE(NEW.amount_in_usd, 0),
        total_unique_traders = total_unique_traders + CASE WHEN is_new_trader THEN 1 ELSE 0 END,
        updated_at = NOW()
    WHERE id = TRUE;

    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Step 6: Attach trigger to ammswap table
CREATE TRIGGER trg_update_volume_and_traders
AFTER INSERT ON ammswap
FOR EACH ROW
EXECUTE FUNCTION update_volume_and_traders();