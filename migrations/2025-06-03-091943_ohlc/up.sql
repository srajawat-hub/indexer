-- Your SQL goes here
CREATE TABLE ohlc_price_tables (
    id               BIGSERIAL PRIMARY KEY,
    token_address    TEXT       NOT NULL,
    chain_id         BIGINT     NOT NULL,
    interval         TEXT       NOT NULL,  -- e.g. '1m', '1h', '1d'
    open_price       NUMERIC    NOT NULL,
    high_price       NUMERIC    NOT NULL,
    low_price        NUMERIC    NOT NULL,
    close_price      NUMERIC    NOT NULL,
    volume_token     NUMERIC    NOT NULL,
    volume_usd       NUMERIC,
    timestamp_bucket TIMESTAMP  NOT NULL,  -- start of candle
    pool_address     TEXT       NOT NULL
);

ALTER TABLE ohlc_price_tables
    ADD CONSTRAINT ohlc_unique
        UNIQUE(token_address, chain_id, interval, timestamp_bucket, pool_address);

CREATE OR REPLACE PROCEDURE public.refresh_ohlc(p_interval TEXT)
    LANGUAGE plpgsql
AS $$
DECLARE
    v_trunc_unit   TEXT;
    v_bucket_expr  TEXT;
    v_last_bucket  TIMESTAMP;
BEGIN
    -------------------------------------------------------------------
    -- 1) Determine the date_trunc unit (and special‐case 5m)
    -------------------------------------------------------------------
    v_trunc_unit := CASE p_interval
                        WHEN '1m' THEN 'minute'
                        WHEN '5m' THEN 'minute'   -- we’ll adjust to multiples of 5 minutes below
                        WHEN '1h' THEN 'hour'
                        WHEN '1d' THEN 'day'
                        ELSE NULL
        END;

    IF v_trunc_unit IS NULL THEN
        RAISE EXCEPTION 'Unsupported interval: %', p_interval;
    END IF;

    IF p_interval = '5m' THEN
        -- Round down to the nearest 5-minute boundary:
        v_bucket_expr :=
                'date_trunc(''minute'', sa.timestamp)'
                    || ' - (extract(minute from sa.timestamp)::int % 5) * ''1 minute''::interval';
    ELSE
        -- Simple truncation by minute/hour/day:
        v_bucket_expr := format(
                'date_trunc(''%s'', sa.timestamp)',
                v_trunc_unit
        );
    END IF;

    -------------------------------------------------------------------
    -- 2) Find the last candle we wrote for this interval
    -------------------------------------------------------------------
    SELECT COALESCE(MAX(timestamp_bucket), '1970-01-01'::timestamp)
    INTO v_last_bucket
    FROM public.ohlc_price_tables
    WHERE interval = p_interval;

    -------------------------------------------------------------------
    -- 3) Build & run a single dynamic‐SQL block that:
    --    a) selects all ammswap rows into a CTE “trades”,
    --       computing both the bucket and the ``token_reserve``
    --    b) groups by (token_reserve, chain_id, pool_address, bucket)
    --    c) inserts one candle per group
    -------------------------------------------------------------------
    EXECUTE format($SQL$
    WITH trades AS (
      SELECT
        sa.*,
        -- 1) Compute the bucket (minute/hour/day or 5m)
        %s AS bucket,
        -- 2) Compute the “reserve token” (the lexicographically smaller of the pair)
        LEAST(sa.token_in, sa.token_out) AS token_reserve
      FROM public.ammswap sa
    )
    INSERT INTO public.ohlc_price_tables (
      token_address, chain_id, interval,
      open_price, high_price, low_price, close_price,
      volume_token, volume_usd,
      timestamp_bucket, pool_address
    )
    SELECT
      t.token_reserve        AS token_address,
      t.chain_id,
      %L                     AS interval,
      -- open_price: the price of the earliest trade in this bucket
      (
        SELECT x.price
        FROM trades x
        WHERE x.token_reserve = t.token_reserve
          AND x.chain_id     = t.chain_id
          AND x.pool_address = t.pool_address
          AND x.bucket       = t.bucket
        ORDER BY x.timestamp
        LIMIT 1
      )                     AS open_price,
      MAX(t.price)          AS high_price,
      MIN(t.price)          AS low_price,
      -- close_price: the price of the latest trade in this bucket
      (
        SELECT x.price
        FROM trades x
        WHERE x.token_reserve = t.token_reserve
          AND x.chain_id     = t.chain_id
          AND x.pool_address = t.pool_address
          AND x.bucket       = t.bucket
        ORDER BY x.timestamp DESC
        LIMIT 1
      )                     AS close_price,
      SUM(t.amount_in)      AS volume_token,
      SUM(t.amount_in_usd)  AS volume_usd,
      t.bucket              AS timestamp_bucket,
      t.pool_address
    FROM trades t
    WHERE t.bucket > %L
    GROUP BY
      t.token_reserve,
      t.chain_id,
      t.pool_address,
      t.bucket
    ON CONFLICT ON CONSTRAINT ohlc_unique DO NOTHING;
  $SQL$,
    -- format() parameters, in order:
               v_bucket_expr,   -- %s → bucket expression (date_trunc or 5-minute logic)
               p_interval,      -- %L → literal interval value (’1m’, ’5m’, ’1h’, ’1d’)
               v_last_bucket    -- %L → literal last_bucket timestamp
        );

END;
$$;

CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Schedule one job that runs every minute and calls all four intervals:
SELECT
  cron.schedule(
    'refresh_all_intervals',
    '*/1 * * * *',
    $cmd$
      DO $do$
      BEGIN
        CALL public.refresh_ohlc('1m');
        -- CALL public.refresh_ohlc('5m');
        CALL public.refresh_ohlc('1h');
        CALL public.refresh_ohlc('1d');
      END
      $do$;
    $cmd$
  );