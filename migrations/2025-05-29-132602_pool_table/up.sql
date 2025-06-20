-- Your SQL goes here
CREATE TABLE pools (
    id BIGSERIAL PRIMARY KEY,
    pool_address TEXT NOT NULL UNIQUE,
    chain_id BIGINT NOT NULL,
    token_0_address TEXT NOT NULL,
    token_1_address TEXT NOT NULL,
    fee NUMERIC NOT NULL,
    tick_spacing BIGINT NOT NULL,
    pool_type pool_type_enum NOT NULL,
    project_manager TEXT NOT NULL,
    block_number BIGINT NOT NULL,
    created_at TIMESTAMP NOT NULL,
    metadata JSONB,
    etp_start_time TIMESTAMP NOT NULL,
    etp_end_time TIMESTAMP NOT NULL,
    launch_type token_launch_type NOT NULL,
    initial_sqrt_price TEXT NOT NULL, -- Keeping it as text as it can overflow from i64 required for tokio-postgres BIGINT
    initial_tick INTEGER NOT NULL,
    token_supply TEXT NOT NULL -- Keeping it as text as it can overflow from i64 required for tokio-postgres BIGINT
);

CREATE TABLE liquidity (
    id BIGSERIAL PRIMARY KEY,
    pool_address TEXT NOT NULL REFERENCES pools(pool_address) ON DELETE CASCADE,
    user_address TEXT NOT NULL,
    is_add BOOLEAN NOT NULL,
    position_id TEXT NOT NULL,
    token_0_amount NUMERIC NOT NULL,
    token_1_amount NUMERIC NOT NULL,
    chain_id BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    transaction_hash TEXT NOT NULL UNIQUE,
    is_Manager BOOLEAN NOT NULL,
    liquidity BIGINT NOT NULL,
    is_vault BOOLEAN NOT NULL
);

CREATE TABLE AmmSwap (
    id BIGSERIAL PRIMARY KEY,
    pool_address TEXT NOT NULL REFERENCES pools(pool_address) ON DELETE CASCADE,
    token_in TEXT NOT NULL,
    token_out TEXT NOT NULL,
    amount_in NUMERIC NOT NULL,
    amount_out NUMERIC NOT NULL,
    chain_id BIGINT NOT NULL,
    amount_in_usd NUMERIC NOT NULL,
    amount_out_usd NUMERIC NOT NULL,
    initiator_user_address TEXT NOT NULL,
    price NUMERIC NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    transaction_hash TEXT NOT NULL UNIQUE,
    block_number BIGINT NOT NULL,
    is_vault_initiated BOOLEAN NOT NULL,
    sqrt_price TEXT NOT NULL, -- can overflow for i64 required for tokio-postgres BIGINT
    liquidity BIGINT NOT NULL,
    tick INTEGER NOT NULL
);