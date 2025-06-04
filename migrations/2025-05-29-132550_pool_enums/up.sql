-- Your SQL goes here

CREATE TYPE pool_type_enum AS ENUM (
    'EVM',
    'SOLANA'
);

CREATE TYPE token_launch_type AS ENUM (
    'FAIR',
    'CURATED'
);