-- Your SQL goes here
CREATE TABLE IF NOT EXISTS acknowledgement (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    sender_address character varying(44) NOT NULL,
    result boolean NOT NULL,
    error_message text,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    order_id bigint NOT NULL,
    metadata text DEFAULT ''::text
);

CREATE TABLE IF NOT EXISTS chain_metadata (
    chain_id text NOT NULL PRIMARY KEY,
    network_name text NOT NULL
);

CREATE TABLE IF NOT EXISTS deposit_received (
    id bigserial NOT NULL PRIMARY KEY,
    user_address text NOT NULL,
    token_address text NOT NULL,
    chain_id text NOT NULL,
    amount text NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    source_transaction_hash text,
    message_id text,
    status integer
);

CREATE TABLE IF NOT EXISTS intent (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    owner_address character varying(66) NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    feeamount text DEFAULT ''::text
);

CREATE TABLE IF NOT EXISTS intent_fees (
    id integer NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    fees jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS intent_state (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    version integer NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    stage text NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    gas_fees bigint,
    gas_token text,
    order_id bigint,
    chain_id bigint NOT NULL,
    initiator_address character varying(66) DEFAULT '0x'::character varying NOT NULL,
    transaction_cost text DEFAULT '0'::text,
    transaction_cost_usd text DEFAULT '0'::text
);

CREATE TABLE IF NOT EXISTS message_dispatched_from_vault (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    sender_address character varying(66) NOT NULL,
    destination_domain_id integer NOT NULL,
    provider integer NOT NULL,
    message text NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    order_id bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS order_created (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    creator_address character varying(66) NOT NULL,
    token_in character varying(66) NOT NULL,
    token_out character varying(66) NOT NULL,
    amount_in text NOT NULL,
    amount_out text NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    order_id bigint NOT NULL,
    source_chain_id text NOT NULL,
    destination_chain_id text NOT NULL,
    multi_leg boolean DEFAULT false NOT NULL,
    order_payload text DEFAULT ''::text NOT NULL,
    solution_type integer,
    receiver_type integer DEFAULT 0 NOT NULL,
    receiver_address text DEFAULT ''::text,
    amount_in_usd text DEFAULT '0'::text,
    amount_out_usd text DEFAULT '0'::text
);

CREATE TABLE IF NOT EXISTS received_message_on_vault (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    origin_domain_id integer NOT NULL,
    sender_address character varying(44) NOT NULL,
    message text NOT NULL,
    provider integer NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    chain_id bigint NOT NULL,
    order_id bigint NOT NULL,
    dln_order_id character varying(255),
    timeout_unix_timestamp_in_sec bigint DEFAULT 0
);

CREATE TABLE IF NOT EXISTS sanction_address_list (
    id integer NOT NULL PRIMARY KEY,
    address text NOT NULL
);

CREATE TABLE IF NOT EXISTS solution (
    id bigserial NOT NULL PRIMARY KEY,
    intent_id bigint NOT NULL,
    solver_address character varying(44) NOT NULL,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);

CREATE TABLE IF NOT EXISTS token_chains (
    id integer NOT NULL PRIMARY KEY,
    token_id text,
    address text,
    decimals integer,
    network text,
    address_bytes32 text
);

CREATE TABLE IF NOT EXISTS tokens (
    id text NOT NULL PRIMARY KEY,
    ticker text,
    full_name text,
    is_stable boolean,
    is_tradable boolean,
    price_usd numeric,
    description text,
    launch_date timestamp without time zone,
    website text,
    cmc_id text
);