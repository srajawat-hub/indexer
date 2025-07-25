CREATE TABLE public.hook_executor_orders (
    id bigserial NOT NULL,
    protocol_id integer NOT NULL,
    order_hash character varying(66) NOT NULL,
    order_id bigint NOT NULL,
    recipient character varying(66) NOT NULL,
    token character varying(66) NOT NULL,
    amount NUMERIC NOT NULL,
    timeout_timestamp bigint NOT NULL,
    reason text,
    transaction_hash character varying(88) NOT NULL,
    block_number bigint NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    status integer DEFAULT 0 NOT NULL,
    destination_chain_id bigint NOT NULL,
    additional_data text,
    CONSTRAINT hook_executor_orders_pkey PRIMARY KEY (id),
    CONSTRAINT hook_executor_orders_order_hash_key UNIQUE (order_hash)
);
