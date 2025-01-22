```
intent (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    owner_address VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
solution (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    solver_address VARCHAR(44) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
acknowledgement (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    sender_address VARCHAR(44) NOT NULL,
    result BOOLEAN NOT NULL,
    error_message TEXT,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_id BIGINT NOT NULL
);
received_message_on_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    origin_domain_id INTEGER NOT NULL,
    sender_address VARCHAR(44) NOT NULL,
    message TEXT NOT NULL,
    provider INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    chain_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL
);
order_created (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    creator_address VARCHAR(66) NOT NULL,
    token_in VARCHAR(66) NOT NULL,
    token_out VARCHAR(66) NOT NULL,
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_id BIGINT NOT NULL,
    source_chain_id TEXT NOT NULL,
    destination_chain_id TEXT NOT NULL,
    multi_leg BOOL NOT NULL DEFAULT false
);
message_dispatched_from_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    sender_address VARCHAR(66) NOT NULL,
    destination_domain_id INTEGER NOT NULL,
    provider INTEGER NOT NULL,
    message TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_id BIGINT NOT NULL,
);
intent_state (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    version INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    stage TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    gas_fees BIGINT,
    gas_token TEXT,
    order_id BIGINT,
    chain_id BIGING NOT NULL,
    initiator_address VARCHAR(66) NOT NULL
);
```
