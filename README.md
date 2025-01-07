```
intent_submitted (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    owner_address VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
solution_submitted (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    solver_address VARCHAR(44) NOT NULL,
    solution TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
acknowledgement_received (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    sender_address VARCHAR(44) NOT NULL,
    result BOOLEAN NOT NULL,
    error_message TEXT,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
received_message_on_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    origin_domain_id INTEGER NOT NULL,
    sender_address VARCHAR(44) NOT NULL,
    message TEXT NOT NULL,
    provider INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
order_created (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    creator_address VARCHAR(66) NOT NULL,
    token_in VARCHAR(66) NOT NULL,
    token_out VARCHAR(66) NOT NULL,
    amount_in NUMERIC(38, 18) NOT NULL,
    amount_out NUMERIC(38, 18) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
message_dispatched_from_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    sender_address VARCHAR(66) NOT NULL,
    destination_domain_id INTEGER NOT NULL,
    provider INTEGER NOT NULL,
    message TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
intent_state (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    version INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    stage TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
```

Index the raw events from each event on a separate table (see if any merges are possible)
then create a separate state table with intent_id and txn hash. Track the state of intent_id with versions of progressions as more of the raw intents come in.

update the schema for this flow