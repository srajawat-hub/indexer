```
<!-- intent_submitted (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    owner_address VARCHAR(44) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

solution_submitted (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    solver_address VARCHAR(44) NOT NULL,
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
); -->

<!-- received_message_on_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    origin_domain_id INTEGER NOT NULL,
    sender_address VARCHAR(44) NOT NULL,
    message TEXT NOT NULL,
    provider INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
); -->

<!-- message_dispatched_from_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    sender_address VARCHAR(44) NOT NULL,
    destination_domain_id INTEGER NOT NULL,
    provider INTEGER NOT NULL,
    message TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
); -->

intent_processor (
    id SERIAL PRIMARY KEY,               -- Unique identifier for the event record
    intent_id INT NOT NULL,              -- Common identifier for the transaction cycle
    event_type VARCHAR(50) NOT NULL,     -- Event type (e.g., "IntentSubmitted", "SolutionSubmitted", "AcknowledgementReceived")
    transaction_hash VARCHAR(255),       -- Transaction hash (can be from EVM or Solana, length adjusted)
    origin_domain_id INT,                -- Optional field to link to the origin domain, if relevant
    sender VARCHAR(255),                 -- The sender (address or identifier)
    solver VARCHAR(255),                 -- For solution-submitted events
    result BOOLEAN,                      -- For acknowledgement-received events, to store success/failure
    error_message TEXT,                  -- Optional error message for failure scenarios
    block_number INT,                    -- Block number where event was recorded
    block_timestamp TIMESTAMP,           -- Timestamp of the event
    UNIQUE (intent_id, event_type, block_number) -- Ensure unique combination per event type
);

order_created (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL REFERENCES intent_submitted(intent_id),
    creator_address VARCHAR(44) NOT NULL,
    token_in VARCHAR(44) NOT NULL,
    token_out VARCHAR(44) NOT NULL,
    amount_in NUMERIC(38, 18) NOT NULL,
    amount_out NUMERIC(38, 18) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);

vault_messages (
    id SERIAL PRIMARY KEY,               -- Unique identifier for the message record
    intent_id INT NOT NULL,              -- Common identifier for the transaction cycle
    message_direction VARCHAR(50) NOT NULL,  -- 'Received' or 'Dispatched' to indicate message direction
    origin_domain_id INT,                -- The domain ID that initiated the message (if relevant)
    sender VARCHAR(255),                 -- The sender of the message
    destination_domain INT,              -- Destination domain for dispatched messages
    provider VARCHAR(255),               -- Provider handling the message (if relevant)
    message TEXT,                        -- The content of the message
    transaction_hash VARCHAR(255),       -- Transaction hash (can come from EVM or Solana)
    block_number INT,                    -- Block number where event was recorded
    block_timestamp TIMESTAMP,           -- Timestamp of the event
    UNIQUE (intent_id, message_direction, block_number) -- Ensure unique combination of intent_id and message_direction
);
```