```
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

order (
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

Tracking all transactions happening on IP, Vault, Mockln contract in an operation lifecycle (stake/transfer/swap)
Tracking (
    id
    intent_id 
    tx_hash
    chain_id ( fk to chain table which has further information )
    stage ( can be an ENUM )
    interop_provider ( ENUM )
    source_domain
    destination_domain
    gas_cost
    created_at
    updated_at                                      -- when the stage of tracking updates
)
```