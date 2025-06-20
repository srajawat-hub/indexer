-- Your SQL goes here
ALTER TABLE pools ADD COLUMN launchpad_token TEXT NOT NULL DEFAULT '';

-- adding constraints
ALTER TABLE liquidity ADD CONSTRAINT liquidity_unique_transaction_hash UNIQUE (transaction_hash);
ALTER TABLE ammswap ADD CONSTRAINT ammswap_unique_transaction_hash UNIQUE (transaction_hash);

ALTER TABLE intent ADD CONSTRAINT intent_unique_transaction_hash UNIQUE (transaction_hash);
ALTER TABLE solution ADD CONSTRAINT solution_unique_transaction_hash UNIQUE (transaction_hash);
ALTER TABLE order_created ADD CONSTRAINT order_created_unique_transaction_hash UNIQUE (transaction_hash);
ALTER TABLE acknowledgement ADD CONSTRAINT acknowledgement_unique_transaction_hash UNIQUE (transaction_hash);

ALTER TABLE deposit_received ADD CONSTRAINT deposit_received_unique_message_id UNIQUE (message_id);
ALTER TABLE intent_fees ADD CONSTRAINT intent_fees_unique_intent_id UNIQUE (intent_id);
ALTER TABLE intent_state ADD CONSTRAINT unique_intent_version_tx UNIQUE (intent_id, version, transaction_hash);
ALTER TABLE message_dispatched_from_vault ADD CONSTRAINT message_dispatched_from_vault_unique_transaction_hash UNIQUE (transaction_hash);
ALTER TABLE received_message_on_vault ADD CONSTRAINT received_message_on_vault_unique_transaction_hash UNIQUE (transaction_hash);

-- remove foreign key constraints
ALTER TABLE liquidity DROP CONSTRAINT liquidity_pool_address_fkey;
ALTER TABLE ammswap DROP CONSTRAINT ammswap_pool_address_fkey;