-- This file should undo anything in `up.sql`

-- Revert added column
ALTER TABLE pools DROP COLUMN launchpad_token;

-- Drop added UNIQUE constraints
ALTER TABLE liquidity DROP CONSTRAINT liquidity_unique_transaction_hash;
ALTER TABLE ammswap DROP CONSTRAINT ammswap_unique_transaction_hash;

ALTER TABLE intent DROP CONSTRAINT intent_unique_transaction_hash;
ALTER TABLE solution DROP CONSTRAINT solution_unique_transaction_hash;
ALTER TABLE order_created DROP CONSTRAINT order_created_unique_transaction_hash;
ALTER TABLE acknowledgement DROP CONSTRAINT acknowledgement_unique_transaction_hash;

ALTER TABLE deposit_received DROP CONSTRAINT deposit_received_unique_message_id;
ALTER TABLE intent_fees DROP CONSTRAINT intent_fees_unique_intent_id;
ALTER TABLE intent_state DROP CONSTRAINT unique_intent_version_tx;
ALTER TABLE message_dispatched_from_vault DROP CONSTRAINT message_dispatched_from_vault_unique_transaction_hash;
ALTER TABLE received_message_on_vault DROP CONSTRAINT received_message_on_vault_unique_transaction_hash;

-- Add back foreign key constraints
ALTER TABLE liquidity
ADD CONSTRAINT liquidity_pool_address_fkey
FOREIGN KEY (pool_address) REFERENCES pools(pool_address) ON DELETE CASCADE;

ALTER TABLE ammswap
ADD CONSTRAINT ammswap_pool_address_fkey
FOREIGN KEY (pool_address) REFERENCES pools(pool_address) ON DELETE CASCADE;