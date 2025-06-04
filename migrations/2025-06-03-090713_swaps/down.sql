-- This file should undo anything in `up.sql`
DROP TABLE IF EXISTS acknowledgement;
DROP TABLE IF EXISTS chain_metadata;
DROP TABLE IF EXISTS deposit_received;
DROP TABLE IF EXISTS intent;
DROP TABLE IF EXISTS intent_fees;
DROP TABLE IF EXISTS intent_state;
DROP TABLE IF EXISTS message_dispatched_from_vault;
DROP TABLE IF EXISTS order_created;
DROP TABLE IF EXISTS received_message_on_vault;
DROP TABLE IF EXISTS sanction_address_list;
DROP TABLE IF EXISTS solution;
DROP TABLE IF EXISTS token_chains;
DROP TABLE IF EXISTS tokens;
