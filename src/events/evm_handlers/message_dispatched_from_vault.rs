use std::sync::Arc;
use anyhow::bail;
use log::{info, error, debug};
use alloy::{dyn_abi::SolType, providers::{Provider, RootProvider}, pubsub::PubSubFrontend, rpc::types::Log, sol_types::SolEvent, transports::http::Http};
use tokio_postgres::Client;

use crate::{events::event_processor::{fetch_intent_initiator, update_intent_state, DepositStatus, IntentStage, IntentVersions}, solidity_structs::{vault::Vault, DispatchId, IntentProcessorBoundMessageAcknowledgementData, IntentProcessorBoundMessageDepositData, SolidityIntentProcessorBoundMessage}};

pub async fn handle_message_dispatched_from_vault_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let Vault::MessageDispatchedFromVault {
        sender,
        destinationDomain,
        provider,
        message,
    } = log.log_decode().unwrap().inner.data;
    debug!(target: "EVM Vault::MessageDispatchedFromVault", "\nMessageDispatchedFromVault log - {:?}", log);
    // have to get intentId here.
    info!(target: "EVM Vault::MessageDispatchedFromVault", "Vault::MessageDispatchedFromVault received from {sender} to destination {destinationDomain}");

    let decoded_message = match SolidityIntentProcessorBoundMessage::abi_decode(
        message.as_ref(),
        true,
    ) {
        Ok(res) => res,
        Err(e) => {
            error!(target: "EVM Vault::MessageDispatchedFromVault", "Failed to decode SolidityIntentProcessorBoundMessage in Vault::MessageDispatchedFromVault: {:?}", e);
            bail!("Failed to decode SolidityIntentProcessorBoundMessage in Vault::MessageDispatchedFromVault: {:?}", e);
        }
    };

    let message_variant = decoded_message.enumVariant as u8;

    match message_variant {
        2 => {
            let log_target = "EVM Vault::MessageDispatchedFromVault Ack";
            // ack
            let decoded_message_data =
                IntentProcessorBoundMessageAcknowledgementData::abi_decode(
                    decoded_message.data.as_ref(),
                    true,
                );
            let decoded_message_data = if let Ok(decoded_message_data) =
                decoded_message_data
            {
                decoded_message_data
            } else {
                error!(target: log_target, "Error decoding IntentProcessorBoundMessageAcknowledgement message data");
                bail!("Error decoding IntentProcessorBoundMessageAcknowledgement message data");
            };

            let order_id = decoded_message_data.orderId as i64;
            let sender_address = log.address().to_string();
            let destination_domain_id = destinationDomain as i32;
            let provider = provider as i32;
            let message = message.to_string();
            let transaction_hash = log.transaction_hash.unwrap();

            let block_number = log.block_number.unwrap() as i64;
            let timestamp = std::time::SystemTime::now();

            // fetch intent_id
            let intent_id_from_vault = "SELECT intent_id, sender_address FROM received_message_on_vault WHERE order_id = $1";
            let (intent_id, initiator_address) = match client
                .query_one(intent_id_from_vault, &[&order_id])
                .await
            {
                Ok(row) => {
                    let intent_id: i64 = row.get("intent_id");
                    let sender_address: String = row.get("sender_address");
                    info!(target: log_target, "Fetched intent_id {:?} for order_id {:?} from received_message_on_vault", intent_id, order_id);
                    (intent_id, sender_address)
                },
                Err(e) => {
                    error!(target: log_target, "Failed to fetch intent_id for event Vault::MessageDispatchedFromVault for order_id {:?}: {:?}", order_id, e);
                    // as a fallback try to get it from order_created table
                    let intent_id_query = "SELECT intent_id, creator_address FROM order_created WHERE order_id = $1";
                    match client.query_one(
                        intent_id_query,
                        &[&order_id],
                    ).await {
                        Ok(row) => {
                            let intent_id: i64 = row.get("intent_id");
                            let creator_address: String = row.get("creator_address");
                            (intent_id, creator_address)
                        },
                        Err(e) => {
                            error!(target: log_target, "Failed to fetch intent_id from order_created for order_id {:?}: {:?}", order_id, e);
                            (0_i64, String::new()) // Fallback to 0 if not found
                        }
                    }
                }
            };

            let query =
                "INSERT INTO message_dispatched_from_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (transaction_hash) DO NOTHING";
            match client
                .execute(
                    query,
                    &[
                        &intent_id,
                        &sender_address,
                        &destination_domain_id,
                        &provider,
                        &message,
                        &transaction_hash.to_string(),
                        &block_number,
                        &timestamp,
                        &order_id,
                    ],
                )
                .await
            {
                Ok(res) => {
                    info!(
                        target: log_target,
                        "Vault::MessageDispatchedFromVault inserted response {:?}",
                        res
                    );
                }
                Err(e) => {
                    error!(target: log_target, "Failed to add data into data");
                }
            };

            update_intent_state(
                &intent_id,
                IntentVersions::MessageDispatchedFromVault as i32,
                &IntentStage::Processing.to_string(),
                log.transaction_hash.unwrap(),
                &client,
                chain_provider.clone(),
                &order_id,
                chain_id,
                initiator_address,
            )
            .await;
        }
        3 => {
            let log_target = "EVM Vault::MessageDispatchedFromVault Deposit";
            let deposit_message_data = IntentProcessorBoundMessageDepositData::abi_decode(
                &decoded_message.data,
                true,
            )
            .unwrap();
            let deposit_user_address = deposit_message_data.userAddress;
            let user_address = deposit_user_address.to_string();
            let amount = deposit_message_data.amount.to_string();
            let token_address = deposit_message_data.tokenAddress.to_string();

            // deposit message
            let transaction_hash = log.transaction_hash.unwrap(); // source_transaction_hash
            let message_id = match chain_provider
                .get_transaction_receipt(transaction_hash)
                .await
            {
                Ok(receipt) => match receipt {
                    Some(tx_receipt) => {
                        let receipt_logs = tx_receipt.inner.logs();

                        let dispatch_id_log = receipt_logs
                            .iter()
                            .find(|log| log.topics()[0] == DispatchId::SIGNATURE_HASH);

                        let dispatch_id = match dispatch_id_log {
                            Some(log) => {
                                let primitive_log = alloy::primitives::Log {
                                    address: log.address(),
                                    data: log.data().clone(),
                                };
                                let decoded =
                                    DispatchId::decode_log(&primitive_log, true).unwrap();
                                let message_dispatch_id = decoded.messageId;
                                Some(message_dispatch_id.to_string())
                            }
                            None => None,
                        };
                        dispatch_id
                    }
                    None => None,
                },
                Err(_e) => None,
            };

            info!(target: log_target, "message_id from source {:?}", message_id);
            info!(target: log_target, "user_address from source {:?}", user_address);

            let chain_id = match chain_provider.get_chain_id().await {
                Ok(id) => id.to_string(),
                Err(_e) => {
                    error!(target: log_target, "Error fetching the chain id for deposit message source {:?}", _e);
                    String::new()
                }
            };
            let timestamp = std::time::SystemTime::now();
            let status = DepositStatus::Initialized as i32;

            let query = "INSERT INTO deposit_received VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
            match client
                .execute(
                    query,
                    &[
                        &user_address,
                        &token_address,
                        &chain_id,
                        &amount,
                        &timestamp,
                        &transaction_hash.to_string(),
                        &message_id,
                        &status,
                    ],
                )
                .await
            {
                Ok(res) => {
                    info!(
                        target: log_target,
                        "IntentLib::DepositedFunds inserted response {:?}",
                        res
                    );
                }
                Err(e) => {
                    error!(target: log_target, "Failed to insert data for deposit with message_id: {:?}, error: {e}", &message_id);
                }
            };
        }
        v => bail!("Unknown message variant {v} in MessageDispatchedFromVault"),
    };

    Ok(())
}