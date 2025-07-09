use std::sync::Arc;
use anyhow::bail;
use log::{info, error};
use alloy::{dyn_abi::SolType, providers::{Provider, RootProvider}, pubsub::PubSubFrontend, rpc::types::Log, sol_types::SolEvent, transports::http::Http};
use tokio_postgres::Client;

use crate::{events::event_processor::{fetch_intent_initiator, update_intent_state, IntentStage, IntentVersions}, solidity_structs::{vault::Vault, CreatedOrder, SolidityVaultBoundMessage, VaultBoundMessagePlaceOrderData}};

pub async fn handle_received_message_on_vault_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let Vault::ReceivedMessageOnVault {
        origin,
        sender,
        message,
        provider,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "EVM Vault::ReceivedMessageOnVault";
    info!(target: log_target, "Vault::ReceivedMessageOnVault from id {origin} by {sender}, message = {message}, using provider = {provider}");

    let message_slice = message.as_ref();
    let decoded_message = match SolidityVaultBoundMessage::abi_decode(message_slice, true) {
        Ok(res) => res,
        Err(e) => {
            error!(target: log_target, "Failed to decode SolidityVaultBoundMessage in Vault::ReceivedMessageOnVault: {:?}", e);
            bail!("Failed to decode SolidityVaultBoundMessage in Vault::ReceivedMessageOnVault: {:?}", e);
        }
    };

    let decoded_message_data = match VaultBoundMessagePlaceOrderData::abi_decode(
        decoded_message.data.as_ref(),
        true,
    ) {
        Ok(data) => data,
        Err(e) => {
            error!(target: log_target, "Failed to decode VaultBoundMessagePlaceOrderData in Vault::ReceivedMessageOnVault: {:?}", e);
            bail!("Failed to decode VaultBoundMessagePlaceOrderData in Vault::ReceivedMessageOnVault: {:?}", e);
        }
    };

    let timeout_unix_timestamp_in_sec =
        decoded_message_data.order.timeoutUnixTimestampInSec as i64;

    let block_number = log.block_number.unwrap() as i64;
    let intent_id = decoded_message_data.order.intentId as i64;
    let order_id = decoded_message_data.order.orderId as i64;
    let origin_domain_id = origin as i32;
    let sender_address = decoded_message_data.order.initiatorAddress.to_string();
    let provider = provider as i32;
    let message = message.to_string();
    let timestamp = std::time::SystemTime::now();

    let transaction_hash = log.transaction_hash.unwrap();

    let dln_order_id = match chain_provider
        .get_transaction_receipt(transaction_hash)
        .await
    {
        Ok(receipt) => match receipt {
            Some(tx_receipt) => {
                let receipt_logs = tx_receipt.inner.logs();
                let order_id_log = receipt_logs
                    .iter()
                    .find(|log| log.topics()[0] == CreatedOrder::SIGNATURE_HASH);
                let dln_order_id = match order_id_log {
                    Some(log) => {
                        let primitive_log = alloy::primitives::Log {
                            address: log.address(),
                            data: log.data().clone(),
                        };
                        let decoded =
                            CreatedOrder::decode_log(&primitive_log, true).unwrap();
                        let debridge_order_id = decoded.orderId;
                        Some(debridge_order_id)
                    } // convert log into primite log
                    None => None,
                };
                dln_order_id
            }
            None => None,
        },
        Err(_e) => None,
    };

    let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (transaction_hash) DO NOTHING";
    match client
        .execute(
            query,
            &[
                &intent_id,
                &origin_domain_id,
                &sender_address,
                &message,
                &provider,
                &transaction_hash.to_string(),
                &block_number,
                &timestamp,
                &chain_id,
                &order_id,
                &dln_order_id.map(hex::encode),
                &timeout_unix_timestamp_in_sec,
            ],
        )
        .await
    {
        Ok(res) => {
            info!(
                target: log_target,
                "Vault::ReceivedMessageOnVault inserted response {:?}",
                res
            );
        }
        Err(e) => {
            error!(target: log_target, "Failed to add data into data: {e}");
        }
    };

    let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

    update_intent_state(
        &intent_id,
        IntentVersions::ReceivedMessageOnVault as i32,
        &IntentStage::Processing.to_string(),
        log.transaction_hash.unwrap(),
        &client,
        chain_provider.clone(),
        &order_id,
        chain_id,
        initiator_address,
    )
    .await;

    Ok(())
}