use alloy::{
    providers::{Provider, RootProvider},
    pubsub::PubSubFrontend,
    rpc::types::Log,
    sol_types::SolEvent,
    transports::http::Http,
};
use log::{error, info};
use std::sync::Arc;
use tokio_postgres::Client;

use crate::{
    events::event_processor::DepositStatus,
    solidity_structs::{intent_processor::IntentProcessorV2, ProcessId},
};

pub async fn handle_deposit_received_event(
    log: Log,
    client: &Arc<Client>,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let IntentProcessorV2::DepositReceived {
        userAddress,
        tokenAddress,
        chainId,
        amount,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "DepositReceived";
    info!(target: log_target, "IntentProcessorV2::DepositReceived from user {userAddress}");
    let chain_id = chainId.to_string();
    let amount = amount.to_string();
    let user_address = userAddress.to_string();
    let token_address = tokenAddress.to_string();
    let timestamp = std::time::SystemTime::now();

    let transaction_hash = log.transaction_hash.unwrap();

    // get message_id from hyperlane process event; get the event log from the transaction receipt
    let process_message_id = match chain_provider
        .get_transaction_receipt(transaction_hash)
        .await
    {
        Ok(receipt) => match receipt {
            Some(tx_receipt) => {
                let receipt_logs = tx_receipt.inner.logs();
                let process_id_log = receipt_logs
                    .iter()
                    .find(|log| log.topics()[0] == ProcessId::SIGNATURE_HASH);
                let message_process_id = match process_id_log {
                    Some(log) => {
                        let primitive_log = alloy::primitives::Log {
                            address: log.address(),
                            data: log.data().clone(),
                        };
                        let decoded = ProcessId::decode_log(&primitive_log, true).unwrap();
                        let process_message_id = decoded.messageId;
                        Some(process_message_id)
                    } // convert log into primite log
                    None => None,
                };
                message_process_id
            }
            None => None,
        },
        Err(_e) => None,
    };
    let deposit_status = DepositStatus::Done as i32;
    match process_message_id {
        Some(id) => {
            let message_id = id.to_string();
            let query = "INSERT INTO deposit_received VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (message_id) DO UPDATE SET status = EXCLUDED.status WHERE deposit_received.status <> 1;";
            let _deposit_transaction = match client
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
                        &deposit_status,
                    ],
                )
                .await
            {
                Ok(_row) => {
                    info!(target: log_target, "Successfully updated deposit status with message_id {:?}", message_id);
                }
                Err(_e) => {
                    error!(target: log_target, "Error updating deposit status with message_id {:?} with error {:?}", message_id, _e);
                }
            };
        }
        None => {
            // if not, we will have to add this into history
            let query =
                "INSERT INTO deposit_received VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
            let response = client
                .execute(
                    query,
                    &[
                        &user_address,
                        &token_address,
                        &chain_id,
                        &amount,
                        &timestamp,
                        &transaction_hash.to_string(),
                        &deposit_status,
                    ],
                )
                .await
                .unwrap();
            info!(
                target: log_target, "IntentProcessorV2::DepositReceived inserted a fallback response {:?}",
                response
            );
        }
    }

    Ok(())
}
