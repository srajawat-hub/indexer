use std::sync::Arc;

use alloy::primitives::FixedBytes;
use alloy::providers::Provider;
use alloy::rpc::types::Log;
use alloy::sol_types::SolValue;
use alloy::{pubsub::SubscriptionStream, sol_types::SolEvent};
use futures_util::stream::StreamExt;
use log::{debug, info};
use tokio_postgres::Client;

use crate::solidity_structs::{
    self, AcknowledgementMetadataStake, AcknowledgementMetadataTransact, CreatedOrder,
    IntentProcessorBoundMessageAcknowledgementData, ReceiverUserAddressData, ReceiverVaultData,
    SolidityAcknowledgementMetadata, SolidityIntentProcessorBoundMessage, SolidityOrder,
    SolidityVaultBoundMessage, VaultBoundMessagePlaceOrderData,
};
use crate::solidity_structs::{
    intent_lib_v2::IntentLibV2, intent_processor::IntentProcessorV2, vault::Vault,
};

pub enum IntentVersions {
    IntentSubmitted,
    SolutionSubmitted,
    OrderCreated,
    ReceivedMessageOnVault,
    MessageDispatchedFromVault,
    AcknowledgementReceived,
}

#[derive(Debug)]
pub enum IntentStage {
    Initialized,
    Processing,
    Done,
    Failed,
}

impl ToString for IntentStage {
    fn to_string(&self) -> String {
        match self {
            IntentStage::Initialized => "Initialized".to_string(),
            IntentStage::Processing => "Processing".to_string(),
            IntentStage::Done => "Done".to_string(),
            IntentStage::Failed => "Failed".to_string(),
        }
    }
}

pub async fn update_intent_state(
    intent_id: &i64,
    version: i32,
    stage: &str,
    transaction_hash: FixedBytes<32>,
    client: &Arc<Client>,
    provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend>,
    order_id: &i64,
    chain_id: i64,
    initiator_address: String,
) {
    // let gas_fees = 1 as i64; // updating gas token
    let query =
        "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, DEFAULT, $7, $8, $9)";
    let timestamp = std::time::SystemTime::now();

    let gas_used: i64 = match provider.get_transaction_receipt(transaction_hash).await {
        Ok(receipt) => {
            let txn = receipt.unwrap();
            let gas_used = txn.gas_used as i64;
            gas_used
        }
        Err(_e) => 0 as i64,
    };
    let txn_hash_str = transaction_hash.to_string();
    let _intent_state_response = client
        .execute(
            query,
            &[
                &intent_id,
                &version,
                &txn_hash_str,
                &stage,
                &timestamp,
                &gas_used,
                &order_id,
                &chain_id,
                &initiator_address,
            ],
        )
        .await
        .unwrap();
    info!("Intent State Updated for intent id: {intent_id} to version: {version}");
}

pub async fn fetch_intent_initiator(intent_id: i64, client: &Arc<Client>) -> String {
    let query = "SELECT owner_address FROM intent WHERE intent_id = $1";
    let response = client.query_one(query, &[&intent_id]).await.unwrap();
    let initiator_address: String = response.get("owner_address");
    initiator_address
}

// Function to listen to events from a specific RPC and contract
pub async fn process_evm_events(
    mut stream: SubscriptionStream<Log>,
    client: Arc<Client>,
    chain_id: i64,
    chain_provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend>,
) {
    while let Some(log) = stream.next().await {
        match log.topic0() {
            Some(&IntentLibV2::IntentSubmitted::SIGNATURE_HASH) => {
                let IntentLibV2::IntentSubmitted { intentId, owner } =
                    log.log_decode().unwrap().inner.data;
                info!("IntentLibV2::IntentSubmitted from {owner} with intentId {intentId}");

                let intent_transaction_hash = log.transaction_hash.unwrap();
                let intent_block_number = log.block_number.unwrap();
                let intent_id = intentId as i64;
                let owner_address = owner.to_string();
                let transaction_hash = intent_transaction_hash.to_string();
                let block_number = intent_block_number as i64;
                let current_timestamp = std::time::SystemTime::now();
                let timestamp = current_timestamp;

                let query = "INSERT INTO intent VALUES(DEFAULT, $1, $2, $3, $4, $5)";
                let response = client
                    .execute(
                        query,
                        &[
                            &intent_id,
                            &owner_address,
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                        ],
                    )
                    .await
                    .unwrap();
                info!(
                    "IntentLibV2::IntentSubmitted inserted response {:?}",
                    response
                );

                let order_id: i64 = 0;

                update_intent_state(
                    &intent_id,
                    IntentVersions::IntentSubmitted as i32,
                    &IntentStage::Initialized.to_string(),
                    log.transaction_hash.unwrap(),
                    &client,
                    chain_provider.clone(),
                    &order_id,
                    chain_id,
                    owner_address,
                )
                .await;
            }
            Some(&IntentLibV2::SolutionSubmitted::SIGNATURE_HASH) => {
                let IntentLibV2::SolutionSubmitted { intentId, solver } =
                    log.log_decode().unwrap().inner.data;

                info!("IntentLibV2::SolutionSubmitted from {solver} with intentId {intentId}");

                let solution_transaction_hash = log.transaction_hash.unwrap();
                let intent_block_number = log.block_number.unwrap();
                let intent_id = intentId as i64;
                let solver_address = solver.to_string();
                let transaction_hash = solution_transaction_hash.to_string();
                let block_number = intent_block_number as i64;
                let timestamp = std::time::SystemTime::now();

                let query = "INSERT INTO solution VALUES(DEFAULT, $1, $2, $3, $4, $5)";
                let response = client
                    .execute(
                        query,
                        &[
                            &intent_id,
                            &solver_address,
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                        ],
                    )
                    .await
                    .unwrap();
                info!(
                    "IntentLibV2::SolutionSubmitted inserted response {:?}",
                    response
                );

                let order_id: i64 = 0;
                let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

                update_intent_state(
                    &intent_id,
                    IntentVersions::SolutionSubmitted as i32,
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
            Some(&IntentLibV2::OrderCreated::SIGNATURE_HASH) => {
                let IntentLibV2::OrderCreated {
                    intentId,
                    orderId,
                    order,
                } = log.log_decode().unwrap().inner.data;
                info!("\nIntentLibV2::OrderCreated for {intentId}, with order Id {orderId}");

                debug!("order data {order}");
                let order_slice = order.as_ref();
                let order_struct = SolidityOrder::abi_decode(order_slice, true).unwrap();

                let intent_id = order_struct.intentId as i64;
                let order_id = order_struct.orderId as i64;
                let creator_address = order_struct.initiatorAddress.to_string();
                let token_in = order_struct.tokenIn.to_string();
                let token_out = order_struct.tokenOut.to_string();
                let amount_in = order_struct.amountIn.to_string();
                let amount_out = order_struct.amountOut.to_string();
                let transaction_hash = log.transaction_hash.unwrap().to_string();
                let block_number = log.block_number.unwrap() as i64;
                let source_chain_id = order_struct.sourceChainId.to_string();
                let destination_chain_id = order_struct.destinationChainId.to_string();
                let multi_leg = order_struct.multiLeg;
                let order_payload: String = order.to_string();
                let solution_type = order_struct.solution.enumVariant as i32;
                let receiver_type: i32 = order_struct.receiver.enumVariant as i32;

                let receiver_data = order_struct.receiver.data;
                let receiver_address: String;
                if receiver_type == 0 {
                    let receiver_address_struct =
                        ReceiverUserAddressData::abi_decode(&receiver_data, true).unwrap();
                    receiver_address = receiver_address_struct.userAddress.to_string();
                } else {
                    let receiver_address_struct =
                        ReceiverVaultData::abi_decode(&receiver_data, true).unwrap();
                    receiver_address = receiver_address_struct.vaultUser.to_string();
                }

                let current_timestamp = std::time::SystemTime::now();
                let timestamp = current_timestamp;

                let query: &str =
                    "INSERT INTO order_created VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";
                let response = client
                    .execute(
                        query,
                        &[
                            &intent_id,
                            &creator_address,
                            &token_in,
                            &token_out,
                            &amount_in,
                            &amount_out,
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                            &order_id,
                            &source_chain_id,
                            &destination_chain_id,
                            &multi_leg,
                            &order_payload,
                            &solution_type,
                            &receiver_type,
                            &receiver_address,
                        ],
                    )
                    .await
                    .unwrap();
                info!("IntentLibV2::OrderCreated inserted response {:?}", response);

                let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

                update_intent_state(
                    &intent_id,
                    IntentVersions::OrderCreated as i32,
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
            Some(&IntentProcessorV2::AcknowledgementReceived::SIGNATURE_HASH) => {
                debug!("\nAcknowledgementReceived log - {:?}", log);
                let IntentProcessorV2::AcknowledgementReceived {
                    orderId,
                    sender,
                    result,
                    errorMessage,
                    metadata,
                } = log.log_decode().unwrap().inner.data;
                info!("IntentProcessorV2::AcknowledgementReceived for orderId - {orderId} from {sender} with result {result}");

                let transaction_hash = log.transaction_hash.unwrap().to_string();
                let block_number = log.block_number.unwrap() as i64;
                let order_id = orderId as i64; // its the order id
                let sender_address = sender.to_string();
                let result = result;
                let error_message = errorMessage;
                let timestamp = std::time::SystemTime::now();
                let ack_metadata: String = metadata.to_string();

                // fetching intent id
                let intent_id_query = "SELECT intent_id FROM order_created WHERE order_id = $1";
                let intent_id_response = client
                    .query_one(intent_id_query, &[&order_id])
                    .await
                    .unwrap();
                let intent_id: i64 = intent_id_response.get("intent_id");

                let query =
                    "INSERT INTO acknowledgement VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9)";
                let response = client
                    .execute(
                        query,
                        &[
                            &intent_id,
                            &sender_address,
                            &result,
                            &error_message,
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                            &order_id,
                            &ack_metadata,
                        ],
                    )
                    .await
                    .unwrap();
                info!(
                    "IntentProcessorV2::AcknowledgementReceived inserted response {:?}",
                    response
                );

                let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

                let intent_stage = match result {
                    true => &IntentStage::Done.to_string(),
                    false => &IntentStage::Failed.to_string(),
                };

                update_intent_state(
                    &intent_id,
                    IntentVersions::AcknowledgementReceived as i32,
                    intent_stage,
                    log.transaction_hash.unwrap(),
                    &client,
                    chain_provider.clone(),
                    &order_id,
                    chain_id,
                    initiator_address,
                )
                .await;

                let decoded_ack_metadata =
                    SolidityAcknowledgementMetadata::abi_decode(metadata.as_ref(), true).unwrap();
                if (decoded_ack_metadata.data.len() > 0) {
                    let mut actual_amount = String::new();
                    let metadata_variant = decoded_ack_metadata.enumVariant as u8;
                    if (metadata_variant == 1) {
                        // stake
                        let metadata_data = AcknowledgementMetadataStake::abi_decode(
                            decoded_ack_metadata.data.as_ref(),
                            true,
                        )
                        .unwrap();
                        actual_amount = metadata_data.amountCredited.to_string();
                    } else {
                        // transact
                        let metadata_data = AcknowledgementMetadataTransact::abi_decode(
                            decoded_ack_metadata.data.as_ref(),
                            true,
                        )
                        .unwrap();
                        actual_amount = metadata_data.amount.to_string();
                    }

                    let update_order_query = "
                    UPDATE order_created
                    SET amount_out = $1
                    WHERE order_id = $2
                    AND (SELECT COUNT(*) FROM order_created WHERE order_id = $2) = 1
                    ";

                    let order_rows_updated = client
                        .execute(update_order_query, &[&actual_amount, &order_id])
                        .await
                        .unwrap();
                    info!(
                        "updated actual amount for order, updated rows count {:?}",
                        order_rows_updated
                    );
                }
            }
            Some(&IntentProcessorV2::DepositReceived::SIGNATURE_HASH) => {
                let IntentProcessorV2::DepositReceived {
                    userAddress,
                    tokenAddress,
                    chainId,
                    amount,
                } = log.log_decode().unwrap().inner.data;

                info!("IntentProcessorV2::DepositReceived from user {userAddress}");
                let chain_id = chainId.to_string();
                let amount = amount.to_string();
                let user_address = userAddress.to_string();
                let token_address = tokenAddress.to_string();
                let timestamp = std::time::SystemTime::now();

                let query = "INSERT INTO deposit_received VALUES (DEFAULT, $1, $2, $3, $4, $5)";
                let response = client
                    .execute(
                        query,
                        &[
                            &user_address,
                            &token_address,
                            &chain_id,
                            &amount,
                            &timestamp,
                        ],
                    )
                    .await
                    .unwrap();
                info!(
                    "IntentProcessorV2::DepositReceived inserted response {:?}",
                    response
                );
            }
            Some(&Vault::ReceivedMessageOnVault::SIGNATURE_HASH) => {
                let Vault::ReceivedMessageOnVault {
                    origin,
                    sender,
                    message,
                    provider,
                } = log.log_decode().unwrap().inner.data;

                info!("Vault::ReceivedMessageOnVault from id {origin} by {sender}, message = {message}, using provider = {provider}");

                let message_slice = message.as_ref();
                let decoded_message =
                    SolidityVaultBoundMessage::abi_decode(message_slice, true).unwrap();
                let decoded_message_data = VaultBoundMessagePlaceOrderData::abi_decode(
                    decoded_message.data.as_ref(),
                    true,
                )
                .unwrap();

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

                let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)";
                let response = client
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
                    .unwrap();
                info!(
                    "Vault::ReceivedMessageOnVault inserted response {:?}",
                    response
                );

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
            }
            Some(&Vault::MessageDispatchedFromVault::SIGNATURE_HASH) => {
                let Vault::MessageDispatchedFromVault {
                    sender,
                    destinationDomain,
                    provider,
                    message,
                } = log.log_decode().unwrap().inner.data;
                debug!("\nMessageDispatchedFromVault log - {:?}", log);
                // have to get intentId here.
                info!("Vault::MessageDispatchedFromVault received from {sender} to destination {destinationDomain}");

                let decoded_message =
                    SolidityIntentProcessorBoundMessage::abi_decode(message.as_ref(), true)
                        .unwrap();
                let decoded_message_data =
                    IntentProcessorBoundMessageAcknowledgementData::abi_decode(
                        decoded_message.data.as_ref(),
                        true,
                    );
                let decoded_message_data = if let Ok(decoded_message_data) = decoded_message_data {
                    decoded_message_data
                } else {
                    debug!("Error decoding message data");
                    continue;
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
                let intent_id_query = "SELECT intent_id FROM order_created WHERE order_id = $1";
                let intent_id_response = client
                    .query_one(intent_id_query, &[&order_id])
                    .await
                    .unwrap();
                let intent_id: i64 = intent_id_response.get("intent_id");

                let query =
                    "INSERT INTO message_dispatched_from_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9)";
                let response = client
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
                    .unwrap();
                info!(
                    "Vault::MessageDispatchedFromVault inserted response {:?}",
                    response
                );

                let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

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
            Some(&solidity_structs::DebridgeOrderCreated::SIGNATURE_HASH) => {
                let solidity_structs::DebridgeOrderCreated {
                    orderId,
                    debridgeOrderId,
                } = log.log_decode().unwrap().inner.data;
                info!("solidity_structs::DebridgeOrderCreated from {orderId} with debridgeOrderId {debridgeOrderId}");

                let order_id = orderId as i64;

                let debridge_order_id = hex::encode(debridgeOrderId);

                let query =
                    "UPDATE received_message_on_vault SET dln_order_id = $1 WHERE order_id = $2";
                let response = client
                    .execute(query, &[&debridge_order_id, &order_id])
                    .await
                    .unwrap();
                info!(
                    "solidity_structs::DebridgeOrderCreated updated response {:?}",
                    response
                );
            }
            _ => {
                info!("\ndidn't match any event, {:?}", log);
            }
        }
    }
}
