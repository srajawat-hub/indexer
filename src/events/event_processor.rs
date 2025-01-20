use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::Bytes;
use alloy::rpc::types::Log;
use alloy::signers::k256::elliptic_curve::bigint;
use alloy::sol_types::SolValue;
use alloy::{pubsub::SubscriptionStream, sol_types::SolEvent};
use chrono::Utc;
use futures_util::stream::StreamExt;
use log::{debug, error, info};
use rust_decimal::Decimal;
use tokio_postgres::{connect, Client, NoTls};

use crate::solidity_structs::intent_lib_v2::IntentTypesLib;
use crate::solidity_structs::{
    intent_lib_v2::IntentLibV2,
    intent_processor::IntentProcessorV2::{self},
    intenterop_lib_v2::InteropLibV2,
    mocked_ln::MockLN,
    vault::Vault,
    IntentPayloadStakeData, SolidityAcknowledgementMetadata, SoliditySolution,
};
use crate::solidity_structs::{
    IntentProcessorBoundMessageAcknowledgementData, SolidityIntentProcessorBoundMessage,
    SolidityOrder, SolidityVaultBoundMessage, VaultBoundMessagePlaceOrderData,
};

enum IntentVersions {
    IntentSubmitted,
    SolutionSubmitted,
    OrderCreated,
    ReceivedMessageOnVault,
    MessageDispatchedFromVault,
    AcknowledgementReceived,
    Error,
}

#[derive(Debug)]
enum IntentStage {
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
    query: &str,
    intent_id: &i64,
    version: i32,
    stage: &str,
    transaction_hash: String,
    client: &Arc<Client>,
) {
    // let gas_fees = 1 as i64;
    let timestamp = std::time::SystemTime::now();
    let intent_state_response = client
        .execute(
            query,
            &[&intent_id, &version, &transaction_hash, &stage, &timestamp],
        )
        .await
        .unwrap();
    info!("Intent State Updated for intent id: {intent_id} to version: {version}");
}

// Function to listen to events from a specific RPC and contract
pub async fn process_evm_events(
    mut stream: SubscriptionStream<Log>,
    client: Arc<Client>,
    chain_id: i64,
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

                let intent_state_update_query =
                    "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, DEFAULT)";
                update_intent_state(
                    intent_state_update_query,
                    &intent_id,
                    IntentVersions::IntentSubmitted as i32,
                    &IntentStage::Initialized.to_string(),
                    transaction_hash,
                    &client,
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

                let intent_state_update_query =
                    "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, DEFAULT)";
                update_intent_state(
                    intent_state_update_query,
                    &intent_id,
                    IntentVersions::SolutionSubmitted as i32,
                    &IntentStage::Processing.to_string(),
                    transaction_hash,
                    &client,
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
                let order_id = orderId as i64;
                let creator_address = order_struct.initiatorAddress.to_string();
                let token_in = order_struct.tokenIn.to_string();
                let token_out = order_struct.tokenOut.to_string();
                let amount_in = order_struct.amountIn.to_string();
                let amount_out = order_struct.amountOut.to_string();
                let transaction_hash = log.transaction_hash.unwrap().to_string();
                let block_number = log.block_number.unwrap() as i64;

                let current_timestamp = std::time::SystemTime::now();
                let timestamp = current_timestamp;

                let query: &str =
                    "INSERT INTO order_created VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
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
                        ],
                    )
                    .await
                    .unwrap();
                info!("IntentLibV2::OrderCreated inserted response {:?}", response);

                let intent_state_update_query =
                    "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, DEFAULT)";
                update_intent_state(
                    intent_state_update_query,
                    &intent_id,
                    IntentVersions::OrderCreated as i32,
                    &IntentStage::Processing.to_string(),
                    transaction_hash,
                    &client,
                )
                .await;
            }
            Some(&InteropLibV2::AcknowledgementReceived::SIGNATURE_HASH) => {
                debug!("\nAcknowledgementReceived log - {:?}", log);
                let InteropLibV2::AcknowledgementReceived {
                    intentId,
                    sender,
                    result,
                    errorMessage,
                } = log.log_decode().unwrap().inner.data;
                info!("InteropLibV2::AcknowledgementReceived for {intentId} from {sender} with result {result}");

                let transaction_hash = log.transaction_hash.unwrap().to_string();
                let block_number = log.block_number.unwrap() as i64;
                let intent_id = intentId as i64;
                let sender_address = sender.to_string();
                let result = result;
                let error_message = errorMessage;
                let timestamp = std::time::SystemTime::now();

                let query =
                    "INSERT INTO acknowledgement VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7)";
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
                        ],
                    )
                    .await
                    .unwrap();
                info!("InteropLibV2::AcknowledgementReceived inserted response {:?}", response);

                let intent_state_update_query =
                    "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, DEFAULT)";
                update_intent_state(
                    intent_state_update_query,
                    &intent_id,
                    IntentVersions::AcknowledgementReceived as i32,
                    &IntentStage::Done.to_string(),
                    transaction_hash,
                    &client,
                )
                .await;
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

                let transaction_hash = log.transaction_hash.unwrap().to_string();
                let block_number = log.block_number.unwrap() as i64;
                let intent_id = decoded_message_data.order.intentId as i64;
                let origin_domain_id = origin as i32;
                let sender_address = decoded_message_data.order.initiatorAddress.to_string();
                let provider = provider as i32;
                let message = message.to_string();
                let timestamp = std::time::SystemTime::now();

                let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9)";
                let response = client
                    .execute(
                        query,
                        &[
                            &intent_id,
                            &origin_domain_id,
                            &sender_address,
                            &message,
                            &provider,
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                            &chain_id,
                        ],
                    )
                    .await
                    .unwrap();
                info!("Vault::ReceivedMessageOnVault inserted response {:?}", response);

                let intent_state_update_query =
                    "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, DEFAULT)";
                update_intent_state(
                    intent_state_update_query,
                    &intent_id,
                    IntentVersions::ReceivedMessageOnVault as i32,
                    &IntentStage::Processing.to_string(),
                    transaction_hash,
                    &client,
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
                    )
                    .unwrap();

                let intent_id = decoded_message_data.intentId as i64;
                let sender_address = log.address().to_string();
                let destination_domain_id = destinationDomain as i32;
                let provider = provider as i32;
                let message = message.to_string();
                let transaction_hash = log.transaction_hash.unwrap().to_string();
                let block_number = log.block_number.unwrap() as i64;
                let timestamp = std::time::SystemTime::now();

                let query =
                    "INSERT INTO message_dispatched_from_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
                let response = client
                    .execute(
                        query,
                        &[
                            &intent_id,
                            &sender_address,
                            &destination_domain_id,
                            &provider,
                            &message,
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                        ],
                    )
                    .await
                    .unwrap();
                info!("Vault::MessageDispatchedFromVault inserted response {:?}", response);

                let intent_state_update_query =
                    "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, DEFAULT)";
                update_intent_state(
                    intent_state_update_query,
                    &intent_id,
                    IntentVersions::MessageDispatchedFromVault as i32,
                    &IntentStage::Processing.to_string(),
                    transaction_hash,
                    &client,
                )
                .await;
            }
            _ => {
                info!("\ndidn't match any event, {:?}", log);
            }
        }
    }
}
