use alloy::{pubsub::SubscriptionStream, sol_types::SolEvent};
use alloy::rpc::types::Log;
use futures_util::stream::StreamExt;

use crate::solidity_structs::{
    intent_lib_v2::IntentLibV2, vault::IntentLib, intenterop_lib_v2::InteropLibV2,
    intent_processor::{IntentProcessorV2::{self}, IntentTypesLib},
    mocked_ln::{MockLN, IMockLN},
    IntentPayloadStakeData,
    vault::Vault, SolidityAcknowledgementMetadata
};

// Function to listen to events from a specific RPC and contract
pub async fn listen_to_events(mut stream: SubscriptionStream<Log>) {
    while let Some(log) = stream.next().await {
        // Match on topic 0, the hash of the signature of the event.
        match log.topic0() {
            Some(&IntentLibV2::IntentSubmitted::SIGNATURE_HASH) => {
                let IntentLibV2::IntentSubmitted { intentId, owner } =
                    log.log_decode().unwrap().inner.data;
                println!("\nIntentSubmitted log - {:?}", log);
                println!("Intent submitted from {owner} with intentId {intentId}");
                let transaction_hash = log.transaction_hash.unwrap();
                // let block_timestamp = log.block_timestamp.unwrap();
                let block_number = log.block_number.unwrap();
            }
            Some(&IntentLibV2::SolutionSubmitted::SIGNATURE_HASH) => {
                println!("\nSolutionSubmitted log - {:?}", log);
                let IntentLibV2::SolutionSubmitted { intentId, solver } =
                    log.log_decode().unwrap().inner.data;

                println!("Intent Solution submitted from {solver} for intentId {intentId}");
            }
            Some(&InteropLibV2::AcknowledgementReceived::SIGNATURE_HASH) => {
                println!("\nAcknowledgementReceived log - {:?}", log);
                let InteropLibV2::AcknowledgementReceived {
                    intentId,
                    sender,
                    result,
                    errorMessage
                } = log.log_decode().unwrap().inner.data;
                println!("AcknowledgementReceived for {intentId} from {sender} with result {result}");
            }
            Some(&Vault::ReceivedMessageOnVault::SIGNATURE_HASH) => {
                let Vault::ReceivedMessageOnVault {
                    origin,
                    sender,
                    message,
                    provider,
                } = log.log_decode().unwrap().inner.data;
                println!("\nReceivedMessageOnVault log - {:?}", log);
                // have to get intent_id here. process the message
                println!("Received Message on Vault from id {origin} by {sender}, message = {message}, using provider = {provider}");
            }
            Some(&MockLN::OrderCreated::SIGNATURE_HASH) => {
                let MockLN::OrderCreated {
                    orderId,
                    creator,
                    tokenIn,
                    tokenOut,
                    amountIn,
                    amountOut
                } = log.log_decode().unwrap().inner.data;
                println!("\nOrderCreated log - {:?}", log);
                // orderId is the intentId
                println!("Order created on MockLN");
            }
            Some(&Vault::MessageDispatchedFromVault::SIGNATURE_HASH) => {
                let Vault::MessageDispatchedFromVault {
                    sender,
                    destinationDomain,
                    provider,
                    message
                } = log.log_decode().unwrap().inner.data;
                println!("\nMessageDispatchedFromVault log - {:?}", log);
                // have to get intentId here.
                println!("Message dispatched from vault");
            }
            _ => {
                println!("\ndidn't match anything, {:?}", log);
            }
        }
    }
}
