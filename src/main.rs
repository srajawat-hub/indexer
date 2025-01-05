// src/main.rs

use alloy::{
    hex::FromHex,
    primitives::{address, Address},
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter},
    sol,
    sol_types::SolEvent,
};
use futures_util::stream::StreamExt;
use log::info;
use std::{
    fmt::Error,
    sync::{Arc, Mutex},
};
use std::{str::FromStr, thread};
use tokio::{runtime::Runtime, task::futures};
pub mod solidity_structs;

use solidity_structs::token::Token;
use solidity_structs::{intent_lib_v2::IntentLibV2, vault::IntentLib, intenterop_lib_v2::InteropLibV2};
use solidity_structs::{
    intent_processor::{
        IntentProcessorV2::{self},
        IntentTypesLib,
    },
    mocked_ln::MockLN,
};
use solidity_structs::{mocked_ln::IMockLN, IntentPayloadStakeData};
use solidity_structs::{vault::Vault, SolidityAcknowledgementMetadata};
use solidity_structs::{
    IntentPayloadEnum, IntentPayloadSwapData, IntentPayloadTransferData,
    IntentProcessorBoundMessageAcknowledgementData, IntentProcessorBoundMessageEnum,
    LiquidityNetworkEnum, LiquidityNetworkMockedLNData, SolidityLiquidityNetwork, SoliditySolution,
    SoliditySolutionEnum, SoliditySolutionSwapSolutionData, SoliditySolutionTransferSolutionData,
    SoliditySolutionType, SolutionTypeCrossChainData, SolutionTypeEnum, SolverSolution,
};
use tokio::signal;

#[tokio::main]
async fn main() {

    env_logger::builder().format(|buf, record| {
        use std::io::Write;
        writeln!(
            buf,
            "[{} - Thread: {:?}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            std::thread::current().id(),
            record.args()
        )
    }).init();

    let rpc_urls = vec![
        "ws://192.241.245.190:18749", // L3
        "wss://arb-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID", // arb
        "wss://opt-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID", // op
        "wss://arb-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID", // arb
        "wss://opt-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID"]; // op
    let contract_addresses = vec![
        "0xFAB814c2A68F54971A12Cf6990Ea3Df2EF14c3FB", // ip
        "0x22c423540918032B206Df38d86AFCB9B22eF1c0f", // arb vault
        "0x42Ad426D1C9dA42648535DEE83D9fc73bAd9f274", //op vault
        "0x49E8FcC52698e78786ea1d929e1b3f1A7945Bccb", // arb mockln
        "0xB5F67202064848c1528AbdC9e9e49a776E08ecC3" // op mockln
    ];

    let mut handles = vec![];
    let mut thread_count=0;
    for (rpc_url, contract_address) in rpc_urls.iter().zip(contract_addresses.iter()) {
        // info!("Listening on {contract_address}");
        let rpc_url = rpc_url.to_string(); // Clone into an owned String
        let contract_address = contract_address.to_string(); // Clone into an owned String

        let handle = tokio::spawn(async move {
            let ws = WsConnect::new(rpc_url);
            let provider = ProviderBuilder::new().on_ws(ws).await.unwrap();
            let contract_addr = Address::from_hex(contract_address).unwrap();
            let filter = Filter::new()
                .address(contract_addr)
                .from_block(BlockNumberOrTag::Latest);

            let sub = provider.subscribe_logs(&filter).await.unwrap();
            let mut stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log> =
                sub.into_stream();

            let task_id = tokio::task::id();
            info!("Starting {task_id}");
            listen_to_events(stream).await;
        });

        handles.push(handle);
    }

    println!("All tasks started. Press Ctrl+C to exit.");
    println!("total threads {:?}", handles.len());

    // Wait for Ctrl+C signal to exit
    signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}

// Function to listen to events from a specific RPC and contract
async fn listen_to_events(mut stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log>) {
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
