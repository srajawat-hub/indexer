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
use std::{
    fmt::Error,
    sync::{Arc, Mutex},
};
use std::{str::FromStr, thread};
use tokio::{runtime::Runtime, task::futures};
pub mod solidity_structs;

use solidity_structs::intent_lib_v2::IntentLibV2;
use solidity_structs::token::Token;
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
    let rpc_urls = vec!["ws://127.0.0.1:8545", "ws://127.0.0.1:8545"];
    let contract_addresses = vec![
        "0x750b8C791080d89e2E9d0620C4CB4982CAEf9217",
        "FAB814c2A68F54971A12Cf6990Ea3Df2EF14c3FB"
    ];

    let mut handles = vec![];
    for (rpc_url, contract_address) in rpc_urls.iter().zip(contract_addresses.iter()) {
        // listen_to_events(stream).await;
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
            listen_to_events(stream).await;
        });

        handles.push(handle);
    }

    println!("All tasks started. Press Ctrl+C to exit.");

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
                let transaction_hash = log.transaction_hash.unwrap();
                let block_timestamp = log.block_timestamp.unwrap();
                let block_number = log.block_number.unwrap();
                println!("Intent submitted from {owner} with intentId {intentId}");
            }
            Some(&IntentLibV2::SolutionSubmitted::SIGNATURE_HASH) => {
                let IntentLibV2::SolutionSubmitted { intentId, solver } =
                    log.log_decode().unwrap().inner.data;

                println!("Intent Solution submitted from {solver} for intentId {intentId}");
            }
            _ => {
                println!("didn't match anything, {:?}", log);
            }
        }
    }
}
