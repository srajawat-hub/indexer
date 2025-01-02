// src/main.rs

use alloy::{
    primitives::address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter},
    sol,
    sol_types::SolEvent,
};
use futures_util::stream::StreamExt;
use std::thread;
use std::{
    fmt::Error,
    sync::{Arc, Mutex},
};
use tokio::runtime::Runtime;
pub mod solidity_structs;

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

#[tokio::main]
async fn main() -> Result<(), Error> {
    let rpc_urls = vec!["ws://127.0.0.1:8545"];
    let contract_addresses = vec!["0xFAB814c2A68F54971A12Cf6990Ea3Df2EF14c3FB"];

    // let mut handles = vec![];
    for (rpc_url, contract_address) in rpc_urls.iter().zip(contract_addresses.iter()) {
        let ws = WsConnect::new(*rpc_url);
        let provider = ProviderBuilder::new().on_ws(ws).await.unwrap();
        let contract_addr = address!("FAB814c2A68F54971A12Cf6990Ea3Df2EF14c3FB");
        let filter = Filter::new()
            .address(contract_addr)
            .from_block(BlockNumberOrTag::Latest);

        let sub = provider.subscribe_logs(&filter).await.unwrap();
        let mut stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log> =
            sub.into_stream();

        listen_to_events(stream).await;

        // let handle = tokio::spawn(async move {
        //     println!("New process thread");
        //     listen_to_events(stream).await;
        // });

        // handles.push(handle);
    }

    // // // Wait for all threads to finish
    // for handle in handles {
    //     tokio::join!(handle);
    // }
    Ok(())
}

// Function to listen to events from a specific RPC and contract
async fn listen_to_events(mut stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log>) {
    while let Some(log) = stream.next().await {
        // Match on topic 0, the hash of the signature of the event.
        // testing ws connection with token contract
        match log.topic0() {
            Some(&Token::Approval::SIGNATURE_HASH) => {
                let Token::Approval {
                    owner,
                    spender,
                    value,
                } = log.log_decode().unwrap().inner.data;
                println!("Approval from {owner} to {spender} of value {value}");
            }
            Some(&Token::Transfer::SIGNATURE_HASH) => {
                let Token::Transfer { from, to, value } = log.log_decode().unwrap().inner.data;
                println!("Transfer from {from} to {to} of value {value}");
            }
            _ => (),
        }
    }
}
