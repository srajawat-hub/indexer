use std::sync::Arc;

use super::BlockchainIndexer;
use alloy::{
    hex::FromHex,
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter},
};
use async_trait::async_trait;
use log::info;
use tokio_postgres::Client;
use crate::events::event_processor;

pub struct EvmIndexer {
    rpc_url: String,
    contract_address: String,
}

impl EvmIndexer {
    pub fn new(rpc_url: String, contract_address: String) -> Self {
        Self {
            rpc_url,
            contract_address,
        }
    }
}

#[async_trait]
impl BlockchainIndexer for EvmIndexer {
    async fn listen_for_events(&self, client: Arc<Client>) -> Result<(), Box<dyn std::error::Error>> {
        // fetch the latest block number from the db and start from there.
        let ws = WsConnect::new(self.rpc_url.clone());
        let provider = ProviderBuilder::new().on_ws(ws).await.unwrap();

        let chainId = provider.get_chain_id().await.unwrap() as i64;
        info!("chain id {:?}", chainId);

        let latest_block_number = match chainId {
            71461164656 => {
                // ip
                let query = "SELECT * FROM intent ORDER BY id DESC LIMIT 1";
                let response = client.query(query, &[]).await.unwrap();
                let block_number: i64 = response[0].get("block_number");
                info!("Response {:?}", block_number);
                block_number as u64
            }
            _ => { // vaults
                let query = "SELECT * FROM received_message_on_vault ORDER BY id DESC WHERE chain_id = $1 LIMIT 1";
                let block_number: i64 = match client.query(query, &[&chainId]).await {
                    Ok(row) => row[0].get("block_number"),
                    Err(e) => {
                        provider.get_block_number().await.unwrap() as i64
                    }
                };
                block_number as u64
            }
        };

        let contract_addr = Address::from_hex(self.contract_address.clone()).unwrap();
        let filter = Filter::new()
            .address(contract_addr) // this can take a vec. we can group addresses of the same chain in the filter
            .from_block(BlockNumberOrTag::Number(latest_block_number)); // make this from the last known block number

        let sub = provider.subscribe_logs(&filter).await.unwrap();
        let stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log> =
            sub.into_stream();

        let task_id = tokio::task::id();
        info!("Starting {task_id}");
        event_processor::process_evm_events(stream, client, chainId).await;
        Ok(())
    }
}
