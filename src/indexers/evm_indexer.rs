use super::BlockchainIndexer;
use alloy::{
    hex::FromHex,
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::{BlockNumberOrTag, Filter},
};
use async_trait::async_trait;
use log::info;
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
    async fn listen_for_events(&self) -> Result<(), Box<dyn std::error::Error>> {
        let ws = WsConnect::new(self.rpc_url.clone());
        let provider = ProviderBuilder::new().on_ws(ws).await.unwrap();
        let contract_addr = Address::from_hex(self.contract_address.clone()).unwrap();
        let filter = Filter::new()
            .address(contract_addr) // this can take a vec. we can group addresses of the same chain in the filter
            .from_block(BlockNumberOrTag::Latest);

        let sub = provider.subscribe_logs(&filter).await.unwrap();
        let stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log> =
            sub.into_stream();

        let task_id = tokio::task::id();
        info!("Starting {task_id}");
        event_processor::process_evm_events(stream).await;
        Ok(())
    }
}
