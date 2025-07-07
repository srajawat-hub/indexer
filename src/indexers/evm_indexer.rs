use std::sync::Arc;

use super::BlockchainIndexer;
use crate::events::event_processor;
use alloy::{
    hex::FromHex,
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};
use async_trait::async_trait;
use log::{error, info};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tokio_postgres::Client;

const NEW_POOLS_FETCH_INTERVAL: Duration = Duration::from_secs(5);

pub struct EvmIndexer {
    http_rpc_url: String,
    ws_rpc_url: String,
    vaults_contract_address: String,
    amm_contract_address: Option<String>,
    hook_executor_contract_address: Option<String>,
    solana_chain_id: u64,
}

impl EvmIndexer {
    pub fn new(
        http_rpc_url: String,
        ws_rpc_url: String,
        vaults_contract_address: String,
        amm_contract_address: Option<String>,
        hook_executor_contract_address: Option<String>,
        solana_chain_id: u64,
    ) -> Self {
        Self {
            http_rpc_url,
            ws_rpc_url,
            vaults_contract_address,
            amm_contract_address,
            hook_executor_contract_address,
            solana_chain_id,
        }
    }

    async fn load_pool_addresses(
        &self,
        client: &Arc<Client>,
        chain_id: i64,
    ) -> Result<Vec<Address>, Box<dyn std::error::Error>> {
        let query = "SELECT pool_address FROM pools WHERE chain_id = $1 AND pool_type = 'EVM'";
        let rows = client.query(query, &[&chain_id]).await?;

        let mut pool_addresses = Vec::new();
        for row in rows {
            let pool_address: String = row.get("pool_address");
            if let Ok(address) = Address::from_hex(pool_address) {
                pool_addresses.push(address);
            }
        }

        info!(
            "Loaded {} pool addresses from database for chain_id {}",
            pool_addresses.len(),
            chain_id
        );
        Ok(pool_addresses)
    }
}

#[async_trait]
impl BlockchainIndexer for EvmIndexer {
    async fn listen_for_events(
        &self,
        client: Arc<Client>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        // Clone necessary data for the monitoring task
        let client_clone = Arc::clone(&client);

        loop {
            // Create a channel for signaling subscription updates
            let (tx, rx) = oneshot::channel::<()>();

            let ws = WsConnect::new(self.ws_rpc_url.clone());
            let provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend> =
                ProviderBuilder::new().on_ws(ws).await?;

            let chain_id = provider.get_chain_id().await? as i64;

            // Load current pool addresses from database
            let pool_addresses = self.load_pool_addresses(&client, chain_id).await?;

            // Build the list of subscribed contracts
            let vaults_contract_addr =
                Address::from_hex(self.vaults_contract_address.clone()).unwrap();
            let mut subscribed_contracts = vec![vaults_contract_addr];


            let mut additional_contracts_num = 0;

            // Only add AMM contract address if it's provided
            if let Some(amm_address) = &self.amm_contract_address {
                let amm_contract_addr = Address::from_hex(amm_address.clone()).unwrap();
                subscribed_contracts.push(amm_contract_addr);
                additional_contracts_num += 1;
            }

            if let Some(hook_executor_address) = &self.hook_executor_contract_address {
                let hook_executor_contract_addr = Address::from_hex(hook_executor_address.clone()).unwrap();
                subscribed_contracts.push(hook_executor_contract_addr);
            }

            let initial_contracts_num = subscribed_contracts.len();
            subscribed_contracts.extend(pool_addresses);

            info!(
                "Subscribing to {} contracts total",
                subscribed_contracts.len()
            );

            let filter = Filter::new().address(subscribed_contracts.clone());

            let sub = match provider.subscribe_logs(&filter).await {
                Ok(sub) => sub,
                Err(e) => {
                    error!("Failed to subscribe to logs: {:?}", e);
                    sleep(Duration::from_secs(10)).await;
                    continue;
                }
            };

            let stream: alloy::pubsub::SubscriptionStream<alloy::rpc::types::Log> =
                sub.into_stream();

            let task_id = tokio::task::id();
            info!("Starting event processor {task_id}");

            // Start monitoring for new pools in a separate task
            let client_monitor = Arc::clone(&client);
            let current_pool_count = subscribed_contracts.len();
            let monitor_chain_id = chain_id;

            tokio::spawn(async move {
                let mut check_interval = tokio::time::interval(NEW_POOLS_FETCH_INTERVAL);

                loop {
                    check_interval.tick().await;

                    // Query for new pools
                    let query = "SELECT COUNT(*) as pool_count FROM pools WHERE chain_id = $1 AND pool_type = 'EVM'";
                    match client_monitor.query_one(query, &[&monitor_chain_id]).await {
                        Ok(row) => {
                            let new_pool_count: i64 = row.get("pool_count");
                            if new_pool_count as usize + initial_contracts_num != current_pool_count
                            {
                                info!(
                                    "Detected new pools! Current count: {}, Previous count: {}",
                                    new_pool_count,
                                    current_pool_count - initial_contracts_num
                                );
                                // Signal main loop to restart subscription
                                if let Err(e) = tx.send(()) {
                                    error!("Failed to send restart signal: {:?}", e);
                                }
                                break;
                            }
                        }
                        Err(e) => {
                            error!("Failed to check for new pools: {:?}", e);
                        }
                    }
                }
            });

            let solana_chain_id = self.solana_chain_id.to_string();
            // Process events with timeout to allow for restart
            tokio::select! {
                _ = event_processor::process_evm_events(
                    stream,
                    client.clone(),
                    chain_id,
                    provider,
                    &solana_chain_id,
                ) => {
                    info!("Event processing completed");
                }
                _ = rx => {
                    info!("Received restart signal due to new pools, restarting subscription...");
                    continue;
                }
            }
        }
    }
}
