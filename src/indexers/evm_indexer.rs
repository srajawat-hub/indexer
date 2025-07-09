use std::sync::Arc;

use super::BlockchainIndexer;
use crate::{constants::BACKFILL_BATCH_SIZE, events::event_processor};
use alloy::{
    eips::BlockNumberOrTag, hex::FromHex, primitives::Address, providers::{Provider, ProviderBuilder, RootProvider, WsConnect}, rpc::types::Filter, transports::http::Http
};
use async_trait::async_trait;
use log::{error, info};
use tokio::sync::oneshot;
use tokio::time::{sleep, Duration};
use tokio_postgres::{row, Client};
use futures_util::stream;


pub struct EvmIndexer {
    http_rpc_url: String,
    vaults_contract_address: String,
    amm_contract_address: Option<String>,
    hook_executor_contract_address: Option<String>,
    solana_chain_id: u64,
}

impl EvmIndexer {
    pub fn new(
        http_rpc_url: String,
        vaults_contract_address: String,
        amm_contract_address: Option<String>,
        hook_executor_contract_address: Option<String>,
        solana_chain_id: u64,
    ) -> Self {
        Self {
            http_rpc_url,
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

    async fn load_last_recorded_block_number(
        &self,
        client: &Arc<Client>,
        chain_id: i64,
        http_provider: &RootProvider<Http<reqwest::Client>>
    ) -> Result<i64, Box<dyn std::error::Error>> {
        let query = "SELECT latest_block FROM chain_metadata WHERE chain_id = $1";
        let row = client.query_one(query, &[&chain_id.to_string()]).await;
        if let Ok(row) = row {
            let latest_block: i64 = row.get("latest_block");
            Ok(latest_block)
        } else {
            // If no record found, fetch from last recorded block number for vault
            let query = "SELECT * FROM received_message_on_vault WHERE chain_id = $1 ORDER BY id DESC LIMIT 1";                    
            let block_number: i64 = match client.query(query, &[&chain_id]).await {
                Ok(row) => {
                    if row.len() > 0 {
                        row[0].get("block_number")
                    } else {
                        http_provider.get_block_number().await.unwrap() as i64
                    }
                }
                Err(_e) => {
                    error!("Error in fetching latest block number from database for chain id {chain_id}, using latest block number from provider");
                    http_provider.get_block_number().await.unwrap() as i64
                }
            };
            Ok(block_number)
        }
    }

    async fn update_last_recorded_block_number(
        &self,
        client: &Arc<Client>,
        chain_id: i64,
        block_number: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            INSERT INTO chain_metadata (chain_id, latest_block)
            VALUES ($1, $2)
            ON CONFLICT (chain_id)
            DO UPDATE SET latest_block = EXCLUDED.latest_block
        "#;
        client.execute(query, &[&chain_id.to_string(), &block_number]).await?;
        Ok(())
    }
}

#[async_trait]
impl BlockchainIndexer for EvmIndexer {
    async fn listen_for_events(
        &self,
        client: Arc<Client>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let client_clone = Arc::clone(&client);

        let http_rpc_url = self.http_rpc_url.parse().unwrap();
        let provider: RootProvider<Http<reqwest::Client>> = ProviderBuilder::new().on_http(http_rpc_url);

        let latest_block_number = provider.get_block_number().await? as i64;
        info!("Latest block number: {}", latest_block_number);

        let chain_id = provider.get_chain_id().await? as i64;
        let mut create_new_pool_check_task = false;
        let l3_chain_id = std::env::var("L3_CHAIN_ID")
            .expect("L3_CHAIN_ID environment variable not set")
            .parse::<i64>()
            .unwrap();
        if chain_id != l3_chain_id {
            create_new_pool_check_task = true;
        }

        let mut start_block_number = self.load_last_recorded_block_number(&client_clone, chain_id, &provider).await?;

        let mut end_block_number = std::cmp::min(
            start_block_number + BACKFILL_BATCH_SIZE as i64 - 1, 
            latest_block_number
        );
        info!(
            "Starting event listener from block {} to block {}",
            start_block_number, end_block_number
        );

        // Load current pool addresses from database
        let pool_addresses = self.load_pool_addresses(&client, chain_id).await?;

        // Build the list of subscribed contracts
        let vaults_contract_addr =
            Address::from_hex(self.vaults_contract_address.clone()).unwrap();
        let mut contract_addrs = vec![vaults_contract_addr];

        let mut additional_contracts_num = 0;

        // Only add AMM contract address if it's provided
        if let Some(amm_address) = &self.amm_contract_address {
            let amm_contract_addr = Address::from_hex(amm_address.clone()).unwrap();
            contract_addrs.push(amm_contract_addr);
            additional_contracts_num += 1;
        }

        if let Some(hook_executor_address) = &self.hook_executor_contract_address {
            let hook_executor_contract_addr = Address::from_hex(hook_executor_address.clone()).unwrap();
            contract_addrs.push(hook_executor_contract_addr);
        }

        let mut initial_contracts_num = contract_addrs.len(); // contract count before adding pools
        contract_addrs.extend(pool_addresses);
        let mut current_pool_count = contract_addrs.len(); // count after adding the pools
        

        loop {
            sleep(Duration::from_secs(1)).await;
            if start_block_number == end_block_number {
                // If we reached the end block, wait for new blocks
                info!("Waiting for new blocks...");
                sleep(Duration::from_secs(3)).await;
                let latest_block_number = provider.get_block_number().await? as i64;
                end_block_number = std::cmp::min(
                    start_block_number + BACKFILL_BATCH_SIZE as i64 - 1, 
                    latest_block_number
                );
                info!(
                    "Resuming event listener for chain_id {} from block {} to block {}",
                    chain_id, start_block_number, end_block_number
                );
                continue;
            }

            info!(
                "Subscribing to {} contracts total for chain_id {}",
                contract_addrs.len(), chain_id
            );

            let filter = Filter::new()
                .address(contract_addrs.clone())
                .from_block(BlockNumberOrTag::Number(start_block_number as u64))
                .to_block(BlockNumberOrTag::Number(end_block_number as u64));

            let logs = provider
            .get_logs(&filter)
            .await
            .unwrap_or_else(|e| {
                error!("Failed to fetch historical logs: {e}");
                vec![]
            });
            let logs_stream = stream::iter(logs.clone());

            let task_id = tokio::task::id();
            info!("Starting event processor {task_id}");

            let solana_chain_id = self.solana_chain_id.to_string();
            event_processor::process_evm_events(
                logs_stream,
                client.clone(),
                chain_id,
                &provider,
                &solana_chain_id,
            ).await;
            info!("Event processing completed for chain-id {} for block range {}-{}", chain_id, start_block_number, end_block_number);

            // Update the last recorded block number in the database
            self.update_last_recorded_block_number(
                &client_clone,
                chain_id,
                end_block_number,
            )
            .await?;
            info!(
                "Updated last recorded block number to {} for chain_id {}",
                end_block_number, chain_id
            );

            // check for new pools
            if create_new_pool_check_task {
                if !logs.is_empty() {
                    let new_pools_query = "SELECT COUNT(*) as pool_count FROM pools WHERE chain_id = $1 AND pool_type = 'EVM'";
                    match client.query_one(new_pools_query, &[&chain_id]).await {
                        Ok(row) => {
                            let new_pool_count: i64 = row.get("pool_count");
                            if new_pool_count as usize + initial_contracts_num != current_pool_count
                            {
                                info!(
                                    "Detected new pools! Current count: {}, Previous count: {}",
                                    new_pool_count,
                                    current_pool_count - initial_contracts_num
                                );
                                let pool_addresses = self.load_pool_addresses(&client, chain_id)
                                .await
                                .unwrap_or_else(|e| {
                                    error!("Failed to load pool addresses: {e}");
                                    Vec::new()
                                });
    
                                contract_addrs.clear();
                                contract_addrs = vec![
                                    Address::from_hex(self.vaults_contract_address.clone()).unwrap(),
                                ];
                                if let Some(amm_contract) = &self.amm_contract_address {
                                    contract_addrs.push(Address::from_hex(amm_contract).unwrap());
                                }
                                initial_contracts_num = contract_addrs.len();
                                contract_addrs.extend(pool_addresses);
                                info!("Updated contract addresses to monitor: {:?}", contract_addrs);
                                current_pool_count = contract_addrs.len(); // update the count after adding new pools
                            } else {
                                // continue with the next batch
                                start_block_number = end_block_number + 1;
                                end_block_number = std::cmp::min(
                                    start_block_number + BACKFILL_BATCH_SIZE as i64 - 1,
                                    latest_block_number,
                                );
                            }
                        }
                        Err(e) => {
                            error!("Failed to check for new pools for chain id {}: {:?}", chain_id, e);
                            // continue with the next batch
                            start_block_number = end_block_number + 1;
                            end_block_number = std::cmp::min(
                                start_block_number + BACKFILL_BATCH_SIZE as i64 - 1,
                                latest_block_number,
                            );
                        }
                    }
                } else {
                    // No logs found, continue to the next batch
                    info!(
                        "No logs found chain-id {} for block range {} to {}, continuing to next batch",
                        chain_id, start_block_number, end_block_number
                    );
                    start_block_number = end_block_number + 1;
                    end_block_number = std::cmp::min(
                        start_block_number + BACKFILL_BATCH_SIZE as i64 - 1,
                        latest_block_number,
                    );
                }
            } else {
                // No new pools to check, continue with the next batch
                info!(
                    "No new pools to check for chain-id {} for block range {} to {}, continuing to next batch",
                    chain_id, start_block_number, end_block_number
                );
                start_block_number = end_block_number + 1;
                end_block_number = std::cmp::min(
                    start_block_number + BACKFILL_BATCH_SIZE as i64 - 1,
                    latest_block_number,
                );
            }
        }
    }
}
