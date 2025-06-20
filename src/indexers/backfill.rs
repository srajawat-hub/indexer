use std::sync::Arc;

use alloy::{
    eips::BlockNumberOrTag,
    hex::FromHex,
    primitives::Address,
    providers::{Provider, ProviderBuilder, WsConnect},
    rpc::types::Filter,
};
use futures_util::stream;
use log::{error, info};
use tokio_postgres::Client;

use crate::{
    constants::BACKFILL_BATCH_SIZE, events::event_processor::process_evm_events, Config,
};

async fn load_pool_addresses(
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

pub async fn fill_history_intents(indexer_configs: Arc<Config>, client: Arc<Client>) {
    let l3_chain_id = std::env::var("L3_CHAIN_ID")
        .expect("L3_CHAIN_ID environment variable not set")
        .parse::<i64>()
        .unwrap();

    for indexer_config in &indexer_configs.indexers {
        let http_rpc_url = indexer_config.url.parse().unwrap();
        let http_provider = ProviderBuilder::new().on_http(http_rpc_url);
        info!("ws url {:?}", indexer_config.ws_url.clone());
        let ws = WsConnect::new(indexer_config.ws_url.clone());
        let ws_provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend> = match ProviderBuilder::new().on_ws(ws).await {
            Ok(provider) => provider,
            Err(e) => {
                error!("Failed to connect to WebSocket provider: {e}");
                continue; // Skip this indexer if WebSocket connection fails
            }
        };

        let chain_id = http_provider.get_chain_id().await.unwrap() as i64;

        let (last_recorded_block_number, additional_addresses) = if chain_id == l3_chain_id {
            // ip
            let query = "SELECT * FROM intent ORDER BY id DESC LIMIT 1";
            let block_number = match client.query(query, &[]).await {
                Ok(row) => {
                    if row.len() > 0 {
                        row[0].get("block_number")
                    } else {
                        http_provider.get_block_number().await.unwrap() as i64
                    }
                }
                Err(_e) => http_provider.get_block_number().await.unwrap() as i64,
            };
            (block_number as u64 + 1, Vec::<Address>::new()) // +1 to start from the next block
        } else {
            // vaults
            let query = "SELECT * FROM received_message_on_vault WHERE chain_id = $1 ORDER BY id DESC LIMIT 1";
            let block_number: i64 = match client.query(query, &[&chain_id]).await {
                Ok(row) => {
                    if row.len() > 0 {
                        row[0].get("block_number")
                    } else {
                        error!("Failed to fetch block number from the database, using latest block number from provider");
                        http_provider.get_block_number().await.unwrap() as i64
                    }
                    // 31638610 as i64 // TODO: remove this hardcoded value
                }
                Err(_e) => {
                    error!("Error in fetching latest block number from database for chain id {chain_id}, using latest block number from provider");
                    http_provider.get_block_number().await.unwrap() as i64
                    // 31638610 as i64 // TODO: remove this hardcoded value
                }
            };

            // getting pool addresses
            let additional_address = load_pool_addresses(&client, chain_id)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to load pool addresses: {e}");
                    Vec::new()
                });

            (block_number as u64 + 1, additional_address) // +1 to start from the next block
        };
        info!("Starting backfill from block number: {last_recorded_block_number}");

        let mut contract_addrs = vec![Address::from_hex(indexer_config.contract.clone()).unwrap()];
        if let Some(amm_contract) = indexer_config.amm_contract.clone() {
            contract_addrs.push(Address::from_hex(amm_contract).unwrap());
        }
        let mut initial_contracts_num = contract_addrs.len();

        contract_addrs.extend(additional_addresses);
        info!("Contract addresses to monitor {:?}", contract_addrs);
        let latest_block_number = http_provider.get_block_number().await.unwrap();
        
        let mut current_pool_count = contract_addrs.len(); // count after adding the pools

        let mut start_block_number = last_recorded_block_number;
        let mut end_block_number = std::cmp::min(
            start_block_number + BACKFILL_BATCH_SIZE - 1,
            latest_block_number,
        );


        // let ws = WsConnect::new(indexer_config.ws_url.clone());
        // let ws_provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend> =
        //     ProviderBuilder::new().on_ws(ws).await.unwrap();

        while start_block_number <= latest_block_number {
            info!(target: "backfill", "Processing block range {} to {}", start_block_number, end_block_number);
            info!(target: "backfill", "Current contract addresses: {:?}", contract_addrs);
            let http_filter = Filter::new()
                .address(contract_addrs.clone()) // this can take a vec. we can group addresses of the same chain in the filter
                .from_block(BlockNumberOrTag::Number(start_block_number))
                .to_block(BlockNumberOrTag::Number(end_block_number));

            let history_logs = http_provider
                .get_logs(&http_filter)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to fetch historical logs: {e}");
                    vec![]
                });
            let backfill_stream = stream::iter(history_logs.clone());
            process_evm_events(
                backfill_stream,
                client.clone(),
                chain_id,
                ws_provider.clone(),
                &indexer_configs.solana_chain_id.to_string(),
            )
            .await;
            info!(
                "Processed logs from block {} to {}",
                start_block_number, end_block_number
            );

            // check for new pools
            if !history_logs.is_empty() {
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
                            let pool_addresses = load_pool_addresses(&client, chain_id)
                            .await
                            .unwrap_or_else(|e| {
                                error!("Failed to load pool addresses: {e}");
                                Vec::new()
                            });

                            contract_addrs.clear();
                            contract_addrs = vec![Address::from_hex(indexer_config.contract.clone()).unwrap()];
                            if let Some(amm_contract) = indexer_config.amm_contract.clone() {
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
                                start_block_number + BACKFILL_BATCH_SIZE - 1,
                                latest_block_number,
                            );
                        }
                    }
                    Err(e) => {
                        error!("Failed to check for new pools: {:?}", e);
                        // continue with the next batch
                        start_block_number = end_block_number + 1;
                        end_block_number = std::cmp::min(
                            start_block_number + BACKFILL_BATCH_SIZE - 1,
                            latest_block_number,
                        );
                    }
                }
            } else {
                // No logs found, continue to the next batch
                info!(
                    "No logs found for block range {} to {}, continuing to next batch",
                    start_block_number, end_block_number
                );
                start_block_number = end_block_number + 1;
                end_block_number = std::cmp::min(
                    start_block_number + BACKFILL_BATCH_SIZE - 1,
                    latest_block_number,
                );
            }
        }
    }
}
