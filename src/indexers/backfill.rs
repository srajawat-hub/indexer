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

pub async fn fill_history_intents(indexer_configs: Arc<Config>, client: Arc<Client>) {
    let l3_chain_id = std::env::var("L3_CHAIN_ID")
        .expect("L3_CHAIN_ID environment variable not set")
        .parse::<i64>()
        .unwrap();

    for indexer_config in &indexer_configs.indexers {
        let http_rpc_url = indexer_config.url.parse().unwrap();
        let http_provider = ProviderBuilder::new().on_http(http_rpc_url);

        let ws = WsConnect::new(indexer_config.ws_url.clone());
        let ws_provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend> =
            ProviderBuilder::new().on_ws(ws).await.unwrap();

        let chain_id = http_provider.get_chain_id().await.unwrap() as i64;

        let last_recorded_block_number = if chain_id == l3_chain_id {
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
            block_number as u64
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
                }
                Err(_e) => {
                    error!("Error in fetching latest block number from database for chain id {chain_id}, using latest block number from provider");
                    http_provider.get_block_number().await.unwrap() as i64
                }
            };
            block_number as u64
        };
        info!("Last recorded block number: {last_recorded_block_number}");

        let contract_addr = Address::from_hex(indexer_config.contract.clone()).unwrap();
        let latest_block_number = http_provider.get_block_number().await.unwrap();

        let mut start_block_number = last_recorded_block_number;
        let mut end_block_number = std::cmp::min(
            start_block_number + BACKFILL_BATCH_SIZE - 1,
            latest_block_number,
        );

        while start_block_number <= latest_block_number {
            let http_filter = Filter::new()
                .address(contract_addr) // this can take a vec. we can group addresses of the same chain in the filter
                .from_block(BlockNumberOrTag::Number(start_block_number))
                .to_block(BlockNumberOrTag::Number(end_block_number));

            let history_logs = http_provider
                .get_logs(&http_filter)
                .await
                .unwrap_or_else(|e| {
                    error!("Failed to fetch historical logs: {e}");
                    vec![]
                });
            let backfill_stream = stream::iter(history_logs);
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
            start_block_number = end_block_number + 1;
            end_block_number = std::cmp::min(
                start_block_number + BACKFILL_BATCH_SIZE - 1,
                latest_block_number,
            );
        }
    }
}
