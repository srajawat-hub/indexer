use std::{str::FromStr, sync::Arc, time::SystemTime};
use anyhow::bail;
use log::{error, info, warn};
use alloy::{providers::{Provider, RootProvider}, pubsub::PubSubFrontend, rpc::types::Log};
use rust_decimal::Decimal;
use tokio_postgres::Client;

use crate::{events::event_processor::{check_vault_initiated_transaction, get_liquidity_provider_address_evm, insert_liquidity_event}, solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib::{self}, utils::unix_to_system_time};

pub async fn handle_uniswap_burn_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<PubSubFrontend>
) -> anyhow::Result<()> {
    let UniswapV3PoolLib::Burn {
        owner,
        tickLower,
        tickUpper,
        amount,
        amount0,
        amount1,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "Burn";
    info!(target: log_target, "UniswapV3PoolLib::Burn by owner {owner}");

    let pool_address = log.address().to_string();
    let periphery_contract_address = owner.to_string();
    let raw_transaction_hash = log.transaction_hash.unwrap();
    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap() as i64;
    let timestamp = match log.block_timestamp {
        Some(ts) => unix_to_system_time(ts),
        None => {
            error!(target: log_target, "Block timestamp is missing for log: {:?}", log);
            SystemTime::now() // Fallback to current time if timestamp is missing
        }
    };

    let transaction_receipt = chain_provider.get_transaction_receipt(raw_transaction_hash).await;
    let user_address: String;
    let liquidity_decoded_user_data = get_liquidity_provider_address_evm(transaction_receipt, &client, periphery_contract_address).await;

    if let Some(user_addr) = liquidity_decoded_user_data.liquidity_user_address {
        user_address = user_addr;
    } else {
        warn!(target: log_target, "Failed to get user address for transaction: {:?}. Reverting to fallback method", raw_transaction_hash);
        user_address = String::new();
    }

    let token_id = liquidity_decoded_user_data.liquidity_token_id;


    let amount_token0 = Decimal::from_str(&amount0.to_string()).unwrap();
    let amount_token1 = Decimal::from_str(&amount1.to_string()).unwrap();
    let liquidity_amount = amount as i64;

    let position_id = format!("{}:{}:{}", owner, tickLower, tickUpper);

    let manager_query = "SELECT project_manager FROM pools WHERE pool_address = $1";
    let manager_row = client.query_one(manager_query, &[&pool_address]).await;
    let is_manager = match manager_row {
        Ok(row) => {
            let pm_address: String = row.get("project_manager");
            pm_address.to_lowercase() == user_address.to_lowercase()
        },
        Err(e) => {
            error!(target: log_target, "Failed to fetch project manager for pool {}: {:?}", pool_address, e);
            bail!("Failed to fetch project manager for pool {}: {:?}", pool_address, e);
            false
        }
    };

    let is_vault_initiated = check_vault_initiated_transaction(chain_provider.clone(), &client, log.transaction_hash.unwrap().clone()).await;

    insert_liquidity_event(
        &client,
        pool_address,
        user_address,
        false, // is_add = false for burn
        is_manager,
        Some(position_id),
        amount_token0,
        amount_token1,
        liquidity_amount,
        None, // fee_amount_0
        None, // fee_amount_1
        transaction_hash,
        block_number,
        timestamp,
        chain_id,
        Some(is_vault_initiated), // is_vault = false
        log_target,
        token_id
    )
    .await?;

    Ok(())
}