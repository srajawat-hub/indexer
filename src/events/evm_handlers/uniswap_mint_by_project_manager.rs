use alloy::{
    providers::{Provider, RootProvider},
    rpc::types::{Log, TransactionReceipt},
    transports::http::Http,
};
use log::{error, info, warn};
use rust_decimal::Decimal;
use std::{str::FromStr, sync::Arc, time::SystemTime};
use tokio_postgres::Client;

use crate::{
    events::event_processor::{
        fallback_fetch_pm_address, get_liquidity_provider_address_evm, insert_liquidity_event,
    },
    solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib::{self},
    utils::unix_to_system_time,
};

pub async fn handle_uniswap_mint_by_pm_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let UniswapV3PoolLib::MintByProjectManager {
        sender,
        owner,
        tickLower,
        tickUpper,
        amount,
        amount0,
        amount1,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "MintByProjectManager";
    info!(target: log_target, "UniswapV3PoolLib::MintByProjectManager by {sender} for owner {owner}");

    let pool_address = log.address().to_string();
    let user_address = owner.to_string();
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

    let transaction_receipt: Result<
        Option<TransactionReceipt>,
        alloy::transports::RpcError<alloy::transports::TransportErrorKind>,
    > = chain_provider
        .get_transaction_receipt(raw_transaction_hash)
        .await;
    let project_manager_address: String;

    let liquidity_decoded_user_data = get_liquidity_provider_address_evm(
        transaction_receipt,
        &client,
        periphery_contract_address,
    )
    .await;
    if let Some(pm_address) = liquidity_decoded_user_data.liquidity_user_address {
        project_manager_address = pm_address;
    } else {
        warn!(target: log_target, "Failed to get project manager address for transaction: {:?}. Reverting to fallback method", raw_transaction_hash);
        project_manager_address =
            fallback_fetch_pm_address(&client, &pool_address, log_target).await;
    };

    let token_id = liquidity_decoded_user_data.liquidity_token_id;

    let amount_token0 = Decimal::from_str(&amount0.to_string()).unwrap();
    let amount_token1 = Decimal::from_str(&amount1.to_string()).unwrap();
    let liquidity_amount = amount as i64;

    let position_id = format!("{}:{}:{}", owner, tickLower, tickUpper);

    insert_liquidity_event(
        &client,
        pool_address,
        project_manager_address,
        true, // is_add = true for mint
        true, // is_manager = true for project manager mint
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
        Some(false), // is_vault = false
        log_target,
        token_id,
    )
    .await?;

    Ok(())
}
