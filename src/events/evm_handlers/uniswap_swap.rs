use alloy::{
    eips::BlockNumberOrTag,
    providers::{Provider, RootProvider},
    rpc::types::{BlockTransactionsKind, Log},
    transports::http::Http,
};
use anyhow::Context;
use log::{error, info, warn};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use sha2::digest::crypto_common::rand_core::block;
use std::{str::FromStr, sync::Arc, time::SystemTime};
use tokio_postgres::{Client, GenericClient};

use crate::{
    events::event_processor::check_vault_initiated_transaction,
    solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib::{self},
    utils::{
        get_evm_block_timestamp, get_native_token_cmc_id, get_token_decimals,
        get_usd_value_of_token, get_wrapped_native_token_address, unix_to_system_time,
    },
};

pub async fn handle_uniswap_swap_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let UniswapV3PoolLib::Swap {
        sender,
        recipient,
        amount0,
        amount1,
        sqrtPriceX96,
        liquidity,
        tick,
    } = log.log_decode().unwrap().inner.data;

    let log_target = format!("{}: Swap", chain_id);
    info!(target: &log_target, "UniswapV3PoolLib::Swap from {sender} to {recipient}");

    let pool_address = log.address().to_string();
    let raw_transaction_hash = log.transaction_hash.unwrap();
    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap() as i64;
    let sqrt_price = sqrtPriceX96.to_string();
    let liquidity_i64 = liquidity as i64;
    let tick_i32 = tick.as_i32();
    let initiator_user_address = sender.to_string();

    // get user address from received message on vault
    let (is_vault_initiated, initiator_address) = check_vault_initiated_transaction(
        chain_provider.clone(),
        &client,
        raw_transaction_hash.clone(),
    )
    .await;

    let sender_address = match initiator_address {
        Some(sender) => sender,
        None => {
            error!(target: &log_target, "Failed to fetch initiator user address for transaction hash {}", transaction_hash);
            initiator_user_address.clone() // Fallback to sender address if query fails
        }
    };

    // Query pool to get token addresses
    let pool_query =
        "SELECT token_0_address, token_1_address, launchpad_token FROM pools WHERE pool_address = $1";
    let pool_row = client
        .query_one(pool_query, &[&pool_address])
        .await
        .context("Failed to fetch pool tokens for swap")?;
    let token0_address: String = pool_row.get("token_0_address");
    let token1_address: String = pool_row.get("token_1_address");
    let launchpad_token: String = pool_row.get("launchpad_token");

    let base_token = if launchpad_token.to_lowercase() == token0_address.to_lowercase() {
        token1_address.clone()
    } else {
        token0_address.clone()
    };

    // Determine token flow based on amount signs
    let (token_in, token_out, amount_in, amount_out) = if amount0.is_negative() {
        (
            token1_address,
            token0_address,
            amount1.to_string(),
            (-amount0).to_string(),
        )
    } else {
        (
            token0_address,
            token1_address,
            amount0.to_string(),
            (-amount1).to_string(),
        )
    };

    // Calculate USD values for amounts

    let block_timestamp_of_trade =
        get_evm_block_timestamp(&chain_provider, block_number as u64).await;

    let timestamp = if block_timestamp_of_trade != 0 {
        unix_to_system_time(block_timestamp_of_trade)
    } else {
        error!(target: &log_target, "Block timestamp is 0 for block number {}", block_number);
        SystemTime::now() // Fallback to current time if timestamp is 0
    };

    let (amount_in_usd, amount_out_usd, price) = get_usd_value_of_token_at_timestamp(
        &base_token,
        &token_in,
        &token_out,
        &amount_in,
        &amount_out,
        chain_id.to_string().as_str(),
        block_timestamp_of_trade,
    )
    .await;

    let is_vault_initiated = check_vault_initiated_transaction(
        chain_provider.clone(),
        &client,
        log.transaction_hash.unwrap().clone(),
    )
    .await
    .0;

    let query = r#"
        INSERT INTO ammswap
            (pool_address, token_in, token_out, amount_in, amount_out, amount_in_usd,
             amount_out_usd, initiator_user_address, price, transaction_hash, block_number,
             timestamp, chain_id, is_vault_initiated, sqrt_price, liquidity, tick)
        VALUES
            ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (transaction_hash) DO NOTHING
    "#;

    let rows = client
        .execute(
            query,
            &[
                &pool_address,
                &token_in,
                &token_out,
                &Decimal::from_str(&amount_in).unwrap(),
                &Decimal::from_str(&amount_out).unwrap(),
                &Decimal::from_str(&amount_in_usd).unwrap(),
                &Decimal::from_str(&amount_out_usd).unwrap(),
                &sender_address,
                &Decimal::from_f64(price.unwrap_or(0.0)),
                &transaction_hash,
                &block_number,
                &timestamp,
                &chain_id,
                &is_vault_initiated,
                &sqrt_price,
                &liquidity_i64,
                &tick_i32,
            ],
        )
        .await
        .context("Failed to insert swap data")?;
    info!(target: &log_target, "UniswapV3PoolLib::Swap inserted: {:?} rows", rows);

    Ok(())
}

async fn get_usd_value_of_token_at_timestamp(
    base_token: &str, //address of the base token
    token_in: &str,
    token_out: &str,
    amount_in: &str,
    amount_out: &str,
    chain_id: &str,
    block_timestamp_of_trade: u64,
) -> (String, String, Option<f64>) {
    let mut amount_in_usd_value: f64 = 0.0;
    let mut amount_out_usd_value: f64 = 0.0;
    let mut normalized_price: f64 = 0.0;
    let log_target = format!("{chain_id}: get_usd_value_of_token_at_timestamp");

    info!("Block timestamp of the trade {block_timestamp_of_trade}");
    let base_token_usd_price = if block_timestamp_of_trade != 0 {
        get_usd_value_of_token(
            Some(&base_token),
            &chain_id.to_string(),
            Some(block_timestamp_of_trade),
        )
        .await
    } else {
        get_usd_value_of_token(Some(&base_token), &chain_id.to_string(), None).await
    };

    info!("base token usd price {base_token_usd_price}");

    let base_token_decimals = get_token_decimals(&base_token, &chain_id.to_string()).await;

    let token_in_decimals = get_token_decimals(&token_in, &chain_id.to_string()).await;
    let token_out_decimals = get_token_decimals(&token_out, &chain_id.to_string()).await;

    let formatted_amount_in =
        amount_in.parse::<f64>().unwrap_or(0.0) / 10_f64.powf(token_in_decimals as f64);
    let formatted_amount_out =
        amount_out.parse::<f64>().unwrap_or(0.0) / 10_f64.powf(token_out_decimals as f64);

    info!(target: &log_target, "formatted_amount_in: {}", formatted_amount_in);
    info!(target: &log_target, "formatted_amount_out: {}", formatted_amount_out);

    if formatted_amount_in == 0.0 {
        warn!("Formatted amount in is 0, returning usd amounts as 0");
        return ("0".to_string(), "0".to_string(), None);
    }

    // Normalize price to always represent "USDC per token"
    let normalized_price = if token_in.to_lowercase() == base_token.to_lowercase() {
        // USDC -> Token swap: price = usdc_in / tokens_out
        formatted_amount_in / formatted_amount_out
    } else {
        // Token -> USDC swap: price = usdc_out / tokens_in
        formatted_amount_out / formatted_amount_in
    };

    info!(target: &log_target, "Normalized price calculated: {}", normalized_price);

    if token_in.to_lowercase() == base_token.to_lowercase() {
        // If token_in is the base token, calculate amount_in_usd based on base token price
        amount_in_usd_value = formatted_amount_in * base_token_usd_price;

        // launchpad token
        let amount_out_usd_price = normalized_price * base_token_usd_price;
        amount_out_usd_value = formatted_amount_out * amount_out_usd_price;
    } else {
        // If token_out is the base token, calculate amount_out_usd based on base token price
        amount_out_usd_value = formatted_amount_out * base_token_usd_price;

        // launchpad token
        let amount_in_usd_price = normalized_price * base_token_usd_price;
        amount_in_usd_value = formatted_amount_in * amount_in_usd_price;
    };

    println!(
        "Calculated USD values: amount_in_usd: {}, amount_out_usd: {}",
        amount_in_usd_value, amount_out_usd_value
    );
    (
        amount_in_usd_value.to_string(),
        amount_out_usd_value.to_string(),
        Some(normalized_price),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_env() {
        let _ = dotenv::dotenv();
    }

    fn init_logger() {
        let _ = env_logger::builder()
            .is_test(true) // ensures logs show up in tests
            .filter_level(log::LevelFilter::Info) // sets minimum level to info
            .try_init();
    }

    #[tokio::test]
    async fn test_get_usd_value_of_token_at_timestamp() {
        init_env();
        init_logger();
        let base_token = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
        let token_in = "0x2017624785ff7889118336F927B86878A7c59B73";
        let token_out = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
        let amount_in = "7532800008145831435294";
        let amount_out = "2204914";
        let chain_id = "8453";
        let block_timestamp_of_trade = 1753962749;
        let (amount_in_usd, amount_out_usd, normalized_price) =
            get_usd_value_of_token_at_timestamp(
                &base_token,
                &token_in,
                &token_out,
                &amount_in,
                &amount_out,
                &chain_id,
                block_timestamp_of_trade,
            )
            .await;

        println!("amount_in_usd: {}", amount_in_usd);
        println!("amount_out_usd: {}", amount_out_usd);
        println!("normalized_price: {:?}", normalized_price);
        // assert!(amount_in_usd > 0.0);
        // assert!(amount_out_usd > 0.0);
        // assert!(normalized_price > 0.0);
    }

    #[tokio::test]
    async fn test_get_usd_value_of_token_at_timestamp_fallback() {
        init_env();
        init_logger();
        let base_token = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
        let token_in = "0x2017624785ff7889118336F927B86878A7c59B73";
        let token_out = "0x833589fCD6eDb6E08f4c7C32D4f71b54bdA02913";
        let amount_in = "7532800008145831435294";
        let amount_out = "2204914";
        let chain_id = "8453";

        // future timestamp
        let block_timestamp_of_trade = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
            + 1000000; // 1 million seconds in the future
        let (amount_in_usd, amount_out_usd, normalized_price) =
            get_usd_value_of_token_at_timestamp(
                &base_token,
                &token_in,
                &token_out,
                &amount_in,
                &amount_out,
                &chain_id,
                block_timestamp_of_trade,
            )
            .await;

        println!("amount_in_usd: {}", amount_in_usd);
        println!("amount_out_usd: {}", amount_out_usd);
        println!("normalized_price: {:?}", normalized_price);
        // assert!(amount_in_usd > 0.0);
        // assert!(amount_out_usd > 0.0);
        // assert!(normalized_price > 0.0);
    }
}
