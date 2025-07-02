use std::{str::FromStr, sync::Arc, time::SystemTime};
use anyhow::Context;
use log::{info, error};
use alloy::{providers::RootProvider, pubsub::PubSubFrontend, rpc::types::Log};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio_postgres::{Client, GenericClient};

use crate::{events::event_processor::{check_vault_initiated_transaction, get_usd_value_of_native}, solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib::{self}, utils::{get_native_token_cmc_id, get_wrapped_native_token_address, unix_to_system_time}};

pub async fn handle_uniswap_swap_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<PubSubFrontend>
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

    let log_target = "Swap";
    info!(target: log_target, "UniswapV3PoolLib::Swap from {sender} to {recipient}");

    let pool_address = log.address().to_string();
    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap() as i64;
    let timestamp = match log.block_timestamp {
        Some(ts) => unix_to_system_time(ts),
        None => {
            error!(target: log_target, "Block timestamp is missing for log: {:?}", log);
            SystemTime::now() // Fallback to current time if timestamp is missing
        }
    };
    let sqrt_price = sqrtPriceX96.to_string();
    let liquidity_i64 = liquidity as i64;
    let tick_i32 = tick.as_i32();
    let initiator_user_address = sender.to_string();

    // get user address from received message on vault
    let user_query = "SELECT sender_address FROM received_message_on_vault WHERE LOWER(transaction_hash) = LOWER($1)";
    let sender_address = match client.query_one(user_query, &[&transaction_hash]).await {
        Ok(row) => row.get::<_, String>("sender_address"),
        Err(e) => {
            error!(target: log_target, "Failed to fetch initiator user address for transaction hash {}: {:?}", transaction_hash, e);
            initiator_user_address.clone() // Fallback to sender address if query fails
        }
    };

    // Query pool to get token addresses
    let pool_query =
        "SELECT token_0_address, token_1_address FROM pools WHERE pool_address = $1";
    let pool_row = client
        .query_one(pool_query, &[&pool_address])
        .await
        .context("Failed to fetch pool tokens for swap")?;
    let token0_address: String = pool_row.get("token_0_address");
    let token1_address: String = pool_row.get("token_1_address");

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

    // Calculate price if amounts are valid
    let price = if let (Ok(amt_in), Ok(amt_out)) =
        (amount_in.parse::<f64>(), amount_out.parse::<f64>())
    {
        if amt_in > 0.0 {
            Some(amt_out / amt_in)
        } else {
            None
        }
    } else {
        None
    };

    // Calculate USD values for amounts
    // For EVM chains, we currently only have ETH/XXX ppols - determine which token is native
    let wrapped_native_address = get_wrapped_native_token_address(chain_id);
    let zero_address = "0x0000000000000000000000000000000000000000";

    let (native_token, _other_token, native_amount, other_amount) =
        if token_in.to_lowercase() == wrapped_native_address.to_lowercase()
            || token_in.to_lowercase() == zero_address
        {
            (
                token_in.clone(),
                token_out.clone(),
                amount_in.clone(),
                amount_out.clone(),
            )
        } else if token_out.to_lowercase() == wrapped_native_address.to_lowercase()
            || token_out.to_lowercase() == zero_address
        {
            (
                token_out.clone(),
                token_in.clone(),
                amount_out.clone(),
                amount_in.clone(),
            )
        } else {
            // If neither token is the native/wrapped native token, we can't calculate USD values accurately
            (
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
                "0".to_string(),
            )
        };

    let (amount_in_usd, amount_out_usd) = if native_token != "0" {
        let decimals = get_native_token_cmc_id(chain_id).1;
        // Get ETH price in USD
        let eth_price_usd = get_usd_value_of_native(
            &chain_id,
            &(10_u128.pow(decimals)),
            None,
            None,
            None,
            None,
        )
        .await;

        let eth_price_f64 = eth_price_usd.parse::<f64>().unwrap_or(0.0);

        if eth_price_f64 > 0.0 && price.is_some() {
            let swap_price = price.unwrap();

            // Calculate USD values using the extracted variables
            let native_amount_eth =
                native_amount.parse::<f64>().unwrap_or(0.0) / 10_f64.powf(decimals as f64);
            let other_amount_tokens = other_amount.parse::<f64>().unwrap_or(0.0);

            let native_usd_val = native_amount_eth * eth_price_f64;
            let other_usd_val = if swap_price != 0.0 {
                other_amount_tokens / swap_price * eth_price_f64
            } else {
                0.0
            };

            // Map back to amount_in_usd and amount_out_usd based on which is which
            if token_in.to_lowercase() == native_token.to_lowercase() {
                (native_usd_val.to_string(), other_usd_val.to_string())
            } else {
                (other_usd_val.to_string(), native_usd_val.to_string())
            }
        } else {
            ("0".to_string(), "0".to_string())
        }
    } else {
        ("0".to_string(), "0".to_string())
    };

    let is_vault_initiated = check_vault_initiated_transaction(chain_provider.clone(), &client, log.transaction_hash.unwrap().clone()).await;

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
    info!(target: log_target, "UniswapV3PoolLib::Swap inserted: {:?} rows", rows);

    Ok(())
}