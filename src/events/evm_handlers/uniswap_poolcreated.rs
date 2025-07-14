use alloy::{
    providers::{Provider, RootProvider},
    rpc::types::Log,
    transports::http::Http,
};
use anyhow::bail;
use log::{error, info};
use rust_decimal::Decimal;
use std::{sync::Arc, time::SystemTime};
use tokio_postgres::Client;

use crate::{
    solidity_structs::{
        token,
        uniswap_v3_factory_lib::UniswapV3FactoryLib::{self},
        uniswap_v3_pool_lib::UniswapV3PoolLib::UniswapV3PoolLibInstance,
    },
    structs::{PoolType, TokenLaunchType},
    utils::{chain_id_to_chain_name, get_token_data, unix_to_system_time},
};

pub async fn handle_uniswap_pool_created_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let UniswapV3FactoryLib::PoolCreated {
        token0,
        token1,
        fee,
        tickSpacing,
        pool,
        launchParams,
    } = log.log_decode().unwrap().inner.data;
    let log_target = "PoolCreated";
    info!(target: log_target, "UniswapV3FactoryLib::PoolCreated: {pool}, {token0}, {token1}");

    let timestamp = SystemTime::now();
    let block_number_i64 = log.block_number.unwrap() as i64;

    let pool_instance = UniswapV3PoolLibInstance::new(pool, chain_provider.clone());
    info!("pool instance {:?}", pool_instance);
    let slot = match pool_instance.slot0().call().await {
        Ok(slot) => {
            info!("slot {:?}", slot);
            slot
        }
        Err(e) => {
            error!(target: log_target, "Error fetching slot0 for pool {}: {:?}", pool, e);
            bail!("Error fetching slot0 for pool {}: {:?}", pool, e);
        }
    };
    let pool_address = pool.to_string();
    let chain_id_i64 = chain_provider.get_chain_id().await? as i64;
    let token0_addr = token0.to_string();
    let token1_addr = token1.to_string();
    let fee_decimal = fee.to::<i64>();
    let tick_spacing_i64 = tickSpacing.as_i64();
    let pool_type = PoolType::EVM;
    let project_manager = launchParams.projectManager.to_string();
    let metadata_json = serde_json::Value::Null;
    let etp_start_time = unix_to_system_time(launchParams.exclusiveTradingPeriodStart.to());
    let etp_end_time = unix_to_system_time(launchParams.exclusiveTradingPeriodEnd.to());
    let extended_liquidity_lock_duration = unix_to_system_time(
        (launchParams.extendedLiquidityLockDuration + launchParams.exclusiveTradingPeriodEnd).to(),
    );
    let launch_type = if launchParams.tokenLaunchType == 0 {
        TokenLaunchType::FAIR
    } else {
        TokenLaunchType::CURATED
    };
    let initial_sqrt = slot.sqrtPriceX96.to_string();
    let initial_tick_i32 = slot.tick.to_string();
    info!("initial_tick {:?}", initial_tick_i32);
    let token = token::Token::new(token1, chain_provider.clone());
    let token_supply_i64 = match token.totalSupply().call().await {
        Ok(supply) => supply._0.to_string(),
        Err(e) => {
            error!("Error in fetching token supply from its contract, defaulting to 0");
            "0".to_string()
        }
    };

    info!(target: log_target, "UniswapV3FactoryLib::PoolCreated - pool_address: {}, chain_id: {}, token_0_address: {}, token_1_address: {}, fee: {}, tick_spacing: {}, pool_type: {:?}, project_manager: {}, block_number: {}, created_at: {:?}, metadata: {:?}, etp_start_time: {:?}, etp_end_time: {:?}, launch_type: {:?}, initial_sqrt_price: {}, initial_tick: {}, token_supply: {}",
        pool_address, chain_id_i64, token0_addr, token1_addr, fee_decimal, tick_spacing_i64, pool_type, project_manager, block_number_i64, timestamp, metadata_json, etp_start_time, etp_end_time, launch_type, initial_sqrt, initial_tick_i32, token_supply_i64);

    // --- perform the INSERT ---
    let query = r#"
          INSERT INTO pools
            (pool_address, chain_id, token_0_address, token_1_address, fee,
             tick_spacing, pool_type, project_manager, block_number, created_at,
             metadata, etp_start_time, etp_end_time, launch_type, initial_sqrt_price,
             initial_tick, token_supply, launchpad_token, liquidity_lock_end_timestamp)
          VALUES
            ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::token_launch_type,$15,$16,$17,$18,$19)
          ON CONFLICT (pool_address) DO NOTHING
        "#;

    let rows = match client
        .execute(
            query,
            &[
                &pool_address,
                &chain_id_i64,
                &token0_addr,
                &token1_addr,
                &Decimal::from(fee_decimal),
                &tick_spacing_i64,
                &pool_type,
                &project_manager,
                &block_number_i64,
                &timestamp,
                &metadata_json,
                &etp_start_time,
                &etp_end_time,
                &launch_type,
                &initial_sqrt,
                &initial_tick_i32.parse::<i32>().unwrap(),
                &token_supply_i64,
                &token1_addr, // token1 is the launchpad token for evm pools
                &extended_liquidity_lock_duration,
            ],
        )
        .await
    {
        Ok(rows) => rows,
        Err(e) => {
            error!(target: log_target, "Error inserting UniswapV3FactoryLib::PoolCreated data: {:?}", e);
            bail!(
                "Error inserting UniswapV3FactoryLib::PoolCreated data: {:?}",
                e
            );
        }
    };
    info!(
        target: log_target, "UniswapV3FactoryLib::PoolCreated inserted a fallback response {:?}",
        rows
    );

    let token_chains_query = r#"
        INSERT INTO token_chains (token_id, address, decimals, network, address_bytes32) VALUES($1, $2, $3, $4, $5)
    "#;

    let tokens_query = "INSERT INTO tokens (ticker, full_name, is_stable, is_tradable, price_usd, description, launch_date, website, cmc_id) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (address, network) DO NOTHING";

    let token_id = &pool_address;
    let network = chain_id_to_chain_name(chain_id);
    let address_bytes32 = format!("{:0>64}", hex::encode(token1));

    let (decimals, ticker, full_name) =
        get_token_data(token1.clone(), chain_provider.clone()).await;
    let is_stable = false;
    let is_tradable = false;
    let price_usd = Decimal::from(0_i64);
    let description = String::new();
    let launch_date = timestamp.clone();
    let website = String::new();
    let cmc_id = String::new();

    // -- add in the tokens table
    match client
        .execute(
            token_chains_query,
            &[
                &token_id,
                &token1_addr,
                &decimals,
                &network,
                &address_bytes32,
            ],
        )
        .await
    {
        Ok(rows) => {
            info!(target: log_target, "Inserted token data into token_chains table: {:?}", rows);
            match client
                .execute(
                    tokens_query,
                    &[
                        &ticker,
                        &full_name,
                        &is_stable,
                        &is_tradable,
                        &price_usd,
                        &description,
                        &launch_date,
                        &website,
                        &cmc_id,
                    ],
                )
                .await
            {
                Ok(rows) => {
                    info!(target: log_target, "Inserted token data into tokens table: {:?}", rows);
                }
                Err(e) => {
                    error!(target: log_target, "Error inserting token chain data into token_chains table: {:?}", e);
                    bail!(
                        "Error inserting token chain data into token_chains table: {:?}",
                        e
                    );
                }
            }
        }
        Err(e) => {
            error!(target: log_target, "Error inserting token data into tokens table: {:?}", e);
            bail!("Error inserting token data into tokens table: {:?}", e);
        }
    };

    Ok(())
}
