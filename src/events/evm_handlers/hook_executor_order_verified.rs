use std::{str::FromStr, sync::Arc, time::SystemTime};
use anyhow::{bail, Context};
use log::{info, error};
use alloy::{providers::RootProvider, rpc::types::Log, transports::http::Http};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio_postgres::{Client, GenericClient};
use crate::{enums::OrderStatus, solidity_structs::hook_executor::HookExecutor, utils::unix_to_system_time};

pub async fn handle_hook_executor_order_verified_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let HookExecutor::OrderVerified {
        protocolId,
        orderHash,
        fulfiller,
    } = log.log_decode()?.inner.data;

    let log_target = "HookExecutor_OrderVerified";
    info!(target: log_target, "HookExecutor::OrderVerified orderHash={orderHash:?}");

    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let timestamp = unix_to_system_time(log.block_timestamp.unwrap());

    let status = OrderStatus::Verified.to_i32();
    let query = r#"
        UPDATE hook_executor_orders
        SET status = $1, transaction_hash = $2, timestamp = $3
        WHERE order_hash = $4
    "#;

    match client
        .execute(
            query,
            &[
                &status,
                &transaction_hash,
                &timestamp,
                &format!("{orderHash:?}"),
            ],
        )
        .await
    {
        Ok(rows) => {
            if rows > 0 {
                info!(target: log_target, "OrderVerified updated: {:?} rows", rows);
            } else {
                info!(target: log_target, "OrderVerified received before OrderPending - no action taken");
            }
        }
        Err(e) => {
            error!(target: log_target, "Failed to update OrderVerified: {:?}", e);
            bail!("Failed to update OrderVerified: {:?}", e);
        }
    }

    Ok(())
}