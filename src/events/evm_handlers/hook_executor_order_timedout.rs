use crate::{
    enums::OrderStatus, solidity_structs::hook_executor::HookExecutor, utils::unix_to_system_time,
};
use alloy::{providers::RootProvider, rpc::types::Log, transports::http::Http};
use anyhow::{bail, Context};
use log::{error, info};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use std::{str::FromStr, sync::Arc, time::SystemTime};
use tokio_postgres::{Client, GenericClient};

pub async fn handle_hook_executor_order_timeout_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let HookExecutor::OrderTimedOut {
        orderHash, orderId, ..
    } = log.log_decode()?.inner.data;

    let log_target = format!("{}: HookExecutor_OrderTimedOut", chain_id);
    info!(target: &log_target, "HookExecutor::OrderTimedOut orderId={orderId}, orderHash={orderHash:?}");

    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let timestamp_value = if log.block_timestamp.is_some() {
        log.block_timestamp.unwrap()
    } else {
        // If block_timestamp is not available, use the current system time
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs()
    };
    let timestamp = unix_to_system_time(timestamp_value);

    let query = r#"
        UPDATE hook_executor_orders
        SET status = $1, transaction_hash = $2, timestamp = $3
        WHERE order_hash = $4
    "#;

    match client
        .execute(
            query,
            &[
                &(OrderStatus::TimedOut.to_i32()),
                &transaction_hash,
                &timestamp,
                &format!("{orderHash:?}"),
            ],
        )
        .await
    {
        Ok(rows) => {
            if rows > 0 {
                info!(target: &log_target, "OrderTimedOut updated: {:?} rows", rows);
            } else {
                info!(target: &log_target, "OrderTimedOut received before OrderPending - no action taken");
            }
        }
        Err(e) => {
            error!(target: &log_target, "Failed to update OrderTimedOut: {:?}", e);
            bail!("Failed to update OrderTimedOut: {:?}", e);
        }
    }

    Ok(())
}
