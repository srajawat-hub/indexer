use std::{str::FromStr, sync::Arc, time::SystemTime};
use anyhow::{bail, Context};
use log::{error, info, warn};
use alloy::{providers::RootProvider, rpc::types::Log, transports::http::Http};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tokio_postgres::{Client, GenericClient};
use crate::{enums::OrderStatus, solidity_structs::hook_executor::HookExecutor, utils::unix_to_system_time};

pub async fn handle_hook_executor_order_failed_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let HookExecutor::OrderFailed {
        orderHash,
        orderId,
        reason,
        ..
    } = log.log_decode()?.inner.data;

    let log_target = "HookExecutor_OrderFailed";
    info!(target: log_target, "HookExecutor::OrderFailed orderId={orderId}, orderHash={orderHash:?}");

    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let timestamp = unix_to_system_time(log.block_timestamp.unwrap());

    let query = r#"
        UPDATE hook_executor_orders
        SET status = $1, reason = $2, transaction_hash = $3, timestamp = $4
        WHERE order_hash = $5
    "#;

    match client
        .execute(
            query,
            &[
                &(OrderStatus::Failed.to_i32()),
                &reason,
                &transaction_hash,
                &timestamp,
                &format!("{orderHash:?}"),
            ],
        )
        .await
    {
        Ok(rows) => {
            if rows > 0 {
                info!(target: log_target, "OrderFailed updated: {:?} rows", rows);
            } else {
                warn!(target: log_target, "OrderFailed received before OrderPending - no action taken");
            }
        }
        Err(e) => {
            error!(target: log_target, "Failed to update OrderFailed: {:?}", e);
            bail!("Failed to update OrderFailed: {:?}", e);
        }
    }

    Ok(())
}