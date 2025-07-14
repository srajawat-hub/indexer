use crate::{
    enums::OrderStatus, solidity_structs::hook_executor::HookExecutor, utils::unix_to_system_time,
};
use alloy::{providers::RootProvider, rpc::types::Log, transports::http::Http};
use anyhow::{bail, Context};
use log::{error, info};
use rust_decimal::{prelude::FromPrimitive, Decimal};
use std::{str::FromStr, sync::Arc, time::SystemTime};
use tokio_postgres::{Client, GenericClient};

pub async fn handle_hook_executor_order_pending_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let HookExecutor::OrderPending {
        protocolId,
        orderHash,
        orderId,
        recipient,
        token,
        amount,
        timeoutTimestamp,
        destinationChainId,
        additionalData,
        reason,
    } = log.log_decode()?.inner.data;

    let log_target = "HookExecutor_OrderPending";
    info!(target: log_target, "HookExecutor::OrderPending orderId={orderId}, orderHash={orderHash:?}, recipient={recipient}");

    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap() as i64;
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
        INSERT INTO hook_executor_orders
        (protocol_id, order_hash, order_id, recipient, token, amount, timeout_timestamp,
         reason, transaction_hash, block_number, timestamp, status,
         destination_chain_id, additional_data)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)
        ON CONFLICT (order_hash) DO UPDATE SET
            status = EXCLUDED.status,
            reason = EXCLUDED.reason,
            timestamp = EXCLUDED.timestamp,
            transaction_hash = EXCLUDED.transaction_hash,
            destination_chain_id = EXCLUDED.destination_chain_id,
            additional_data = EXCLUDED.additional_data
    "#;

    match client
        .execute(
            query,
            &[
                &(protocolId as i32),
                &format!("{orderHash:?}"),
                &(orderId as i64),
                &recipient.to_string(),
                &token.to_string(),
                &rust_decimal::Decimal::from_str_exact(&amount.to_string()).unwrap_or_default(),
                &(timeoutTimestamp.to::<i64>()),
                &reason,
                &transaction_hash,
                &block_number,
                &timestamp,
                &(OrderStatus::Pending.to_i32()),
                &(destinationChainId as i64),
                &if additionalData.is_empty() {
                    None
                } else {
                    Some(hex::encode(&additionalData))
                },
            ],
        )
        .await
    {
        Ok(rows) => {
            info!(target: log_target, "OrderPending inserted/updated: {:?} rows", rows);
        }
        Err(e) => {
            error!(target: log_target, "Failed to insert OrderPending: {:?}", e);
            bail!("Failed to insert OrderPending: {:?}", e);
        }
    }

    Ok(())
}
