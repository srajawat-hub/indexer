use alloy::{dyn_abi::SolType, providers::RootProvider, rpc::types::Log, transports::http::Http};
use anyhow::bail;
use log::{error, info};
use std::sync::Arc;
use tokio_postgres::Client;

use crate::{
    enums::OrderStatus,
    solidity_structs::hook_executor::HookExecutor::{self, OrderProcessingData},
    utils::{get_evm_block_timestamp, unix_to_system_time},
};

pub async fn handle_hook_executor_hyp_msg_received(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let log_target = "HookExecutor_HyperlaneMessageReceived";

    let HookExecutor::HyperlaneMessageReceived {
        origin,
        sender,
        messageData,
    } = log.log_decode()?.inner.data;

    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap();
    let block_timestamp = get_evm_block_timestamp(&chain_provider, block_number).await;
    let timestamp = unix_to_system_time(block_timestamp);

    let order_data = match OrderProcessingData::abi_decode(&messageData, true) {
        Ok(order_data) => order_data,
        Err(e) => {
            error!(target: log_target, "Failed to decode hookexecutor order data: {}", e);
            return Err(anyhow::anyhow!("Failed to decode order data: {}", e));
        }
    };

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

    let reason = format!("Hyperlane message received from origin {}", origin);

    match client
        .execute(
            query,
            &[
                &(order_data.protocolId as i32),
                &order_data.orderHash.to_string(),
                &(order_data.orderId as i64),
                &order_data.recipient.to_string(),
                &order_data.token.to_string(),
                &rust_decimal::Decimal::from_str_exact(&order_data.amount.to_string())
                    .unwrap_or_default(),
                &(order_data.timeoutTimestamp.to::<i64>()),
                &reason,
                &transaction_hash,
                &(block_number as i64),
                &timestamp,
                &(OrderStatus::Pending.to_i32()),
                &(order_data.destinationChainId as i64),
                &if order_data.additionalData.is_empty() {
                    None
                } else {
                    Some(hex::encode(&order_data.additionalData))
                },
            ],
        )
        .await
    {
        Ok(rows) => {
            info!(target: log_target, "HookExecutor_HyperlaneMessageReceived inserted/updated: {:?} rows", rows);
        }
        Err(e) => {
            error!(target: log_target, "Failed to insert HookExecutor_HyperlaneMessageReceived: {:?}", e);
            bail!(
                "Failed to insert HookExecutor_HyperlaneMessageReceived: {:?}",
                e
            );
        }
    }

    Ok(())
}
