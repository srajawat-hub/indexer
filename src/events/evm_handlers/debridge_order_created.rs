use std::sync::Arc;
use log::info;
use alloy::rpc::types::Log;
use tokio_postgres::Client;

use crate::solidity_structs;

pub async fn handle_debridge_order_created_event(
    log: Log,
    client: &Arc<Client>,
) -> anyhow::Result<()> {
    let log_target = "EVM DebridgeOrderCreated";
    let solidity_structs::DebridgeOrderCreated {
        orderId,
        debridgeOrderId,
    } = log.log_decode().unwrap().inner.data;
    info!(target: log_target, "solidity_structs::DebridgeOrderCreated from {orderId} with debridgeOrderId {debridgeOrderId}");

    let order_id = orderId as i64;

    let debridge_order_id = hex::encode(debridgeOrderId);

    let query =
        "UPDATE received_message_on_vault SET dln_order_id = $1 WHERE order_id = $2";
    let response = client
        .execute(query, &[&debridge_order_id, &order_id])
        .await
        .unwrap();
    info!(target: log_target,
        "solidity_structs::DebridgeOrderCreated updated response {:?}",
        response
    );

    Ok(())
}