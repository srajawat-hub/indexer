use crate::solidity_structs::intent_processor::IntentProcessorV2;
use alloy::rpc::types::Log;
use anyhow::bail;
use log::{error, info};
use std::sync::Arc;
use tokio_postgres::Client;

pub async fn handle_intent_fees_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
) -> anyhow::Result<()> {
    let IntentProcessorV2::IntentFees {
        intentId,
        feeAmount,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "IntentFees";
    info!(target: log_target, "IntentProcessorV2::IntentFees with intent_id {intentId} and feeAmount {feeAmount}");

    let fee_amount = feeAmount.to_string();
    let intent_id = intentId as i64;
    let query = "UPDATE intent SET feeamount = $1 WHERE intent_id = $2";

    let intent_rows_updated = match client.execute(query, &[&fee_amount, &intent_id]).await {
        Ok(res) => res,
        Err(e) => {
            error!(target: log_target, "Failed to update intent feeAmount {:?}", e);
            bail!("Failed to update intent feeAmount {:?}", e);
        }
    };
    info!(target: log_target,
        "updated actual amount for order, updated rows count {:?}",
        intent_rows_updated
    );

    Ok(())
}
