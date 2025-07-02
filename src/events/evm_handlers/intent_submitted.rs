// pass the log and db client here and process it

use std::sync::Arc;
use log::info;
use alloy::{providers::RootProvider, pubsub::PubSubFrontend, rpc::types::Log};
use tokio_postgres::Client;
use crate::{events::event_processor::{update_intent_state, IntentStage, IntentVersions}, solidity_structs::intent_lib_v2::IntentLibV2};

pub async fn handle_intent_submitted_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<PubSubFrontend>,
) -> anyhow::Result<()> {
    let IntentLibV2::IntentSubmitted {
        intentId,
        owner,
        feeAmount,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "IntentSubmitted";
    info!(target: log_target, "IntentLibV2::IntentSubmitted from {owner} with intentId {intentId}");

    let intent_transaction_hash = log.transaction_hash.unwrap();
    let intent_block_number = log.block_number.unwrap();
    let intent_id = intentId as i64;
    let owner_address = owner.to_string();
    let transaction_hash = intent_transaction_hash.to_string();
    let block_number = intent_block_number as i64;
    let current_timestamp = std::time::SystemTime::now();
    let timestamp = current_timestamp;
    let fee_amount = feeAmount.to_string();

    let query = "INSERT INTO intent VALUES(DEFAULT, $1, $2, $3, $4, $5, $6) ON CONFLICT (transaction_hash) DO NOTHING";
    let response = client
        .execute(
            query,
            &[
                &intent_id,
                &owner_address,
                &transaction_hash,
                &block_number,
                &timestamp,
                &fee_amount,
            ],
        )
        .await
        .unwrap();
    info!(target: log_target,
        "IntentLibV2::IntentSubmitted inserted response {:?}",
        response
    );

    let order_id: i64 = 0;

    update_intent_state(
        &intent_id,
        IntentVersions::IntentSubmitted as i32,
        &IntentStage::Initialized.to_string(),
        log.transaction_hash.unwrap(),
        &client,
        chain_provider.clone(),
        &order_id,
        chain_id,
        owner_address,
    )
    .await;
    
    Ok(())
}