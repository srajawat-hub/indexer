use std::sync::Arc;
use log::info;
use alloy::{providers::RootProvider, pubsub::PubSubFrontend, rpc::types::Log};
use tokio_postgres::Client;
use crate::{
    events::event_processor::{
        fetch_intent_initiator, 
        update_intent_state, 
        IntentStage, 
        IntentVersions
    }, solidity_structs::intent_lib_v2::IntentLibV2};

pub async fn handle_solution_submitted_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<PubSubFrontend>
) -> anyhow::Result<()> {
    let IntentLibV2::SolutionSubmitted { intentId, solver } =
        log.log_decode().unwrap().inner.data;

    let log_target = "SolutionSubmitted";
    info!(target: log_target, "IntentLibV2::SolutionSubmitted from {solver} with intentId {intentId}");

    let solution_transaction_hash = log.transaction_hash.unwrap();
    let intent_block_number = log.block_number.unwrap();
    let intent_id = intentId as i64;
    let solver_address = solver.to_string();
    let transaction_hash = solution_transaction_hash.to_string();
    let block_number = intent_block_number as i64;
    let timestamp = std::time::SystemTime::now();

    let query = "INSERT INTO solution VALUES(DEFAULT, $1, $2, $3, $4, $5) ON CONFLICT (transaction_hash) DO NOTHING";
    let response = client
        .execute(
            query,
            &[
                &intent_id,
                &solver_address,
                &transaction_hash,
                &block_number,
                &timestamp,
            ],
        )
        .await
        .unwrap();
    info!(target: log_target,
        "IntentLibV2::SolutionSubmitted inserted response {:?}",
        response
    );

    let order_id: i64 = 0;
    let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

    update_intent_state(
        &intent_id,
        IntentVersions::SolutionSubmitted as i32,
        &IntentStage::Processing.to_string(),
        log.transaction_hash.unwrap(),
        &client,
        chain_provider.clone(),
        &order_id,
        chain_id,
        initiator_address,
    )
    .await;

    Ok(())
}