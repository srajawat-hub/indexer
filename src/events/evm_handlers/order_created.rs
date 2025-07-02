use std::sync::Arc;
use alloy::dyn_abi::SolType;
use anyhow::bail;
use log::{info, error};
use alloy::{providers::RootProvider, pubsub::PubSubFrontend, rpc::types::Log};
use tokio_postgres::Client;
use crate::events::event_processor::{fetch_intent_initiator, get_amount_usd_value, get_fees_data, update_intent_state, IntentStage, IntentVersions};
use crate::solidity_structs::intent_lib_v2::IntentLibV2;
use crate::solidity_structs::{
    self, ReceiverUserAddressData, ReceiverVaultData, ResultCosts, SolidityOrder
};

pub async fn handle_order_created_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<PubSubFrontend>,
    solana_chain_id: &str
) -> anyhow::Result<()> {
    let IntentLibV2::OrderCreated {
        intentId,
        orderId,
        order,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "OrderCreated";
    info!(target: log_target, "IntentLibV2::OrderCreated for {intentId}, with order Id {orderId}");

    let order_slice = order.as_ref();
    let order_struct = SolidityOrder::abi_decode(order_slice, true).unwrap();

    let intent_id = order_struct.intentId as i64;
    let order_id = order_struct.orderId as i64;
    let creator_address = order_struct.initiatorAddress.to_string();
    let token_in = order_struct.tokenIn.to_string();
    let token_out = order_struct.tokenOut.to_string();
    let amount_in = order_struct.amountIn.to_string();
    let amount_out = order_struct.amountOut.to_string();
    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap() as i64;
    let source_chain_id = order_struct.sourceChainId.to_string();
    let destination_chain_id = order_struct.destinationChainId.to_string();
    let multi_leg = order_struct.multiLeg;
    let order_payload: String = order.to_string();
    let solution_type = order_struct.solution.enumVariant as i32;
    let receiver_type: i32 = order_struct.receiver.enumVariant as i32;

    let receiver_data = order_struct.receiver.data;
    let receiver_address: String;
    if receiver_type == 0 {
        let receiver_address_struct =
            ReceiverUserAddressData::abi_decode(&receiver_data, true).unwrap();
        receiver_address = receiver_address_struct.userAddress.to_string();
    } else {
        let receiver_address_struct =
            ReceiverVaultData::abi_decode(&receiver_data, true).unwrap();
        receiver_address = receiver_address_struct.vaultUser.to_string();
    }

    let current_timestamp = std::time::SystemTime::now();
    let timestamp = current_timestamp;

    let amount_in_usd = get_amount_usd_value(
        token_in.clone(),
        source_chain_id.clone(),
        Some(amount_in.clone()),
        &client,
        None,
    )
    .await;
    let amount_out_usd = get_amount_usd_value(
        token_out.clone(),
        destination_chain_id.clone(),
        Some(amount_out.clone()),
        &client,
        None,
    )
    .await;

    let query: &str =
        "INSERT INTO order_created VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19) ON CONFLICT (transaction_hash) DO NOTHING";
    let response = client
        .execute(
            query,
            &[
                &intent_id,
                &creator_address,
                &token_in,
                &token_out,
                &amount_in,
                &amount_out,
                &transaction_hash,
                &block_number,
                &timestamp,
                &order_id,
                &source_chain_id,
                &destination_chain_id,
                &multi_leg,
                &order_payload,
                &solution_type,
                &receiver_type,
                &receiver_address,
                &amount_in_usd,
                &amount_out_usd,
            ],
        )
        .await
        .unwrap();
    info!(target: log_target, "IntentLibV2::OrderCreated inserted response {:?}", response);

    let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

    update_intent_state(
        &intent_id,
        IntentVersions::OrderCreated as i32,
        &IntentStage::Processing.to_string(),
        log.transaction_hash.unwrap(),
        &client,
        chain_provider.clone(),
        &order_id,
        chain_id,
        initiator_address,
    )
    .await;

    let intent_fees: ResultCosts = get_fees_data(
        &source_chain_id,
        &destination_chain_id,
        &token_in,
        &token_out,
        &amount_in,
        solana_chain_id,
    )
    .await;

    let inclusive_layer_fee_usd = get_amount_usd_value(
        token_in.clone(),
        source_chain_id.clone(),
        intent_fees.inclusive_layer_fee.value.clone(),
        &client,
        None,
    )
    .await;

    let mut fee_data_json = match serde_json::to_value(&intent_fees) {
        Ok(value) => value,
        Err(e) => {
            error!(target: log_target, "Error in getting fees data from quotation service: {:?}", e);
            bail!("Error in getting fees data from quotation service {:?}", e);
        }
    };

    let inclusive_layer_fee_usd_json = serde_json::json!({
        "value": Some(inclusive_layer_fee_usd),
        "value_type": Some(String::from("USD"))
    });

    // Insert the new field into the object
    if let serde_json::Value::Object(ref mut map) = fee_data_json {
        map.insert(
            "inclusive_layer_fee_usd".to_string(),
            inclusive_layer_fee_usd_json,
        );
    }

    let intent_fee_add_query = "INSERT INTO intent_fees VALUES(DEFAULT, $1, $2) ON CONFLICT (intent_id) DO NOTHING";
    match client
        .execute(intent_fee_add_query, &[&intent_id, &fee_data_json])
        .await
    {
        Ok(res) => {
            info!(target: log_target, "intent_fee_add_res {:?}", res);
        }
        Err(e) => {
            error!(target: log_target, "error in posting to intent_fees table {:?}", e);
            bail!("error in posting to intent_fees table {:?}", e);
        }
    };

    info!("Order created event received, but no specific handling implemented yet.");
    Ok(())
}