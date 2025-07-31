use crate::{
    events::event_processor::{
        fetch_intent_initiator, update_intent_state, IntentStage, IntentVersions,
    },
    solidity_structs::{
        intent_processor::IntentProcessorV2, AcknowledgementMetadataLaunchpadAddLiquidity,
        AcknowledgementMetadataLaunchpadRemoveLiquidity, AcknowledgementMetadataLaunchpadSwap,
        AcknowledgementMetadataStake, AcknowledgementMetadataTransact,
        AcknowledgementMetadataTransactFailed, SolidityAcknowledgementMetadata,
    },
    utils::{get_token_decimals, get_usd_value_of_token},
};
use alloy::{
    dyn_abi::SolType, providers::RootProvider, pubsub::PubSubFrontend, rpc::types::Log,
    transports::http::Http,
};
use anyhow::bail;
use log::{error, info, warn};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use tokio_postgres::Client;

pub async fn handle_acknowledgement_received_event(
    log: Log,
    client: &Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<Http<reqwest::Client>>,
) -> anyhow::Result<()> {
    let IntentProcessorV2::AcknowledgementReceived {
        orderId,
        sender,
        result,
        errorMessage,
        metadata,
    } = log.log_decode().unwrap().inner.data;

    let log_target = "AcknowledgementReceived";
    info!(target: log_target, "IntentProcessorV2::AcknowledgementReceived for orderId - {orderId} from {sender} with result {result}");

    let transaction_hash = log.transaction_hash.unwrap().to_string();
    let block_number = log.block_number.unwrap() as i64;
    let order_id = orderId as i64; // its the order id
    let sender_address = sender.to_string();
    let result = result;
    let error_message = errorMessage;
    let timestamp = std::time::SystemTime::now();
    let ack_metadata: String = metadata.to_string();

    // fetching intent id
    let intent_id_query = "SELECT intent_id, token_out, source_chain_id, destination_chain_id, timestamp FROM order_created WHERE order_id = $1";
    let intent_id_response = match client.query_one(intent_id_query, &[&order_id]).await {
        Ok(row) => row,
        Err(e) => {
            error!(target: log_target, "Error in IntentProcessorV2::AcknowledgementReceived for order_id {:?}: {:?}", order_id, e);
            bail!(
                "Error in IntentProcessorV2::AcknowledgementReceived for order_id {:?}: {:?}",
                order_id,
                e
            );
        }
    };
    let intent_id: i64 = intent_id_response.get("intent_id");
    let token_out: String = intent_id_response.get("token_out");
    let source_chain_id: String = intent_id_response.get("source_chain_id");
    let destination_chain_id: String = intent_id_response.get("destination_chain_id");
    let order_timestamp: SystemTime = intent_id_response.get("timestamp");
    let _time = order_timestamp
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        .to_string();

    let query =
        "INSERT INTO acknowledgement VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (transaction_hash) DO NOTHING";
    let response = client
        .execute(
            query,
            &[
                &intent_id,
                &sender_address,
                &result,
                &error_message,
                &transaction_hash,
                &block_number,
                &timestamp,
                &order_id,
                &ack_metadata,
            ],
        )
        .await
        .unwrap();
    info!(target: log_target,
        "IntentProcessorV2::AcknowledgementReceived inserted response {:?}",
        response
    );

    let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

    let intent_stage = match result {
        true => &IntentStage::Done.to_string(),
        false => &IntentStage::Failed.to_string(),
    };

    update_intent_state(
        &intent_id,
        IntentVersions::AcknowledgementReceived as i32,
        intent_stage,
        log.transaction_hash.unwrap(),
        &client,
        chain_provider.clone(),
        &order_id,
        chain_id,
        initiator_address,
    )
    .await;

    info!("ack metadata to decode: {:?}", metadata);
    match SolidityAcknowledgementMetadata::abi_decode(metadata.as_ref(), true) {
        Ok(decoded_ack_metadata) => {
            if decoded_ack_metadata.data.len() > 0 {
                let metadata_variant = decoded_ack_metadata.enumVariant as u8;

                let actual_amount: String = if metadata_variant == 1 {
                    // stake
                    AcknowledgementMetadataStake::abi_decode(
                        decoded_ack_metadata.data.as_ref(),
                        true
                    )
                        .map(|data| data.amountCredited.to_string())
                        .map_err(|e| {
                            error!(target: log_target, "Failed to decode AcknowledgementMetadataStake: {:?}", e);
                            e
                        })
                        .unwrap_or_else(|_| "0".to_string())
                } else if metadata_variant == 2 {
                    AcknowledgementMetadataLaunchpadSwap::abi_decode(
                        decoded_ack_metadata.data.as_ref(),
                        true,
                    )
                    .map(|data| data.receivedAmount.to_string())
                    .map_err(|e| {
                        error!(target: log_target, "Failed to decode AcknowledgementMetadataLaunchpadSwap: {:?}", e);
                        e
                    })
                    .unwrap_or_else(|_| "0".to_string())
                } else if metadata_variant == 3 {
                    AcknowledgementMetadataLaunchpadAddLiquidity::abi_decode(
                        decoded_ack_metadata.data.as_ref(),
                        true,
                    )
                    .map(|data| data.amount1.to_string())
                    .map_err(|e| {
                        error!(target: log_target, "Failed to decode AcknowledgementMetadataLaunchpadAddLiquidity: {:?}", e);
                        e
                    })
                    .unwrap_or_else(|_| "0".to_string())
                } else if metadata_variant == 4 {
                    AcknowledgementMetadataLaunchpadRemoveLiquidity::abi_decode(
                        decoded_ack_metadata.data.as_ref(),
                        true,
                    )
                    .map(|data| data.amount1.to_string())
                    .map_err(|e| {
                        error!(target: log_target, "Failed to decode AcknowledgementMetadataLaunchpadRemoveLiquidity: {:?}", e);
                        e
                    })
                    .unwrap_or_else(|_| "0".to_string())
                } else {
                    // transact
                    if !result {
                        AcknowledgementMetadataTransactFailed::abi_decode(
                            &decoded_ack_metadata.data,
                            true,
                        )
                        .map(|data| data.amountCredited.to_string())
                        .map_err(|e| {
                            error!(target: log_target, "Failed to decode AcknowledgementMetadataTransactFailed: {:?}", e);
                            e
                        })
                        .unwrap_or_else(|_| "0".to_string())
                    } else {
                        AcknowledgementMetadataTransact::abi_decode(
                            decoded_ack_metadata.data.as_ref(),
                            true,
                        )
                        .map(|data| data.amount.to_string())
                        .map_err(|e| {
                            error!(target: log_target, "Failed to decode AcknowledgementMetadataTransact: {:?}", e);
                            e
                        })
                        .unwrap_or_else(|_| "0".to_string())
                    }
                };

                let update_order_query = "
                    UPDATE order_created
                    SET amount_out = $1, amount_out_usd = $2
                    WHERE order_id = $3
                    AND (SELECT COUNT(*) FROM order_created WHERE order_id = $3) = 1
                    ";

                // update amount_out_usd here as well in order_created
                let token_out_usd_price =
                    get_usd_value_of_token(Some(&token_out), &destination_chain_id, None).await;
                let token_out_decimals =
                    get_token_decimals(&token_out, &destination_chain_id).await;
                let amount_out_usd = ((actual_amount.parse::<f64>().unwrap_or(0.0)
                    / 10.0_f64.powf(token_out_decimals))
                    * token_out_usd_price)
                    .to_string();

                let order_rows_updated = client
                    .execute(
                        update_order_query,
                        &[&actual_amount, &amount_out_usd, &order_id],
                    )
                    .await
                    .unwrap_or(0);
                info!(target: log_target,
                    "updated actual amount for order, updated rows count {:?}",
                    order_rows_updated
                );
            }
        }
        Err(e) => {
            error!(target: log_target, "Failed to decode SolidityAcknowledgementMetadata: {:?}", e);
        }
    };

    Ok(())
}
