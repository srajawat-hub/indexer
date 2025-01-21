use std::{str::FromStr, sync::Arc};

use crate::{
    events::event_processor::{IntentStage, IntentVersions},
    solidity_structs::{SolidityVaultBoundMessage, VaultBoundMessagePlaceOrderData},
};

use super::BlockchainIndexer;
use alloy::dyn_abi::SolType;
use async_trait::async_trait;
use base64::Engine as Base64Engine;
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Local;
use log::{error, info};
use serde::{Deserialize, Serialize};
use solana_client::{
    client_error::reqwest, nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::VersionedTransaction};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::fmt::Debug;
use tokio_postgres::Client;

const MAX_LIMIT: u64 = 100;

pub struct SolanaIndexer {
    rpc_url: String,
    _ws_url: String,
    chain_id: i64,
    program_id: Pubkey,
}

pub async fn update_intent_state(
    intent_id: &i64,
    version: i32,
    stage: &str,
    transaction_hash: String,
    client: &Arc<Client>,
) {
    // let gas_fees = 1 as i64; // updating gas token
    let query = "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, DEFAULT)";
    let timestamp = std::time::SystemTime::now();

    let txn_hash_str = transaction_hash.to_string();
    let _intent_state_response = client
        .execute(
            query,
            &[
                &intent_id,
                &version,
                &txn_hash_str,
                &stage,
                &timestamp,
                &0i64,
            ],
        )
        .await
        .unwrap();
    info!("Intent State Updated for intent id: {intent_id} to version: {version}");
}

impl SolanaIndexer {
    pub fn new(rpc_url: String, ws_url: String, chain_id: i64, program_id: String) -> Self {
        Self {
            rpc_url,
            _ws_url: ws_url,
            chain_id,
            program_id: Pubkey::from_str(&program_id).unwrap(),
        }
    }
    /// Fetches all the events from the previous slot to the latest slot
    ///
    /// Fetches in batches and pushes all of them together
    pub async fn fetch_historical_transactions(
        &self,
        database_client: Arc<Client>,
        previously_fetched_slot: u64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let mut current_slot = 0;
        let mut last_searched_hash_val: Option<Signature> = None;
        let rpc_client = RpcClient::new(self.rpc_url.clone());
        let mut total_transactions = 0;
        while current_slot > (previously_fetched_slot - 10) {
            info!(
                "THis is last searched slot {:?} and last searched hash {:?}",
                current_slot, last_searched_hash_val
            );
            let sigs = rpc_client
                .get_signatures_for_address_with_config(
                    &self.program_id,
                    GetConfirmedSignaturesForAddress2Config {
                        // Start searching from this hash
                        before: last_searched_hash_val,
                        // Stops the search by checking the slot
                        until: None,
                        limit: Some(MAX_LIMIT as usize),
                        ..GetConfirmedSignaturesForAddress2Config::default()
                    },
                )
                .await
                .unwrap();
            if sigs.is_empty() {
                info!("Breaking because sigs length is 0");
                return Ok(());
            }
            // if last_searched_hash_val.is_none() {
            //     last_searched_hash_val = Some(sigs.first().unwrap().signature.to_string().clone());
            //     info!("latest hash is none {:?}", last_searched_hash_val);
            // }
            info!("Got signatures");
            let signatures_length = sigs.len();
            total_transactions += signatures_length;
            let last_sig = sigs[signatures_length - 1].clone();
            let mut body = vec![];
            for sig in sigs.clone() {
                let signature = sig.signature.clone();
                let payload = Payload {
                    jsonrpc: "2.0".to_string(),
                    id: 1,
                    method: "getTransaction".to_string(),
                    params: (
                        signature,
                        Param {
                            commitment: "confirmed".to_string(),
                            maxSupportedTransactionVersion: 0,
                            encoding: Some(UiTransactionEncoding::Base64),
                        },
                    ),
                };
                body.push(payload);
            }
            // Fetching multiple transaction data from signatures in a single RPC.
            // Since the rpc payload can be a array of objects, we can perform multiple
            // requests in a single RPC calls which is much much faster.
            let transactions: std::result::Result<Vec<Response>, reqwest::Error> =
                reqwest::blocking::Client::new()
                    .post(rpc_client.url())
                    .json(&body)
                    .send()?
                    .json();

            let transactions = transactions?;
            info!("Got transactions");
            for (index, tx) in transactions.iter().enumerate() {
                let logs = match tx.result.transaction.meta.clone().unwrap().log_messages {
                    solana_transaction_status::option_serializer::OptionSerializer::Some(e) => e,
                    _ => Vec::new(),
                };
                let events = get_events_from_logs(logs);
                let timestamp = if let Some(block_time) = tx.result.block_time {
                    block_time * 1000
                } else {
                    Local::now().timestamp()
                };
                let block_height = tx.result.slot as i64;
                let signatures = &tx.result.transaction.transaction;
                // let decoded_tx =
                let tx_decoded = match signatures {
                    solana_transaction_status::EncodedTransaction::Binary(data, _) => {
                        let decoded = base64::decode(data).expect("Invalid base64");
                        let tx_decoded = bincode::deserialize::<VersionedTransaction>(&decoded)
                            .expect("Invalid bincode");
                        tx_decoded
                    }
                    _ => panic!("Invalid type"),
                };
                let found_sig = tx_decoded.signatures[0].to_string();
                let transaction_hash = sigs[index].signature.clone();
                if found_sig != transaction_hash {
                    info!(
                        "Found sig {} from events {}\n all signatures {:?}",
                        found_sig,
                        sigs[index].signature.to_string(),
                        tx_decoded.signatures
                    );
                    info!("Signatures do not match");
                }
                let block_number = sigs[index].slot as i64;
                for event in events {
                    match event {
                        Event::MessageDispatchedFromVault(event) => {
                            let MessageDispatchedFromVaultEvent {
                                user_address,
                                destination_domain,
                                interop_provider,
                                message,
                            } = event;
                            info!(target: "solana_indexer", "Vault::MessageDispatchedFromVault received from {user_address:?} to destination {destination_domain}");
                            let message_slice = message.as_ref();
                            let decoded_message =
                                SolidityVaultBoundMessage::abi_decode(message_slice, true).unwrap();
                            let decoded_message_data = VaultBoundMessagePlaceOrderData::abi_decode(
                                decoded_message.data.as_ref(),
                                true,
                            )
                            .unwrap();

                            let intent_id = decoded_message_data.order.intentId as i64;
                            let order_id = decoded_message_data.order.orderId as i64;
                            let origin_domain_id = destination_domain as i32;
                            let sender_address =
                                decoded_message_data.order.initiatorAddress.to_string();
                            let provider = interop_provider as i32;
                            let tx_hash = sigs[index].signature.clone();

                            let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
                            let response = database_client
                                .execute(
                                    query,
                                    &[
                                        &intent_id,
                                        &origin_domain_id,
                                        &sender_address,
                                        &message,
                                        &provider,
                                        &tx_hash,
                                        &block_height,
                                        &timestamp,
                                        &self.chain_id,
                                        &order_id,
                                    ],
                                )
                                .await
                                .unwrap();
                            info!(
                                "Vault::ReceivedMessageOnVault inserted response {:?}",
                                response
                            );

                            update_intent_state(
                                &intent_id,
                                IntentVersions::ReceivedMessageOnVault as i32,
                                &IntentStage::Processing.to_string(),
                                tx_hash,
                                &database_client,
                            )
                            .await;
                        }
                        Event::ReceivedMessageOnVault(event) => {
                            let ReceivedMessageOnVaultEvent {
                                sender,
                                source_domain,
                                interop_provider,
                                message,
                            } = event;
                            info!(target: "solana_indexer", "Vault::ReceivedMessageOnVault by {sender:?}, message = {message:?}, using provider = {interop_provider:?}");
                            let message_slice = message.as_ref();
                            let decoded_message =
                                SolidityVaultBoundMessage::abi_decode(message_slice, true).unwrap();
                            let decoded_message_data = VaultBoundMessagePlaceOrderData::abi_decode(
                                decoded_message.data.as_ref(),
                                true,
                            )
                            .unwrap();

                            let intent_id = decoded_message_data.order.intentId as i64;
                            let order_id = decoded_message_data.order.orderId as i64;
                            let origin_domain_id = source_domain as i32;
                            let sender_address =
                                decoded_message_data.order.initiatorAddress.to_string();
                            let provider = interop_provider as i32;
                            let tx_hash = sigs[index].signature.clone();

                            let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
                            let response = database_client
                                .execute(
                                    query,
                                    &[
                                        &intent_id,
                                        &origin_domain_id,
                                        &sender_address,
                                        &message,
                                        &provider,
                                        &tx_hash,
                                        &block_number,
                                        &timestamp,
                                        &self.chain_id,
                                        &order_id,
                                    ],
                                )
                                .await
                                .unwrap();
                            info!(
                                "Vault::ReceivedMessageOnVault inserted response {:?}",
                                response
                            );

                            update_intent_state(
                                &intent_id,
                                IntentVersions::ReceivedMessageOnVault as i32,
                                &IntentStage::Processing.to_string(),
                                tx_hash,
                                &database_client,
                            )
                            .await;
                        }
                        _ => unimplemented!(),
                    };
                }
            }
            info!(
                "Total: {} , Latest hash: {:?}",
                total_transactions, last_searched_hash_val
            );
            // Fetches the last signatures from the batch of signatures which are fetched. Used for getting
            // the signatures before the signature below.
            last_searched_hash_val = Some(Signature::from_str(&last_sig.signature)?);
            current_slot = last_sig.slot;
        }
        Ok(())
    }
}

#[async_trait]
impl BlockchainIndexer for SolanaIndexer {
    async fn listen_for_events(
        &self,
        client: Arc<Client>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let latest_block_number: Option<i64> = {
            let query = "SELECT * FROM received_message_on_vault ORDER BY id DESC WHERE chain_id = $1 LIMIT 1";
            match client.query(query, &[&self.chain_id]).await {
                Ok(row) => {
                    if row.len() > 0 {
                        Some(row[0].get("block_number"))
                    } else {
                        None
                    }
                }
                Err(e) => {
                    error!("Error fetching latest block number: {:?}", e);
                    None
                }
            }
        };

        if let Some(latest_block_number) = latest_block_number {
            let block_number = latest_block_number as u64;
            // Fetch historical transactions backwards from the latest block number
            // Open a new thread to fetch historical transactions
            self.fetch_historical_transactions(client, block_number)
                .await?;
        }

        // Placeholder logic for listening to Solana program events
        info!(
            "Listening to events for Solana program {} on RPC {}",
            self.program_id, self.rpc_url
        );

        // Subscribe to logs

        // Simulating a stream of Solana events (replace with real logic using Solana SDK)
        let simulated_events = vec!["Event1", "Event2", "Event3"];
        for event in simulated_events {
            info!("Solana event: {}", event);
        }

        Ok(())
    }
}

/// Traverses the logs and extracts the events by deserializing them from base64
/// and then again using borsh.
pub fn get_events_from_logs(logs: Vec<String>) -> Vec<Event> {
    let serialized_events: Vec<&str> = logs
        .iter()
        .filter_map(|log| {
            if log.starts_with("Program data: ") {
                Some(log.strip_prefix("Program data: ").unwrap())
            } else {
                None
            }
        })
        .collect();
    let events: Vec<Event> = serialized_events
        .iter()
        .filter_map(|event| {
            let decoded_event = base64::prelude::BASE64_STANDARD.decode(event);
            if let Ok(decoded_event) = decoded_event {
                let decoded_event: Result<Event, String> =
                    borsh::BorshDeserialize::try_from_slice(&decoded_event).map_err(|e| {
                        // log::error!("These are logs {:?}", logs);
                        // log::error!("This is decoded event {:?}", decoded_event);
                        e.to_string()
                    });
                if let Ok(decoded_event) = decoded_event {
                    return Some(decoded_event);
                }
            }
            None
        })
        .collect();
    events
}

#[derive(Clone, Debug, borsh::BorshSerialize, borsh::BorshDeserialize)]
pub enum Event {
    DepositedFunds(DepositFundsEvent),
    DepositContractCreated(DepositContractCreatedEvent),
    MessageDispatchedFromVault(MessageDispatchedFromVaultEvent),
    ReceivedMessageOnVault(ReceivedMessageOnVaultEvent),
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DepositFundsEvent {
    // The 20 byte evm address of the user for whom the deposit is being made
    pub user_address: Vec<u8>,
    pub token_address: Pubkey,
    pub amount: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct DepositContractCreatedEvent {
    // The 20 byte evm address of the user for whom the deposit is being made
    pub user_address: Vec<u8>,
    // The derived deposit contract address
    pub derived_deposit_contract_address: Pubkey,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct MessageDispatchedFromVaultEvent {
    pub user_address: Vec<u8>,
    pub destination_domain: u32,
    pub interop_provider: LocalInteropProvider,
    pub message: Vec<u8>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct ReceivedMessageOnVaultEvent {
    pub sender: Vec<u8>,
    pub source_domain: u32,
    pub interop_provider: LocalInteropProvider,
    pub message: Vec<u8>,
}

// Defining a local interop provider so that it can be used to export as a crate
// and define traits for it.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub enum LocalInteropProvider {
    LayerZero,
    Hyperlane,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Payload {
    jsonrpc: String,
    id: u64,
    method: String,
    params: (String, Param),
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Param {
    commitment: String,
    maxSupportedTransactionVersion: u16,
    encoding: Option<UiTransactionEncoding>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    jsonrpc: String,
    id: u64,
    result: EncodedConfirmedTransactionWithStatusMeta,
}
