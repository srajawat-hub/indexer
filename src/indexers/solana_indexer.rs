use std::{
    str::FromStr,
    sync::Arc,
    thread::sleep,
    time::{Duration, SystemTime},
};

use crate::{
    events::event_processor::{DepositStatus, IntentStage, IntentVersions},
    solidity_structs::{
        IntentProcessorBoundMessageAcknowledgementData, IntentProcessorBoundMessageDepositData,
        SolidityIntentProcessorBoundMessage,
    },
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

const MAX_LIMIT: u64 = 20;

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
    order_id: &i64,
    chain_id: &i64,
    initiator_address: String,
    client: &Arc<Client>,
) {
    // let gas_fees = 1 as i64; // updating gas token
    let query = "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10)";
    let timestamp = std::time::SystemTime::now();

    let txn_hash_str = transaction_hash.to_string();
    let initiator_address_str = initiator_address.to_string();
    info!("Tx hash length {:?}", txn_hash_str.len());
    info!("Initiator address length {:?}", initiator_address_str.len());
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
                &"SOL".to_string(),
                &order_id,
                &chain_id,
                &initiator_address,
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
        info!("Fetching historical transactions");
        let mut last_searched_hash_val: Option<Signature> = None;
        let rpc_client = RpcClient::new(self.rpc_url.clone());
        let mut total_transactions = 0;
        let mut current_slot = rpc_client.get_slot().await.unwrap();
        let mut fetched_previous_slots = true;
        if current_slot < previously_fetched_slot {
            fetched_previous_slots = false;
        }
        let mut latest_tx = None;
        loop {
            sleep(Duration::from_secs(10));
            info!(
                "THis is last searched slot {:?} and last searched hash {:?}",
                current_slot, last_searched_hash_val
            );
            let sigs = match rpc_client
                .get_signatures_for_address_with_config(
                    &self.program_id,
                    GetConfirmedSignaturesForAddress2Config {
                        before: last_searched_hash_val,
                        until: latest_tx,
                        limit: Some(MAX_LIMIT as usize),
                        ..GetConfirmedSignaturesForAddress2Config::default()
                    },
                )
                .await
            {
                Ok(sigs) => sigs,
                Err(e) => {
                    error!("Error fetching signatures: {:?}", e);
                    continue;
                }
            };
            if sigs.is_empty() {
                info!("Breaking because sigs length is 0");
                current_slot = rpc_client.get_slot().await.unwrap();
                continue;
            }
            if last_searched_hash_val.is_none() {
                latest_tx = Some(Signature::from_str(&sigs.first().unwrap().signature).unwrap());
                info!("latest hash is {:?}", latest_tx);
            }
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
            let transactions = match reqwest::Client::new()
                .post(rpc_client.url())
                .json(&body)
                .send()
                .await
            {
                Ok(response) => match response.json::<Vec<Response>>().await {
                    Ok(tx) => tx,
                    Err(e) => {
                        error!("Error parsing transaction response: {:?}", e);
                        continue;
                    }
                },
                Err(e) => {
                    error!("Error sending transaction request: {:?}", e);
                    continue;
                }
            };

            info!("Got transactions");
            for (index, tx) in transactions.iter().enumerate() {
                if let Some(err) = tx.result.transaction.meta.clone().unwrap().err {
                    info!("Error in transaction {:?}", err);
                    continue;
                }
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
                let system_time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp as u64);
                let block_height = tx.result.slot as i64;
                let signatures = &tx.result.transaction.transaction;
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
                        Event::ReceivedMessageOnVault(event) => {
                            let ReceivedMessageOnVaultEvent {
                                sender,
                                source_domain,
                                interop_provider,
                                message,
                            } = event;
                            info!(target: "solana_indexer", "Vault::ReceivedMessageOnVault received from {sender:?} to source {source_domain}");

                            let order_id =
                                (u64::from_be_bytes(message[24..32].try_into().unwrap())) as i64;

                            let query =
                                "SELECT * FROM received_message_on_vault WHERE order_id = $1";
                            let response =
                                database_client.query(query, &[&order_id]).await.unwrap();
                            if response.len() > 0 {
                                continue;
                            }

                            let query = "SELECT * FROM order_created WHERE order_id = $1";
                            let response =
                                database_client.query(query, &[&order_id]).await.unwrap();

                            let (intent_id, sender_address) = match response.len() {
                                0 => (0i64, "".to_string()),
                                _ => (
                                    response[0].get("intent_id"),
                                    response[0].get("creator_address"),
                                ),
                            };

                            let origin_domain_id = source_domain as i32;
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
                                        &hex::encode(message),
                                        &provider,
                                        &tx_hash,
                                        &block_height,
                                        &system_time,
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
                                &i64::from(intent_id),
                                IntentVersions::ReceivedMessageOnVault as i32,
                                &IntentStage::Processing.to_string(),
                                tx_hash,
                                &order_id,
                                &self.chain_id,
                                sender_address,
                                &database_client,
                            )
                            .await;
                        }
                        Event::MessageDispatchedFromVault(event) => {
                            let MessageDispatchedFromVaultEvent {
                                user_address,
                                destination_domain,
                                interop_provider,
                                message,
                            } = event;
                            info!(target: "solana_indexer", "Vault::MessageDispatchedFromVault by {user_address:?}, message = {message:?}, using provider = {interop_provider:?}");
                            let message_slice = message.as_ref();
                            if message.is_empty() {
                                continue;
                            }
                            let decoded_message = SolidityIntentProcessorBoundMessage::abi_decode(
                                message_slice,
                                true,
                            )
                            .unwrap();
                            let decoded_message_data =
                                IntentProcessorBoundMessageAcknowledgementData::abi_decode(
                                    decoded_message.data.as_ref(),
                                    true,
                                );
                            let decoded_message_data = if let Ok(decoded_message_data) =
                                decoded_message_data
                            {
                                decoded_message_data
                            } else {
                                let decoded_message_data =
                                    IntentProcessorBoundMessageDepositData::abi_decode(
                                        decoded_message.data.as_ref(),
                                        true,
                                    );
                                error!("Error decoding message data: {:?}", decoded_message_data);
                                continue;
                            };

                            let order_id = decoded_message_data.orderId as i64;
                            let origin_domain_id = destination_domain as i32;
                            let provider = interop_provider as i32;
                            let tx_hash = sigs[index].signature.clone();

                            let query =
                                "SELECT * FROM message_dispatched_from_vault WHERE order_id = $1";
                            let response =
                                database_client.query(query, &[&order_id]).await.unwrap();
                            if response.len() > 0 {
                                continue;
                            }

                            // fetch intent_id
                            let intent_id_query =
                                "SELECT intent_id,creator_address FROM order_created WHERE order_id = $1";
                            let intent_id_response = database_client
                                .query(intent_id_query, &[&order_id])
                                .await
                                .unwrap();
                            info!("Intent length {:?}", intent_id_response.len());
                            info!("intent id response {:?}", intent_id_response);
                            let (intent_id, creator_address) = match intent_id_response.len() {
                                0 => (0i64, "".to_string()),
                                _ => (
                                    intent_id_response[0].get("intent_id"),
                                    intent_id_response[0].get("creator_address"),
                                ),
                            };

                            let query = "INSERT INTO message_dispatched_from_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9)";
                            let response = database_client
                                .execute(
                                    query,
                                    &[
                                        &intent_id,
                                        &creator_address,
                                        &origin_domain_id,
                                        &provider,
                                        &hex::encode(message),
                                        &transaction_hash,
                                        &block_number,
                                        &system_time,
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
                                IntentVersions::MessageDispatchedFromVault as i32,
                                &IntentStage::Processing.to_string(),
                                tx_hash,
                                &order_id,
                                &self.chain_id,
                                creator_address,
                                &database_client,
                            )
                            .await;
                        }
                        Event::DepositedFunds(event) => {
                            let DepositFundsEvent {
                                user_address,
                                token_address,
                                amount,
                                message_id,
                            } = event;
                            log::info!(target: "solana_indexer", "message_id from source {:?}", message_id);
                            log::info!(target: "solana_indexer", "user_address from source {:?}", user_address);

                             
                            let timestamp = std::time::SystemTime::now();
                            let status = DepositStatus::Initialized as i32;

                            let amount = amount as i64;

                            let query = "INSERT INTO deposit_received VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
                            let response = database_client
                                .execute(
                                    query,
                                    &[
                                        &user_address,
                                        &token_address,
                                        &self.chain_id,
                                        &amount,
                                        &timestamp,
                                        &transaction_hash.to_string(),
                                        &message_id,
                                        &status,
                                    ],
                                )
                                .await
                                .unwrap();
                            log::info!(target: "solana_indexer", "IntentLib::DepositedFunds inserted response {:?}", response);
                        }
                        _ => unimplemented!(),
                    };
                }
            }
            info!(
                "Total: {} , Latest hash: {:?}",
                total_transactions, last_searched_hash_val
            );
            if current_slot < (previously_fetched_slot - 10) || fetched_previous_slots {
                // If the current slot is less than the previously fetched slot, then we have fetched all the transactions
                last_searched_hash_val = None;
                current_slot = rpc_client.get_slot().await.unwrap();
                fetched_previous_slots = true;
            } else {
                // Fetches the last signatures from the batch of signatures which are fetched. Used for getting
                // the signatures before the signature below.
                last_searched_hash_val = Some(Signature::from_str(&last_sig.signature)?);
                current_slot = last_sig.slot;
            }
        }
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
            self.fetch_historical_transactions(client.clone(), block_number)
                .await?;
        }
        self.fetch_historical_transactions(client, 355876827)
            .await?;

        // Placeholder logic for listening to Solana program events
        info!(
            "Listening to events for Solana program {} on RPC {}",
            self.program_id, self.rpc_url
        );

        // Subscribe to logs

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
    let mut events: Vec<Event> = serialized_events
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
    // If the instruction is `transfer_funds_to_vault`, then we need to get the message_id from the data
    // and then we need to get the message from the message_id
    if logs.iter().any(|log| log.contains("TransferFundsToVault")) {
        let dispatch_message_log = logs
            .iter()
            .find(|log| log.contains("Dispatched message to"));
        if let Some(dispatch_message_log) = dispatch_message_log {
            let message_id = dispatch_message_log
                .split("Program log: Dispatched message to 18082, ID ")
                .nth(1)
                .unwrap();
            let message_id = message_id.to_string();
            // Also unpack the message that was sent.
            let deposit_event = match events.first() {
                Some(Event::MessageDispatchedFromVault(event)) => {
                    let message = event.message.clone();
                    let decoded_message =
                        SolidityIntentProcessorBoundMessage::abi_decode(message.as_ref(), true)
                            .unwrap();
                    let deposit_message = IntentProcessorBoundMessageDepositData::abi_decode(
                        decoded_message.data.as_ref(),
                        true,
                    )
                    .unwrap();
                    let deposit_event = DepositFundsEvent {
                        user_address: format!("0x{}", hex::encode(deposit_message.userAddress)),
                        token_address: format!("0x{}", hex::encode(deposit_message.tokenAddress)),
                        amount: u64::from_be_bytes(
                            deposit_message.amount.to_be_bytes::<32>()[24..32]
                                .try_into()
                                .unwrap(),
                        ),
                        message_id,
                    };
                    Some(deposit_event)
                }
                _ => None,
            };
            if let Some(deposit_event) = deposit_event {
                events.push(Event::DepositedFunds(deposit_event));
            }
        }
    }
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
    pub user_address: String,
    pub token_address: String,
    pub amount: u64,
    pub message_id: String,
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

#[tokio::test]
pub async fn test_get_events_from_logs() {
    let rpc_client = RpcClient::new("https://api.mainnet-beta.solana.com".to_string());
    let tx_hash = Signature::from_str("3BZfM9oJdmvP1MXNfCUToEmHQHwf5trV8vhGZrN4xnn7kdymcAotGLQsoosLx1L9qc7KAk4yktYZtzMmvzRhn5UH").expect("Failed to parse transaction hash");
    let logs = rpc_client.get_transaction(&tx_hash, UiTransactionEncoding::Base64).await.expect("Failed to get transaction logs");
    let logs = logs.transaction.meta.unwrap().log_messages;
    let logs = match logs {
        solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => logs,
        _ => Vec::new(),
    };
    let events = get_events_from_logs(logs);

    let user_address = "0xfCE5eEBff30d36121D965dcB862270D700f3b687";
    let token_address = "0xc6fa7af3bedbad3a3d65f36aabc97431b1bbe4c2d2f6e0e47ca60203452f5d61";
    let amount = 10000000;
    let message_id = "0x3466d9c67793ef637dd6beb3ec91a572295e5085b63ffbdb959bdb829b87dafd";

    // 2nd event is deposit event
    let deposit_event = events[1].clone();
    let deposit_event = match deposit_event {
        Event::DepositedFunds(event) => event,
        _ => panic!("Expected deposit event"),
    };
    assert_eq!(deposit_event.user_address.to_ascii_lowercase(), user_address.to_ascii_lowercase());
    assert_eq!(deposit_event.token_address.to_ascii_lowercase(), token_address.to_ascii_lowercase());
    assert_eq!(deposit_event.amount, amount);
    assert_eq!(deposit_event.message_id, message_id);
}