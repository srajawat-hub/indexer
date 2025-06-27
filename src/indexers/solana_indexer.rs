use std::{
    collections::HashMap,
    str::FromStr,
    sync::Arc,
    thread::sleep,
    time::{Duration, SystemTime},
};
use rust_decimal::{prelude::FromPrimitive, Decimal};

use crate::{
    events::event_processor::{DepositStatus, IntentStage, IntentVersions},
    skip_fail,
    solidity_structs::{
        IntentProcessorBoundMessageAcknowledgementData, IntentProcessorBoundMessageDepositData, PoolType, SolidityIntentProcessorBoundMessage, TokenLaunchType
    },
};

use super::BlockchainIndexer;
use crate::indexers::raydium_events::{
    AmmConfig, CollectPersonalFeeEvent, CollectProtocolFeeEvent, ConfigChangeEvent,
    CreatePersonalPositionEvent, DecreaseLiquidityEvent, IncreaseLiquidityEvent, LaunchType,
    PoolCreatedEvent, PoolCreatedEventWithState, PoolState, SwapEvent, AMM_CONFIG_SEED,
};
use crate::utils::unix_to_system_time;
use alloy::dyn_abi::SolType;
use anchor_lang::{AccountDeserialize, Discriminator};
use async_trait::async_trait;
use base64::prelude::BASE64_STANDARD;
use base64::Engine as Base64Engine;
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Local;
use log::{debug, error, info, warn, LevelFilter};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use serde::{Deserialize, Serialize};
use solana_client::rpc_response::RpcConfirmedTransactionStatusWithSignature;
use solana_client::{
    client_error::reqwest, nonblocking::rpc_client::RpcClient,
    rpc_client::GetConfirmedSignaturesForAddress2Config,
};
use solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::VersionedTransaction};
use solana_transaction_status::{EncodedConfirmedTransactionWithStatusMeta, UiTransactionEncoding};
use std::fmt::{Debug, Display};
use tokio_postgres::Client;

const MAX_LIMIT: u64 = 20;

pub struct SolanaIndexer {
    rpc_url: String,
    _ws_url: String,
    chain_id: i64,
    vaults_program_id: Pubkey,
    amm_program_id: Pubkey,
    rpc_client: RpcClient,
}

fn get_native_token_symbol(chain_id: i64) -> (String, u32) {
    match chain_id {
        137 => (String::from("POL"), 18),
        1399811149 => (String::from("SOL"), 9),
        18082 => (String::from("USDC"), 18),
        _ => (String::from("ETH"), 18),
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: HashMap<String, Vec<TokenInfo>>,
}

#[derive(Debug, Deserialize)]
struct TokenInfo {
    id: u64,
    name: String,
    symbol: String,
    quote: HashMap<String, Quote>,
}

#[derive(Debug, Deserialize)]
struct Quote {
    price: Option<f64>,
    market_cap: Option<f64>,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
enum ProgramName {
    Vault,
    Raydium,
}

impl Display for ProgramName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProgramName::Vault => write!(f, "Vault"),
            ProgramName::Raydium => write!(f, "Raydium"),
        }
    }
}

pub async fn get_usd_value_of_native(chain_id: &i64, transaction_cost: &u128) -> String {
    let cmc_api_key = std::env::var("CMC_API_KEY")
        .expect("CMC_API_KEY must be set")
        .parse::<String>()
        .unwrap();

    let (token_symbol, token_decimals) = get_native_token_symbol(*chain_id);

    let cmc_api = format!(
        "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?symbol={}",
        token_symbol
    );
    let mut headers = HeaderMap::new();
    match HeaderValue::from_str(&cmc_api_key) {
        Ok(header_value) => {
            headers.insert("X-CMC_PRO_API_KEY", header_value);
        }
        Err(e) => {
            error!("Failed to get USD values from CMC, defaulting to 0. Invalid header value for CMC api: {}", e);
            return String::from("0"); // or handle the error accordingly
        }
    }

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let api_client = reqwest::Client::new();
    let mut transaction_fees_usd = String::from("0");
    return transaction_fees_usd;

    let response_result = api_client.get(&cmc_api).headers(headers).send().await;

    match response_result {
        Ok(response) => {
            let json_result = response.json::<ApiResponse>().await;
            match json_result {
                Ok(api_response) => {
                    if let Some(tokens) = api_response.data.get(&token_symbol) {
                        if let Some(first_token) = tokens.first() {
                            if let Some(quote) = first_token.quote.get("USD") {
                                match quote.price {
                                    Some(price) => {
                                        info!("Price of {}: ${}", token_symbol, price);
                                        transaction_fees_usd = ((*transaction_cost as f64
                                            / 10_f64.powf(token_decimals as f64))
                                            * price)
                                            .to_string()
                                    }
                                    None => warn!("Price not available for {}", token_symbol),
                                }
                            } else {
                                info!("USD quote not available.");
                            }
                        } else {
                            info!("No token info found.");
                        }
                    } else {
                        info!("No data found in response.");
                    }
                }
                Err(e) => {
                    info!("Failed to parse JSON: {}", e);
                }
            }
        }
        Err(e) => {
            info!("Request failed: {}", e);
        }
    };

    transaction_fees_usd
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
    compute_units_consumed: &i64,
    transaction_cost: &str,
) {
    // let gas_fees = 1 as i64; // updating gas token
    let query = "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (intent_id, version, transaction_hash) DO NOTHING";
    let timestamp = std::time::SystemTime::now();

    let transaction_cost_usd =
        get_usd_value_of_native(chain_id, &transaction_cost.parse::<u128>().unwrap()).await;

    let initiator_address_str = initiator_address.to_string();
    info!("Tx hash length {:?}", transaction_hash.len());
    info!("Initiator address length {:?}", initiator_address_str.len());

    let _intent_state_response = client
        .execute(
            query,
            &[
                &intent_id,
                &version,
                &transaction_hash,
                &stage,
                &timestamp,
                compute_units_consumed,
                &"SOL".to_string(),
                &order_id,
                &chain_id,
                &initiator_address,
                &transaction_cost,
                &transaction_cost_usd,
            ],
        )
        .await
        .unwrap();
    info!("Intent State Updated for intent id: {intent_id} to version: {version}");
}

impl SolanaIndexer {
    pub fn new(
        rpc_url: String,
        ws_url: String,
        chain_id: i64,
        vaults_program_id: String,
        amm_program_id: String,
    ) -> Self {
        let rpc_client = RpcClient::new(rpc_url.clone());
        Self {
            rpc_url,
            _ws_url: ws_url,
            chain_id,
            vaults_program_id: Pubkey::from_str(&vaults_program_id).unwrap(),
            amm_program_id: Pubkey::from_str(&amm_program_id).unwrap(),
            rpc_client,
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
        info!("Starting continuous transaction indexer from slot {} onwards", previously_fetched_slot);

        // Process vault and raydium programs separately in parallel
        let vault_task = {
            let database_client = database_client.clone();
            let vault_program_id = self.vaults_program_id;
            let amm_program_id = self.amm_program_id;
            let rpc_url = self.rpc_url.clone();
            let chain_id = self.chain_id;
            tokio::spawn(async move {
                loop {
                    let result = Self::process_program_transactions(
                        database_client.clone(),
                        rpc_url.clone(),
                        vault_program_id,
                        amm_program_id,
                        ProgramName::Vault,
                        previously_fetched_slot,
                        chain_id,
                    ).await;
                    if let Err(e) = result {
                        error!("Error processing vault transactions: {:?}", e);
                    }
                }
            })
        };

        let raydium_task = {
            let database_client = database_client.clone();
            let amm_program_id = self.amm_program_id;
            let vault_program_id = self.vaults_program_id;
            let rpc_url = self.rpc_url.clone();
            let chain_id = self.chain_id;
            tokio::spawn(async move {
                loop {
                    let result = Self::process_program_transactions(
                        database_client.clone(),
                        rpc_url.clone(),
                        amm_program_id,
                        vault_program_id,
                        ProgramName::Raydium,
                        previously_fetched_slot,
                        chain_id,
                    ).await;
                    if let Err(e) = result {
                        error!("Error processing raydium transactions: {:?}", e);
                    }
                }
            })
        };

        // Wait for both tasks to complete
        let _ = tokio::try_join!(vault_task, raydium_task)?;

        Ok(())
    }

    /// Process transactions for a specific program
    async fn process_program_transactions(
        database_client: Arc<Client>,
        rpc_url: String,
        program_id: Pubkey,
        other_program_id: Pubkey, // needed for parsing events
        program_name: ProgramName,
        previously_fetched_slot: u64,
        chain_id: i64,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let rpc_client = RpcClient::new(rpc_url);
        let mut current_slot = previously_fetched_slot;

        // Get current blockchain slot
        let current_blockchain_slot = rpc_client.get_slot().await?;
        let mut is_catching_up = current_slot < current_blockchain_slot.saturating_sub(1000);
        let mut until_sig = None;

        info!("[{}] Starting from slot: {}, current blockchain slot: {}, catching up: {}",
              program_name, current_slot, current_blockchain_slot, is_catching_up);

        // Phase 1: Backfill historical transactions if catching up
        if is_catching_up {
            info!("[{}] Starting backfill from latest slot {} down to slot {}",
                  program_name, current_blockchain_slot, previously_fetched_slot);

            let mut all_historical_sigs = Vec::new();
            let mut before_sig = None; // Start from latest

            // Fetch all signatures backwards until we reach previously_fetched_slot or get empty results
            loop {
                let sigs = match rpc_client
                    .get_signatures_for_address_with_config(
                        &program_id,
                        GetConfirmedSignaturesForAddress2Config {
                            before: before_sig,
                            until: None, // No until limit, we'll filter manually
                            limit: Some(MAX_LIMIT as usize),
                            ..GetConfirmedSignaturesForAddress2Config::default()
                        },
                    )
                    .await
                {
                    Ok(sigs) => sigs,
                    Err(e) => {
                        error!("[{}] Error fetching signatures during backfill: {:?}", program_name, e);
                        break;
                    }
                };

                if sigs.is_empty() {
                    info!("[{}] Reached end of signatures during backfill", program_name);
                    break;
                }

                // Filter signatures to only include those > previously_fetched_slot
                let filtered_sigs: Vec<_> = sigs
                    .into_iter()
                    .filter(|sig| sig.slot > previously_fetched_slot)
                    .collect();

                if filtered_sigs.is_empty() {
                    info!("[{}] Reached previously fetched slot {} during backfill",
                          program_name, previously_fetched_slot);
                    break;
                }

                info!("[{}] Fetched {} signatures for backfill (slots {} to {})",
                      program_name, filtered_sigs.len(),
                      filtered_sigs.last().unwrap().slot,
                      filtered_sigs.first().unwrap().slot);

                if until_sig.is_none() {
                    until_sig = Some(Signature::from_str(&filtered_sigs.first().unwrap().signature.clone())?);
                }
                // Update before_sig for next iteration (last signature chronologically)
                before_sig = Some(skip_fail!(Signature::from_str(&filtered_sigs.last().unwrap().signature)));

                // Add to our accumulating list
                all_historical_sigs.extend(filtered_sigs);

                // If we got less than MAX_LIMIT, we might have reached the end
                if all_historical_sigs.len() < MAX_LIMIT as usize {
                    break;
                }
            }
            all_historical_sigs.reverse();

            if !all_historical_sigs.is_empty() {
                info!("[{}] Processing {} historical signatures from backfill (slots {} to {})",
                      program_name, all_historical_sigs.len(),
                      all_historical_sigs.first().unwrap().slot,
                      all_historical_sigs.last().unwrap().slot);

                // Process all historical transactions at once
                let processed = Self::process_transaction_batch_for_program(
                    &database_client,
                    &rpc_client,
                    &program_id,
                    &other_program_id,
                    program_name,
                    all_historical_sigs.clone(),
                    chain_id,
                ).await?;

                info!("[{}] Processed {} historical transactions during backfill",
                      program_name, processed);

                // Update current_slot to the latest processed transaction
                if let Some(latest_sig) = all_historical_sigs.last() {
                    current_slot = latest_sig.slot;
                    info!("[{}] Updated position to slot: {} after backfill", program_name, current_slot);

                    // Update chain_metadata with latest processed block from backfill
                    if let Err(e) = Self::update_chain_metadata_latest_block(
                        &database_client,
                        chain_id,
                        current_slot,
                        program_name,
                    ).await {
                        error!("[{}] Failed to update chain metadata after backfill: {:?}", program_name, e);
                    }
                }
            }

            // Switch to real-time mode
            is_catching_up = false;
            info!("[{}] Backfill complete, switching to real-time mode", program_name);
        }

        // Phase 2: Real-time processing loop
        if until_sig.is_none() {
            until_sig = Self::get_last_signature_at_slot(&rpc_client, &program_id, current_slot).await;
        }

        loop {
            sleep(Duration::from_secs(10));

            // Get current blockchain slot
            let latest_slot = rpc_client.get_slot().await?;

            info!("[{}] Real-time check: current position {}, latest slot: {}",
                  program_name, current_slot, latest_slot);

            // Fetch new signatures since our last position
            let mut sigs = match rpc_client
                .get_signatures_for_address_with_config(
                    &program_id,
                    GetConfirmedSignaturesForAddress2Config {
                        before: None, // Get latest
                        until: until_sig,
                        limit: Some(MAX_LIMIT as usize),
                        ..GetConfirmedSignaturesForAddress2Config::default()
                    },
                )
                .await
            {
                Ok(sigs) => sigs,
                Err(e) => {
                    error!("[{}] Error fetching signatures in real-time: {:?}", program_name, e);
                    continue;
                }
            };
            sigs.reverse();

            if sigs.is_empty() {
                debug!("[{}] No new transactions, waiting...", program_name);
                continue;
            }

            info!("[{}] Processing {} new signatures (slots {} to {})",
                  program_name, sigs.len(),
                  sigs.first().unwrap().slot,
                  sigs.last().unwrap().slot);

            // Process new transactions
            let processed = Self::process_transaction_batch_for_program(
                &database_client,
                &rpc_client,
                &program_id,
                &other_program_id,
                program_name,
                sigs.clone(),
                chain_id,
            ).await?;

            info!("[{}] Processed {} new transactions", program_name, processed);

            // Update our position to the newest transaction
            if let Some(newest_sig) = sigs.last() {
                current_slot = newest_sig.slot;
                until_sig = Some(skip_fail!(Signature::from_str(&newest_sig.signature)));
                info!("[{}] Updated position to slot: {}", program_name, current_slot);

                // Update chain_metadata with latest processed block
                if let Err(e) = Self::update_chain_metadata_latest_block(
                    &database_client,
                    chain_id,
                    current_slot,
                    program_name,
                ).await {
                    error!("[{}] Failed to update chain metadata: {:?}", program_name, e);
                }
            }
        }
    }

    /// Process a batch of transactions for a specific program
    async fn process_transaction_batch_for_program(
        database_client: &Arc<Client>,
        rpc_client: &RpcClient,
        program_id: &Pubkey,
        other_program_id: &Pubkey,
        program_name: ProgramName,
        sigs: Vec<RpcConfirmedTransactionStatusWithSignature>,
        chain_id: i64,
    ) -> Result<usize, Box<dyn std::error::Error>> {
        if sigs.is_empty() {
            return Ok(0);
        }
        // dbg!(program_name, &sigs);

        // Build RPC request body
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

        // Fetch transaction data
        let transactions = reqwest::Client::new()
            .post(rpc_client.url())
            .json(&body)
            .send()
            .await?
            .json::<Vec<Response>>()
            .await?;

        let mut processed_count = 0;
        for (index, tx) in transactions.iter().enumerate() {
            if let Some(err) = tx.result.transaction.meta.clone().unwrap().err {
                info!("[{}] Skipping failed transaction: {:?}", program_name, err);
                continue;
            }

            let logs = match tx.result.transaction.meta.clone().unwrap().log_messages {
                solana_transaction_status::option_serializer::OptionSerializer::Some(e) => e,
                _ => Vec::new(),
            };
            let logs = logs.iter().map(|x| x.as_ref()).collect::<Vec<&str>>();

            let timestamp = if let Some(block_time) = tx.result.block_time {
                block_time * 1000
            } else {
                Local::now().timestamp()
            };

            let (tx_cost, compute_units_used) = match &tx.result.transaction.meta {
                Some(metadata) => {
                    let tx_cost = metadata.fee.to_string();
                    let compute_units =
                        metadata.compute_units_consumed.clone().unwrap_or(0) as i64;
                    (tx_cost, compute_units)
                }
                None => (String::from("0"), 0i64),
            };

            let system_time = SystemTime::UNIX_EPOCH + Duration::from_millis(timestamp as u64);
            let block_height = tx.result.slot as i64;
            let signatures = &tx.result.transaction.transaction;
            let tx_decoded = match signatures {
                solana_transaction_status::EncodedTransaction::Binary(data, _) => {
                    let decoded = BASE64_STANDARD.decode(data).expect("Invalid base64");
                    let tx_decoded = bincode::deserialize::<VersionedTransaction>(&decoded)
                        .expect("Invalid bincode");
                    tx_decoded
                }
                _ => {
                    error!("[{}] Invalid transaction encoding type", program_name);
                    continue;
                }
            };
            let found_sig = tx_decoded.signatures[0].to_string();
            let transaction_hash = sigs[index].signature.clone();
            if found_sig != transaction_hash {
                info!(
                    "[{}] Signature mismatch: found {} vs expected {}",
                    program_name, found_sig, transaction_hash
                );
            }
            let block_number = sigs[index].slot as i64;

            // Create a temporary SolanaIndexer instance to use the parse_solana_events method
            let temp_indexer = SolanaIndexer {
                rpc_url: rpc_client.url().to_string(),
                _ws_url: "".to_string(),
                chain_id,
                vaults_program_id: *other_program_id,
                amm_program_id: *program_id,
                rpc_client: RpcClient::new(rpc_client.url().to_string()),
            };

            // Use the unified parse_solana_events method for both vault and raydium events
            let all_events = temp_indexer.parse_solana_events(&logs, program_id, other_program_id).await?;

            // Process events based on their type
            for events in all_events {
                match events {
                    Events::Vaults(vault_events) if program_name == ProgramName::Vault => {
                        temp_indexer.process_vaults_events(
                            database_client,
                            &mut sigs.clone(),
                            index,
                            &tx_cost,
                            &compute_units_used,
                            system_time,
                            block_height,
                            transaction_hash.clone(),
                            block_number,
                            vault_events,
                        ).await;
                    }
                    Events::Raydium(raydium_events) if program_name == ProgramName::Raydium => {
                        temp_indexer.process_raydium_events(
                            database_client,
                            &mut sigs.clone(),
                            index,
                            &tx_cost,
                            compute_units_used,
                            system_time,
                            block_height,
                            transaction_hash.clone(),
                            block_number,
                            raydium_events,
                        ).await;
                    }
                    Events::Vaults(_) | Events::Raydium(_) => (),
                }
            }

            processed_count += 1;
        }

        Ok(processed_count)
    }

    /// Update the latest block number in chain_metadata table
    async fn update_chain_metadata_latest_block(
        database_client: &Arc<Client>,
        chain_id: i64,
        latest_block: u64,
        program_name: ProgramName,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let query = r#"
            INSERT INTO chain_metadata (chain_id, network_name, latest_block)
            VALUES ($1, $2, $3)
            ON CONFLICT (chain_id)
            DO UPDATE SET latest_block = EXCLUDED.latest_block
        "#;

        let network_name = match chain_id {
            1399811149 => "solana-mainnet",
            _ => "unknown",
        };

        match database_client
            .execute(query, &[&chain_id.to_string(), &network_name, &(latest_block as i64)])
            .await
        {
            Ok(_) => {
                debug!("[{}] Updated chain_metadata latest_block to {} for chain_id {}",
                       program_name, latest_block, chain_id);
                Ok(())
            }
            Err(e) => {
                error!("[{}] Failed to update chain_metadata: {:?}", program_name, e);
                Err(Box::new(e))
            }
        }
    }

    /// Get a signature at or near a specific slot for a specific program
    async fn get_last_signature_at_slot(
        rpc_client: &RpcClient,
        _program_id: &Pubkey,
        slot: u64
    ) -> Option<Signature> {
        let block = rpc_client
            .get_block(slot)
            .await
            .ok()?;

        // Get the first transaction in the block
        let first_tx = block.transactions.last()?;

        // Extract the signature from the transaction
        match &first_tx.transaction {
            solana_transaction_status::EncodedTransaction::Json(ui_tx) => {
                // For JSON encoded transactions, get the first signature
                ui_tx.signatures.first().and_then(|sig_str| {
                    Signature::from_str(sig_str).ok()
                })
            }
            solana_transaction_status::EncodedTransaction::Binary(data, _encoding) => {
                // For binary encoded transactions, decode and extract signature
                let decoded = BASE64_STANDARD.decode(data).ok()?;
                let tx_decoded = bincode::deserialize::<VersionedTransaction>(&decoded).ok()?;
                tx_decoded.signatures.first().copied()
            }
            _ => None,
        }
    }

    async fn process_vaults_events(
        &self,
        database_client: &Arc<Client>,
        sigs: &mut Vec<RpcConfirmedTransactionStatusWithSignature>,
        index: usize,
        tx_cost: &String,
        compute_units_used: &i64,
        system_time: SystemTime,
        block_height: i64,
        transaction_hash: String,
        block_number: i64,
        events: Vec<VaultsEvent>,
    ) {
        for event in events {
            match event {
                VaultsEvent::ReceivedMessageOnVault(event) => {
                    let ReceivedMessageOnVaultEvent {
                        sender,
                        source_domain,
                        interop_provider,
                        message,
                    } = event;
                    info!(target: "solana_indexer", "Vault::ReceivedMessageOnVault received from {sender:?} to source {source_domain}");

                    let order_id =
                        u64::from_be_bytes(skip_fail!(message[24..32].try_into())) as i64;

                    let query = "SELECT * FROM received_message_on_vault WHERE order_id = $1";
                    let response = skip_fail!(database_client.query(query, &[&order_id]).await);
                    if response.len() > 0 {
                        continue;
                    }

                    let query = "SELECT * FROM order_created WHERE order_id = $1";
                    let response = skip_fail!(database_client.query(query, &[&order_id]).await);

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

                    let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10) ON CONFLICT (transaction_hash) DO NOTHING";
                    let response = skip_fail!(
                        database_client
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
                    );
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
                        &compute_units_used,
                        &tx_cost,
                    )
                    .await;
                }
                VaultsEvent::MessageDispatchedFromVault(event) => {
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
                    let decoded_message = skip_fail!(
                        SolidityIntentProcessorBoundMessage::abi_decode(message_slice, true,)
                    );
                    let decoded_message_data =
                        IntentProcessorBoundMessageAcknowledgementData::abi_decode(
                            decoded_message.data.as_ref(),
                            true,
                        );
                    let decoded_message_data =
                        if let Ok(decoded_message_data) = decoded_message_data {
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

                    let query = "SELECT * FROM message_dispatched_from_vault WHERE order_id = $1";
                    let response = skip_fail!(database_client.query(query, &[&order_id]).await);
                    if response.len() > 0 {
                        continue;
                    }

                    // fetch intent_id
                    let intent_id_query =
                        "SELECT intent_id,creator_address FROM order_created WHERE order_id = $1";
                    let intent_id_response =
                        skip_fail!(database_client.query(intent_id_query, &[&order_id]).await);
                    info!("Intent length {:?}", intent_id_response.len());
                    info!("intent id response {:?}", intent_id_response);
                    let (intent_id, creator_address) = match intent_id_response.len() {
                        0 => (0i64, "".to_string()),
                        _ => (
                            intent_id_response[0].get("intent_id"),
                            intent_id_response[0].get("creator_address"),
                        ),
                    };

                    let query = "INSERT INTO message_dispatched_from_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (transaction_hash) DO NOTHING";
                    let response = skip_fail!(
                        database_client
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
                    );
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
                        &compute_units_used,
                        &tx_cost,
                    )
                    .await;
                }
                VaultsEvent::DepositedFunds(event) => {
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

                    let amount = amount.to_string();
                    let chain_id = self.chain_id.to_string();

                    let query = "INSERT INTO deposit_received VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
                    let response = skip_fail!(
                        database_client
                            .execute(
                                query,
                                &[
                                    &user_address,
                                    &token_address,
                                    &chain_id,
                                    &amount,
                                    &timestamp,
                                    &transaction_hash,
                                    &message_id,
                                    &status,
                                ],
                            )
                            .await
                    );
                    log::info!(target: "solana_indexer", "IntentLib::DepositedFunds inserted response {:?}", response);
                }
                _ => unimplemented!(),
            }
        }
    }

    async fn process_raydium_events(
        &self,
        database_client: &Arc<Client>,
        sigs: &mut Vec<RpcConfirmedTransactionStatusWithSignature>,
        index: usize,
        tx_cost: &str,
        compute_units_used: i64,
        system_time: SystemTime,
        block_height: i64,
        _transaction_hash: String,
        _block_number: i64,
        events: Vec<RaydiumEvent>,
    ) {
        for event in events {
            if let Err(e) = self
                .process_raydium_event(
                    database_client.clone(),
                    sigs,
                    index,
                    &tx_cost,
                    compute_units_used,
                    system_time,
                    block_height,
                    event,
                )
                .await
            {
                error!(target: "solana_indexer", "Error in process_raydium_event: {:?}", e);
            }
        }
    }

    async fn process_raydium_event(
        &self,
        database_client: Arc<Client>,
        sigs: &mut Vec<RpcConfirmedTransactionStatusWithSignature>,
        index: usize,
        _tx_cost: &str,
        _compute_units_used: i64,
        system_time: SystemTime,
        block_height: i64,
        event: RaydiumEvent,
    ) -> anyhow::Result<()> {
        match event {
            RaydiumEvent::PoolCreatedEvent(event) => {
                let PoolCreatedEventWithState {
                    inner:
                        PoolCreatedEvent {
                            token_mint_0,
                            token_mint_1,
                            tick_spacing,
                            pool_state: pool_state_pk,
                            sqrt_price_x64,
                            tick,
                            token_vault_0: _,
                            token_vault_1: _,
                            fee_tier_index,
                        },
                    pool_state,
                    amm_config,
                } = event;
                info!(target: "solana_indexer", "RaydiumEvent::PoolCreatedEvent received on {pool_state_pk}");

                let pool_address = pool_state_pk.to_string();
                let chain_id_i64 = self.chain_id;
                let token0_addr = token_mint_0.to_string();
                let token1_addr = token_mint_1.to_string();

                let fee = Decimal::from(amm_config.fee_tiers[fee_tier_index as usize].trade_fee_rate as i32);
                let tick_spacing_i64 = tick_spacing as i64;
                let pool_type = PoolType::SOLANA;
                let project_manager = pool_state.project_liquidity_provider.to_string();
                let timestamp = system_time;
                let metadata_json = serde_json::Value::Null;

                let etp_start_time =
                    unix_to_system_time(pool_state.exclusive_trading_period_start_time);
                let etp_close_time =
                    unix_to_system_time(pool_state.exclusive_trading_period_end_time);
                let launch_type = if pool_state.launch_type == 0 {
                    TokenLaunchType::FAIR
                } else {
                    TokenLaunchType::CURATED
                };
                let initial_sqrt = sqrt_price_x64.to_string();
                let initial_tick_i32 = tick;
                let reserve_token_mint =
                    pool_state.get_reserve_token_mint(&token_mint_0, &token_mint_1);
                let reserve_token_supply =
                    self.rpc_client.get_token_supply(reserve_token_mint).await?;
                let token_supply_i64 = reserve_token_supply.amount.to_string();

                let query = r#"
                    INSERT INTO pools
                        (pool_address, chain_id, token_0_address, token_1_address, fee,
                         tick_spacing, pool_type, project_manager, block_number, created_at,
                         metadata, etp_start_time, etp_end_time, launch_type, initial_sqrt_price,
                         initial_tick, token_supply, launchpad_token)
                    VALUES
                        ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17,$18)
                    ON CONFLICT (pool_address) DO NOTHING
                "#;

                let response = database_client
                    .execute(
                        query,
                        &[
                            &pool_address,
                            &chain_id_i64,
                            &token0_addr,
                            &token1_addr,
                            &fee,
                            &tick_spacing_i64,
                            &pool_type,
                            &project_manager,
                            &block_height,
                            &timestamp,
                            &metadata_json,
                            &etp_start_time,
                            &etp_close_time,
                            &launch_type,
                            &initial_sqrt,
                            &initial_tick_i32,
                            &token_supply_i64,
                            &reserve_token_mint.to_string()
                        ],
                    )
                    .await?;
                info!(target: "solana_indexer", "RaydiumEvent::PoolCreatedEvent inserted response {:?}", response);
            }
            RaydiumEvent::SwapEvent(event) => {
                let SwapEvent {
                    pool_state: pool_state_pk,
                    sender,
                    token_account_0: _,
                    token_account_1: _,
                    amount_0,
                    transfer_fee_0: _,
                    amount_1,
                    transfer_fee_1,
                    zero_for_one,
                    sqrt_price_x64,
                    liquidity,
                    tick,
                    via_vault,
                } = event;
                info!(target: "solana_indexer", "RaydiumEvent::SwapEvent received from {sender:?} on {pool_state_pk}");
                let pool_address = pool_state_pk.to_string();
                let transaction_hash = sigs[index].signature.clone();
                let block_number = block_height;
                let timestamp = system_time;
                let sqrt_price = sqrt_price_x64.to_string();
                let liquidity_i64 = liquidity as i64;
                let initiator_user_address = sender.to_string();
                let chain_id_i64 = self.chain_id;

                // Query pool to get token addresses
                let pool_query =
                    "SELECT token_0_address, token_1_address FROM pools WHERE pool_address = $1";
                let pool_row = database_client
                    .query_one(pool_query, &[&pool_address])
                    .await?;
                let token0_address: String = pool_row.get("token_0_address");
                let token1_address: String = pool_row.get("token_1_address");

                // Determine token flow based on zero_for_one direction
                // Include transfer fees in the amounts
                let (token_in, token_out, amount_in, amount_out) = if zero_for_one {
                    // Swapping token_0 for token_1
                    (
                        token0_address,
                        token1_address,
                        amount_0.to_string(),
                        amount_1.to_string(),
                    )
                } else {
                    // Swapping token_1 for token_0
                    (
                        token1_address,
                        token0_address,
                        (amount_1 + transfer_fee_1).to_string(), // Include transfer fee
                        amount_0.to_string(),
                    )
                };

                // Calculate price if amounts are valid
                let price = if let (Ok(amt_in), Ok(amt_out)) =
                    (amount_in.parse::<f64>(), amount_out.parse::<f64>())
                {
                    if amt_in > 0.0 {
                        Some(amt_out / amt_in)
                    } else {
                        None
                    }
                } else {
                    None
                };
                let is_vault_initiated = via_vault;

                // Calculate USD values for amounts
                // For Solana, we currently only have SOL/XXX pairs - determine which token is native
                let sol_token_address = spl_token::native_mint::id().to_string();
                let native_sol_address = "11111111111111111111111111111111";

                let (native_token, _other_token, native_amount, other_amount) =
                    if token_in == sol_token_address || token_in == native_sol_address {
                        (
                            token_in.clone(),
                            token_out.clone(),
                            amount_in.clone(),
                            amount_out.clone(),
                        )
                    } else if token_out == sol_token_address || token_out == native_sol_address {
                        (
                            token_out.clone(),
                            token_in.clone(),
                            amount_out.clone(),
                            amount_in.clone(),
                        )
                    } else {
                        // If neither token is SOL, we can't calculate USD values accurately
                        (
                            "0".to_string(),
                            "0".to_string(),
                            "0".to_string(),
                            "0".to_string(),
                        )
                    };

                let (amount_in_usd, amount_out_usd) = if native_token != "0" {
                    // Get SOL price in USD
                    let decimals = 9;
                    let sol_price_usd =
                        get_usd_value_of_native(&self.chain_id, &(10_u128.pow(decimals))).await;

                    let sol_price_f64 = sol_price_usd.parse::<f64>().unwrap_or(0.0);

                    if sol_price_f64 > 0.0 && price.is_some() {
                        let swap_price = price.unwrap();

                        // Calculate USD values using the extracted variables
                        let native_amount_sol = native_amount.parse::<f64>().unwrap_or(0.0)
                            / 10_f64.powf(decimals as _);
                        let other_amount_tokens = other_amount.parse::<f64>().unwrap_or(0.0);

                        let native_usd_val = native_amount_sol * sol_price_f64;
                        let other_usd_val = if swap_price != 0.0 {
                            other_amount_tokens / swap_price * sol_price_f64
                        } else {
                            0.0
                        };

                        // Map back to amount_in_usd and amount_out_usd based on which is which
                        if token_in == native_token {
                            (native_usd_val.to_string(), other_usd_val.to_string())
                        } else {
                            (other_usd_val.to_string(), native_usd_val.to_string())
                        }
                    } else {
                        ("0".to_string(), "0".to_string())
                    }
                } else {
                    ("0".to_string(), "0".to_string())
                };

                let query = r#"
                    INSERT INTO ammswap
                        (pool_address, token_in, token_out, amount_in, amount_out,
                         amount_in_usd, amount_out_usd, initiator_user_address, price,
                         transaction_hash, block_number, timestamp, chain_id, is_vault_initiated,
                         sqrt_price, liquidity, tick)
                    VALUES
                        ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (transaction_hash) DO NOTHING
                "#;

                let response = database_client
                    .execute(
                        query,
                        &[
                            &pool_address,
                            &token_in,
                            &token_out,
                            &Decimal::from(amount_in.parse::<i64>().unwrap()),
                            &Decimal::from(amount_out.parse::<i64>().unwrap()),
                            &Decimal::from_f64(amount_in_usd.parse::<f64>().unwrap()),
                            &Decimal::from_f64(amount_out_usd.parse::<f64>().unwrap()),
                            &initiator_user_address,
                            &Decimal::from_f64(price.unwrap_or(0.0)),
                            &transaction_hash,
                            &block_number,
                            &timestamp,
                            &chain_id_i64,
                            &is_vault_initiated,
                            &sqrt_price,
                            &liquidity_i64,
                            &tick,
                        ],
                    )
                    .await?;
                info!(target: "solana_indexer", "RaydiumEvent::SwapEvent inserted response {:?}", response);
            }
            RaydiumEvent::CreatePersonalPositionEvent(event) => {
                let CreatePersonalPositionEvent {
                    pool_state,
                    minter,
                    nft_owner,
                    nft_mint,
                    nft_account: _,
                    tick_lower_index: _,
                    tick_upper_index: _,
                    liquidity,
                    deposit_amount_0,
                    deposit_amount_1,
                    deposit_amount_0_transfer_fee,
                    deposit_amount_1_transfer_fee,
                    is_deposited_by_project,
                    is_deposited_by_vault,
                } = event;

                info!(target: "solana_indexer", "RaydiumEvent::CreatePersonalPositionEvent from {minter:?} for {nft_owner:?} on {pool_state}");

                let pool_address = pool_state.to_string();
                let user_address = nft_owner.to_string();
                let transaction_hash = sigs[index].signature.clone();

                let temp_position_id = nft_mint.to_string();
                let amount_token0 = deposit_amount_0 as i64;
                let amount_token1 = deposit_amount_1 as i64;
                let liquidity_amount = liquidity as i64;

                self.insert_liquidity_event(
                    &database_client,
                    pool_address,
                    user_address,
                    true,                    // is_add = true for creating position
                    is_deposited_by_project, // is_manager
                    Some(temp_position_id),
                    amount_token0,
                    amount_token1,
                    liquidity_amount,
                    Some(deposit_amount_0_transfer_fee as i64), // fee_amount_0
                    Some(deposit_amount_1_transfer_fee as i64), // fee_amount_1
                    transaction_hash,
                    system_time,
                    Some(is_deposited_by_vault),
                    "CreatePersonalPositionEvent",
                )
                .await?;
            }
            RaydiumEvent::IncreaseLiquidityEvent(event) => {
                let IncreaseLiquidityEvent {
                    position_nft_mint,
                    liquidity,
                    amount_0,
                    amount_1,
                    amount_0_transfer_fee,
                    amount_1_transfer_fee,
                } = event;

                info!(target: "solana_indexer", "RaydiumEvent::IncreaseLiquidityEvent for position {position_nft_mint}");

                let position_nft_str = position_nft_mint.to_string();
                let (is_vault, is_manager, pool_address, user_address) = self
                    .get_position_flags(&database_client, &position_nft_str)
                    .await?;

                let transaction_hash = sigs[index].signature.clone();
                let position_id = position_nft_mint.to_string();
                let amount_token0 = amount_0 as i64;
                let amount_token1 = amount_1 as i64;
                let liquidity_amount = liquidity as i64;

                self.insert_liquidity_event(
                    &database_client,
                    pool_address,
                    user_address,
                    true, // is_add = true for increasing liquidity
                    is_manager,
                    Some(position_id),
                    amount_token0,
                    amount_token1,
                    liquidity_amount,
                    Some(amount_0_transfer_fee as i64),
                    Some(amount_1_transfer_fee as i64),
                    transaction_hash,
                    system_time,
                    Some(is_vault),
                    "IncreaseLiquidityEvent",
                )
                .await?;
            }
            RaydiumEvent::DecreaseLiquidityEvent(event) => {
                let DecreaseLiquidityEvent {
                    position_nft_mint,
                    liquidity,
                    decrease_amount_0,
                    decrease_amount_1,
                    fee_amount_0,
                    fee_amount_1,
                    reward_amounts: _,
                    transfer_fee_0,
                    transfer_fee_1,
                } = event;

                info!(target: "solana_indexer", "RaydiumEvent::DecreaseLiquidityEvent for position {position_nft_mint}");

                let position_nft_str = position_nft_mint.to_string();
                let (is_vault, is_manager, pool_address, user_address) = self
                    .get_position_flags(&database_client, &position_nft_str)
                    .await?;

                let transaction_hash = sigs[index].signature.clone();
                let position_id = position_nft_mint.to_string();
                let amount_token0 = decrease_amount_0 as i64;
                let amount_token1 = decrease_amount_1 as i64;
                let liquidity_amount = liquidity as i64;

                // Combine fee_amount and transfer_fee for total fees
                let total_fee_0 = (fee_amount_0 + transfer_fee_0) as i64;
                let total_fee_1 = (fee_amount_1 + transfer_fee_1) as i64;

                self.insert_liquidity_event(
                    &database_client,
                    pool_address,
                    user_address,
                    false, // is_add = false for decreasing liquidity
                    is_manager,
                    Some(position_id),
                    amount_token0,
                    amount_token1,
                    liquidity_amount,
                    Some(total_fee_0),
                    Some(total_fee_1),
                    transaction_hash,
                    system_time,
                    Some(is_vault),
                    "DecreaseLiquidityEvent",
                )
                .await?;
            }
            ev => debug!("Skipped event processing {:?}", ev),
        }
        Ok(())
    }

    async fn insert_liquidity_event(
        &self,
        database_client: &Arc<Client>,
        pool_address: String,
        user_address: String,
        is_add: bool,
        is_manager: bool,
        position_id: Option<String>,
        amount_token0: i64,
        amount_token1: i64,
        liquidity_amount: i64,
        fee_amount_0: Option<i64>,
        fee_amount_1: Option<i64>,
        transaction_hash: String,
        timestamp: SystemTime,
        is_vault: Option<bool>,
        log_target: &str,
    ) -> anyhow::Result<()> {
        let query = r#"
            INSERT INTO liquidity
                (pool_address, user_address, is_add, position_id, token_0_amount, token_1_amount,
                 chain_id, timestamp, transaction_hash, is_manager, liquidity, is_vault)
            VALUES
                ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (transaction_hash) DO NOTHING
        "#;

        info!(target: log_target, "\n\nInserting liquidity event: pool_address = {}, user_address = {}, is_add = {}, position_id = {:?}, amount_token0 = {}, amount_token1 = {}, liquidity_amount = {}, transaction_hash = {} \n\n",
            pool_address, user_address, is_add, position_id, amount_token0, amount_token1, liquidity_amount, transaction_hash);

        match database_client
            .execute(
                query,
                &[
                    &pool_address,
                    &user_address,
                    &is_add,
                    &position_id,
                    &Decimal::from(amount_token0),
                    &Decimal::from(amount_token1),
                    &self.chain_id,
                    &timestamp,
                    &transaction_hash,
                    &is_manager,
                    &liquidity_amount,
                    &is_vault,
                ],
            )
            .await
        {
            Ok(rows) => {
                info!(target: log_target, "Liquidity event inserted: {:?} rows", rows);
                Ok(())
            }
            Err(e) => {
                error!(target: log_target, "Failed to insert liquidity data: {:?}", e);
                Err(anyhow::anyhow!("Failed to insert liquidity data: {:?}", e))
            }
        }
    }

    /// Helper function to query the original is_vault and is_manager values for a position
    async fn get_position_flags(
        &self,
        database_client: &Arc<Client>,
        position_id: &str,
    ) -> anyhow::Result<(bool, bool, String, String)> {
        let position_query = r#"
            SELECT pool_address, user_address, is_vault, is_manager
            FROM liquidity
            WHERE position_id = $1
            ORDER BY timestamp ASC
            LIMIT 1
        "#;

        if let Ok(Some(row)) = database_client
            .query_opt(position_query, &[&position_id])
            .await
        {
            let pool_address: String = row.get("pool_address");
            let user_address: String = row.get("user_address");
            let is_vault: Option<bool> = row.get("is_vault");
            let is_manager: bool = row.get("is_manager");

            Ok((
                is_vault.unwrap_or(false),
                is_manager,
                pool_address,
                user_address,
            ))
        } else {
            // Default values if position not found
            Ok((false, false, "unknown".to_string(), "unknown".to_string()))
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
            let query = "SELECT latest_block FROM chain_metadata WHERE chain_id = $1";
            match client.query_opt(query, &[&self.chain_id.to_string()]).await {
                Ok(Some(row)) => {
                    let latest_block: i64 = row.get("latest_block");
                    info!("Loaded latest block from chain_metadata: {}", latest_block);
                    Some(latest_block)
                }
                Ok(None) => {
                    info!("No chain_metadata found for chain_id {}, starting fresh", self.chain_id);
                    None
                }
                Err(e) => {
                    error!("Error fetching latest block from chain_metadata: {:?}", e);
                    None
                }
            }
        };

        if let Some(latest_block_number) = latest_block_number {
            let block_number = latest_block_number as u64;
            // Fetch historical transactions backwards from the latest block number
            // Open a new thread to fetch historical transactions
            info!("Solana starting block number {:?}", block_number);
            self.fetch_historical_transactions(client.clone(), block_number)
                .await?;
        }
        self.fetch_historical_transactions(client, 346293323)
            .await?;

        // Placeholder logic for listening to Solana program events
        info!(
            "Listening to events for Solana program {} on RPC {}",
            self.vaults_program_id, self.rpc_url
        );

        // Subscribe to logs

        Ok(())
    }
}

#[derive(Debug, Clone)]
enum Events {
    Raydium(Vec<RaydiumEvent>),
    Vaults(Vec<VaultsEvent>),
}

impl SolanaIndexer {
    async fn get_amm_config(&self, config_index: Option<u16>) -> anyhow::Result<AmmConfig> {
        let (amm_config_pubkey, _bump) = match config_index {
            Some(idx) => {
                let idx_bytes = idx.to_be_bytes();
                let seeds = &[AMM_CONFIG_SEED.as_bytes(), &idx_bytes][..];
                Pubkey::find_program_address(seeds, &self.amm_program_id)
            }
            None => {
                let seeds = &[AMM_CONFIG_SEED.as_bytes()][..];
                Pubkey::find_program_address(seeds, &self.amm_program_id)
            }
        };
        let amm_config = AmmConfig::try_deserialize(
            &mut &self.rpc_client.get_account_data(&amm_config_pubkey).await?[..],
        )?;
        Ok(amm_config)
    }

    async fn parse_raydium_events(&self, logs: &[&str]) -> anyhow::Result<Vec<RaydiumEvent>> {
        let serialized_events: Vec<&str> = logs
            .iter()
            .filter_map(|log| log.strip_prefix("Program data: "))
            .collect();
        let mut events = vec![];
        for event in serialized_events {
            let decoded_event = base64::prelude::BASE64_STANDARD.decode(event);
            match decoded_event {
                Ok(mut decoded_event) => {
                    if decoded_event.len() < 8 {
                        warn!(target: "solana_indexer", "Received event with less than 8 bytes");
                        continue;
                    }
                    let data = decoded_event.split_off(8);
                    let discriminator = decoded_event;
                    let result = match discriminator.as_ref() {
                        IncreaseLiquidityEvent::DISCRIMINATOR => {
                            <IncreaseLiquidityEvent as borsh_0_10::BorshDeserialize>::try_from_slice(
                                &data,
                            )
                            .map(RaydiumEvent::IncreaseLiquidityEvent)
                        }
                        DecreaseLiquidityEvent::DISCRIMINATOR => {
                            <DecreaseLiquidityEvent as borsh_0_10::BorshDeserialize>::try_from_slice(
                                &data,
                            )
                            .map(RaydiumEvent::DecreaseLiquidityEvent)
                        }
                        SwapEvent::DISCRIMINATOR => {
                            <SwapEvent as borsh_0_10::BorshDeserialize>::try_from_slice(&data)
                                .map(RaydiumEvent::SwapEvent)
                        }
                        ConfigChangeEvent::DISCRIMINATOR => {
                            <ConfigChangeEvent as borsh_0_10::BorshDeserialize>::try_from_slice(&data)
                                .map(RaydiumEvent::ConfigChangeEvent)
                        }
                        CreatePersonalPositionEvent::DISCRIMINATOR => {
                            <CreatePersonalPositionEvent as borsh_0_10::BorshDeserialize>::try_from_slice(&data)
                                .map(RaydiumEvent::CreatePersonalPositionEvent)
                        }
                        PoolCreatedEvent::DISCRIMINATOR => {
                            let event = <PoolCreatedEvent as borsh_0_10::BorshDeserialize>::try_from_slice(&data)?;
                            let pool_state = PoolState::try_deserialize(&mut &self.rpc_client.get_account_data(&event.pool_state).await?[..])?;
                            let amm_config = self.get_amm_config(None).await?;
                            Ok(RaydiumEvent::PoolCreatedEvent(PoolCreatedEventWithState {
                                inner: event,
                                pool_state,
                                amm_config
                            }))
                        }
                        d => {
                            debug!(target: "solana_indexer", "Unknown discriminator: {:?}", d);
                            continue;
                        }
                    };
                    match result {
                        Ok(event) => {
                            events.push(event);
                            continue;
                        }
                        Err(e) => {
                            error!(target: "solana_indexer", "Error parsing event data: {:?}", e);
                            continue;
                        }
                    }
                }
                Err(e) => {
                    warn!(target: "solana_indexer", "Failed to decode event data: {:?}", e);
                    continue;
                }
            }
        }

        Ok(events)
    }

    /// Parses a list of Solana program log lines and parses events
    /// emitted by a specified program ID.
    ///
    /// # Arguments
    ///
    /// * `logs` – A slice of log lines (`Vec<String>`) from a confirmed transaction.
    /// * `raydium_program_id` – The public key of the Raydium program whose events we track.
    /// * `vault_program_id` – The public key of the Vaults program whose events we track.
    ///
    /// # Returns
    ///
    /// A vector of `Events` structs, in the order they were emitted.
    async fn parse_solana_events(
        &self,
        logs: &[&str],
        raydium_program_id: &Pubkey,
        vaults_program_id: &Pubkey,
    ) -> anyhow::Result<Vec<Events>> {
        let mut events = Vec::new();
        let mut current_program = None;
        let mut event_logs: Vec<&str> = Vec::new();

        for line in logs {
            // Track callstack for each program to maintain a proper scope for the events
            if let Some(id_str) = line
                .strip_prefix("Program ")
                .and_then(|l| l.split_whitespace().next())
            {
                if line.contains(" invoke [") {
                    if let Ok(pk) = Pubkey::from_str(id_str) {
                        if current_program.is_none() {
                            current_program = Some(pk);
                            event_logs.clear();
                        }
                        continue;
                    }
                }
                if *line == format!("Program {} success", id_str) {
                    if let Some(top) = &current_program {
                        if top.to_string() == id_str {
                            let evs = get_events_from_logs(&event_logs);
                            if !evs.is_empty() {
                                events.push(Events::Vaults(evs));
                            }
                            let evs = self.parse_raydium_events(&event_logs).await?;
                            if !evs.is_empty() {
                                events.push(Events::Raydium(evs));
                            }

                            current_program = None;
                            event_logs.clear();
                        }
                    }
                    continue;
                }
            }

            if current_program.as_ref() == Some(raydium_program_id)
                || current_program.as_ref() == Some(vaults_program_id)
            {
                event_logs.push(line.as_ref());
            } else {
                debug!(target: "solana_indexer", "Unknown event: {}", line);
            }
        }
        if let Some(program) = current_program {
            warn!(
                target: "solana_indexer", "Invoke stack is not empty after events parsing: {}",
                program.to_string(),
            )
        }

        Ok(events)
    }
}

/// Traverses the logs and extracts the events by deserializing them from base64
/// and then again using borsh.
pub fn get_events_from_logs(logs: &[&str]) -> Vec<VaultsEvent> {
    let serialized_events: Vec<&str> = logs
        .iter()
        .filter_map(|log| log.strip_prefix("Program data: "))
        .collect();
    let mut events: Vec<VaultsEvent> = serialized_events
        .iter()
        .filter_map(|event| {
            let decoded_event = base64::prelude::BASE64_STANDARD.decode(event);
            if let Ok(decoded_event) = decoded_event {
                let decoded_event: Result<VaultsEvent, String> =
                    BorshDeserialize::try_from_slice(&decoded_event).map_err(|e| {
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
                .nth(1);
            if message_id.is_none() {
                log::error!(target: "solana_indexer", "No message id found");
                return events;
            }
            let message_id = message_id.unwrap().to_string();
            // Also unpack the message that was sent.
            let deposit_event = match events.first() {
                Some(VaultsEvent::MessageDispatchedFromVault(event)) => {
                    let message = event.message.clone();
                    let decoded_message =
                        SolidityIntentProcessorBoundMessage::abi_decode(message.as_ref(), true);
                    if decoded_message.is_err() {
                        log::error!(target: "solana_indexer", "Error decoding message: {:?}", decoded_message.err());
                        return events;
                    }
                    let decoded_message = decoded_message.unwrap();
                    let deposit_message = IntentProcessorBoundMessageDepositData::abi_decode(
                        decoded_message.data.as_ref(),
                        true,
                    );
                    if deposit_message.is_err() {
                        log::error!(target: "solana_indexer", "Error decoding deposit message: {:?}", deposit_message.err());
                        return events;
                    }
                    let deposit_message = deposit_message.unwrap();
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
                events.push(VaultsEvent::DepositedFunds(deposit_event));
            }
        }
    }
    events
}

/// A unified enum of all Raydium events we care about.
#[derive(Clone, Debug)]
pub enum RaydiumEvent {
    IncreaseLiquidityEvent(IncreaseLiquidityEvent),
    DecreaseLiquidityEvent(DecreaseLiquidityEvent),
    CollectPersonalFeeEvent(CollectPersonalFeeEvent),
    CollectProtocolFeeEvent(CollectProtocolFeeEvent),
    PoolCreatedEvent(PoolCreatedEventWithState),
    ConfigChangeEvent(ConfigChangeEvent),
    SwapEvent(SwapEvent),
    CreatePersonalPositionEvent(CreatePersonalPositionEvent),
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
pub enum VaultsEvent {
    DepositedFunds(DepositFundsEvent),
    DepositContractCreated(DepositContractCreatedEvent),
    MessageDispatchedFromVault(MessageDispatchedFromVaultEvent),
    ReceivedMessageOnVault(ReceivedMessageOnVaultEvent),
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
pub struct DepositFundsEvent {
    // The 20 byte evm address of the user for whom the deposit is being made
    pub user_address: String,
    pub token_address: String,
    pub amount: u64,
    pub message_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
pub struct DepositContractCreatedEvent {
    // The 20 byte evm address of the user for whom the deposit is being made
    pub user_address: Vec<u8>,
    // The derived deposit contract address
    pub derived_deposit_contract_address: Pubkey,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
pub struct MessageDispatchedFromVaultEvent {
    pub user_address: Vec<u8>,
    pub destination_domain: u32,
    pub interop_provider: LocalInteropProvider,
    pub message: Vec<u8>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
pub struct ReceivedMessageOnVaultEvent {
    pub sender: Vec<u8>,
    pub source_domain: u32,
    pub interop_provider: LocalInteropProvider,
    pub message: Vec<u8>,
}

// Defining a local interop provider so that it can be used to export as a crate
// and define traits for it.
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
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

fn init_logger() {
    let _ = env_logger::builder()
        .filter_level(LevelFilter::Info)
        .try_init();
}

#[cfg(test)]
mod tests {
    use anchor_lang::pubkey;
    use solana_client::rpc_config::RpcTransactionConfig;
    use super::*;

    #[tokio::test]
    pub async fn test_get_events_from_logs() {
        init_logger();
        let url = "https://api.mainnet-beta.solana.com".to_string();
        let program_id = pubkey!("CQTC16KM4XqjVJ8ASMPLxjv3siGAQLVcMauPGu1jMGNz");
        let amm_program_id = pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK");
        let indexer = SolanaIndexer::new(
            url.clone(),
            String::new(),
            0,
            program_id.to_string(),
            amm_program_id.to_string(),
        );
        let rpc_client = RpcClient::new(url);
        let tx_hash = Signature::from_str(
            "3BZfM9oJdmvP1MXNfCUToEmHQHwf5trV8vhGZrN4xnn7kdymcAotGLQsoosLx1L9qc7KAk4yktYZtzMmvzRhn5UH",
        )
            .unwrap();
        let logs = rpc_client
            .get_transaction(&tx_hash, UiTransactionEncoding::Base64)
            .await
            .unwrap();
        let logs = logs.transaction.meta.unwrap().log_messages;
        let logs = match &logs {
            solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => {
                logs.iter().map(|x| x.as_ref()).collect::<Vec<&str>>()
            }
            _ => Vec::new(),
        };
        println!("{}", logs.join("\n"));
        let mut events = indexer
            .parse_solana_events(
                &logs,
                &pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"),
                &program_id,
            )
            .await
            .unwrap();
        dbg!(&events);
        let events = if let Events::Vaults(events) = events.pop().unwrap() {
            events
        } else {
            panic!()
        };

        let user_address = "0xfCE5eEBff30d36121D965dcB862270D700f3b687";
        let token_address = "0xc6fa7af3bedbad3a3d65f36aabc97431b1bbe4c2d2f6e0e47ca60203452f5d61";
        let amount = 10000000;
        let message_id = "0x3466d9c67793ef637dd6beb3ec91a572295e5085b63ffbdb959bdb829b87dafd";

        // 2nd event is deposit event
        let deposit_event = events[1].clone();
        let deposit_event = match deposit_event {
            VaultsEvent::DepositedFunds(event) => event,
            _ => panic!("Expected deposit event"),
        };
        assert_eq!(
            deposit_event.user_address.to_ascii_lowercase(),
            user_address.to_ascii_lowercase()
        );
        assert_eq!(
            deposit_event.token_address.to_ascii_lowercase(),
            token_address.to_ascii_lowercase()
        );
        assert_eq!(deposit_event.amount, amount);
        assert_eq!(deposit_event.message_id, message_id);
    }

    #[tokio::test]
    pub async fn test_get_raydium_events_from_logs() {
        init_logger();
        let url = "https://api.mainnet-beta.solana.com".to_string();
        let program_id = pubkey!("CQTC16KM4XqjVJ8ASMPLxjv3siGAQLVcMauPGu1jMGNz");
        let amm_program_id = pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK");

        let indexer = SolanaIndexer::new(
            url.clone(),
            String::new(),
            0,
            program_id.to_string(),
            amm_program_id.to_string(),
        );
        let rpc_client = RpcClient::new(url);
        let tx_hash = Signature::from_str(
            "39Zoyu6Wpp5STngzihSa6FVTjr2vT9GT7zUXL2XAv1as6iMYVExZdJbb7SZiX9KvXbFGkzxTaFMg1Eb9XGPChXiQ",
        )
            .unwrap();
        let logs = rpc_client
            .get_transaction_with_config(
                &tx_hash,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: None,
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
            .unwrap();
        let logs = logs.transaction.meta.unwrap().log_messages;
        let logs = match &logs {
            solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => {
                logs.iter().map(|x| x.as_ref()).collect::<Vec<&str>>()
            }
            _ => Vec::new(),
        };
        println!("{}", logs.join("\n"));
        let mut events = indexer
            .parse_solana_events(
                &logs,
                &pubkey!("CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK"),
                &pubkey!("CQTC16KM4XqjVJ8ASMPLxjv3siGAQLVcMauPGu1jMGNz"),
            )
            .await
            .unwrap();
        dbg!(&events);
        let events = match events.pop().unwrap() {
            Events::Raydium(events) => events,
            _ => panic!("Expected raydium events"),
        };
        let deposit_event = events[0].clone();
        let deposit_event = match deposit_event {
            RaydiumEvent::DecreaseLiquidityEvent(event) => event,
            _ => panic!("Expected deposit event"),
        };

        let expected_event = DecreaseLiquidityEvent {
            position_nft_mint: pubkey!("4UaVjQ5cUunvCaZiyij2K7rfmn8HgBjCn2HExfbpYNL4"),
            liquidity: 0,
            decrease_amount_0: 0,
            decrease_amount_1: 0,
            fee_amount_0: 0,
            fee_amount_1: 15008807,
            reward_amounts: [0, 0, 0],
            transfer_fee_0: 0,
            transfer_fee_1: 0,
        };
        assert_eq!(deposit_event, expected_event);
    }

    #[tokio::test]
    pub async fn test_get_raydium_events_from_logs2() {
        init_logger();
        let url = "https://api.mainnet-beta.solana.com".to_string();
        let program_id = pubkey!("2RBS3DPck8CoF9b31nQDRE3j9xsx5io1STAk2irhgoBC");
        let amm_program_id = pubkey!("3eap9FEhnPAjd9aatu4Bw2osw6XPZ8cZJHjQAv2DjWnH");

        let indexer = SolanaIndexer::new(
            url.clone(),
            String::new(),
            0,
            program_id.to_string(),
            amm_program_id.to_string(),
        );
        let rpc_client = RpcClient::new(url);
        let tx_hash = Signature::from_str(
            "3VusxYsmcca64L66ayyzGTgLSrWAe3SHGz74UQJaDw9QMqofi9XYvPj1rqqExyN6tWE2W6FmQPYC52kbkYVNDQkU",
        )
            .unwrap();
        let logs = rpc_client
            .get_transaction_with_config(
                &tx_hash,
                RpcTransactionConfig {
                    encoding: Some(UiTransactionEncoding::Base64),
                    commitment: None,
                    max_supported_transaction_version: Some(0),
                },
            )
            .await
            .unwrap();
        let logs = logs.transaction.meta.unwrap().log_messages;
        let logs = match &logs {
            solana_transaction_status::option_serializer::OptionSerializer::Some(logs) => {
                logs.iter().map(|x| x.as_ref()).collect::<Vec<&str>>()
            }
            _ => Vec::new(),
        };
        println!("{}", logs.join("\n"));
        let mut events = indexer
            .parse_solana_events(
                &logs,
                &pubkey!("3eap9FEhnPAjd9aatu4Bw2osw6XPZ8cZJHjQAv2DjWnH"),
                &pubkey!("2RBS3DPck8CoF9b31nQDRE3j9xsx5io1STAk2irhgoBC"),
            )
            .await
            .unwrap();
        dbg!(&events);
        let events = match events.pop().unwrap() {
            Events::Raydium(events) => events,
            _ => panic!("Expected raydium events"),
        };
        let event = events[0].clone();
        let swap_event = match event {
            RaydiumEvent::SwapEvent(event) => event,
            _ => panic!("Expected deposit event"),
        };
        dbg!(&swap_event);

        let expected_event = SwapEvent {
            pool_state: pubkey!("C2FMp7HLFcZA1DjVhX9T2WCPKssKFSMAx9KnS6TQb4c3"),
            sender: pubkey!("HY28ik8ZceEYUkT2Bh5mJH5V5xNGEMUw8Mqs3atP2yZq"),
            token_account_0: pubkey!("GxrUmNpSpR3a9Gj3SsNiai42H4SyXqzPGkr1yubNAe1u"),
            token_account_1: pubkey!("J3FXcH1v9x3UDk92oAVpQ1fqgyTH8RBfbkJsJ74p7Vne"),
            amount_0: 987276753434,
            transfer_fee_0: 0,
            amount_1: 100000,
            transfer_fee_1: 0,
            zero_for_one: false,
            sqrt_price_x64: 5908371332803506,
            liquidity: 24595358991,
            tick: -160934,
            via_vault: true,
        };
        assert_eq!(swap_event, expected_event);
    }
}