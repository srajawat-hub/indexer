use crate::mayan::messages::{BatchUnlockMessage, CancelMessage, SwiftVaaAction, UnlockMessage};
use crate::schema::mayan_vaas;
use crate::structs::{WormholeExplorerResponse, WormholeSingleVaaResponse, WormholeVaaData};
use base64::{engine::general_purpose, Engine as _};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use diesel::pg::PgConnection;
use diesel::prelude::*;
use log::{debug, error, info, warn};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::time::sleep;

// Configuration constants
const WORMHOLE_EXPLORER_API: &str = "https://api.wormholescan.io";
const ETHEREUM_CHAIN_ID: u16 = 2;
const BASE_CHAIN_ID: u16 = 30;
const SOLANA_CHAIN_ID: u16 = 1;
const MAYAN_EMITTER_ADDRESS: &str = "0xc38e4e6a15593f908255214653d3d947ca1c2338";
const MAYAN_EMITTER_ADDRESS_SOLANA: &str =
    "23b1261d67d23099d43d0ad07a71b81d1e3c8c1d95da4c609b543ce7143fc069";
const BATCH_SIZE: usize = 100;
const PROCESSING_DELAY: Duration = Duration::from_secs(10);
const DEFAULT_START_SEQUENCE_ETHEREUM: u64 = 336056;
const DEFAULT_START_SEQUENCE_BASE: u64 = 68161;
const DEFAULT_START_SEQUENCE_SOLANA: u64 = 132611;

// TODO: fix the function to fetch other chains
fn get_default_start_sequence(chain_id: u16) -> u64 {
    // match chain_id {
    //     ETHEREUM_CHAIN_ID => DEFAULT_START_SEQUENCE_ETHEREUM,
    //     BASE_CHAIN_ID => DEFAULT_START_SEQUENCE_BASE,
    //     SOLANA_CHAIN_ID => DEFAULT_START_SEQUENCE_SOLANA,
    //     cid => panic!("unknown chain ID: {cid}")
    // }
    DEFAULT_START_SEQUENCE_SOLANA
}

#[derive(Insertable)]
#[diesel(table_name = mayan_vaas)]
pub struct NewMayanVaa {
    pub sequence: i64,
    pub order_hash: String,
    pub vaa_action: i16,
    pub timestamp: chrono::NaiveDateTime,
    pub vaa_data: Option<String>,
    pub chain_id: i16,
    pub solana_order_hash: Option<String>,
}

#[derive(Queryable, Selectable)]
#[diesel(table_name = mayan_vaas)]
pub struct MayanVaa {
    pub id: i64,
    pub sequence: i64,
    pub order_hash: String,
    pub vaa_action: i16,
    pub timestamp: chrono::NaiveDateTime,
    pub vaa_data: Option<String>,
    pub chain_id: i16,
    pub solana_order_hash: Option<String>,
    pub created_at: chrono::NaiveDateTime,
}

/// Local order processing data struct (exactly mirrors Solana hook executor)
/// Used to generate the alternative SHA-256 hash for VAA queries
#[derive(Debug, Clone, BorshSerialize, BorshDeserialize)]
#[borsh(crate = "::borsh")]
pub struct LocalOrderProcessingData {
    pub protocol_id: u8,
    pub order_hash: [u8; 32],
    pub order_id: u64,
    pub recipient: [u8; 32], // Pubkey as 32-byte array
    pub token: [u8; 32],     // Pubkey as 32-byte array
    pub amount: u64,
    pub timeout_timestamp: i64, // UnixTimestamp (i64)
    pub destination_chain_id: u32,
    pub additional_data: Vec<u8>,
}

impl LocalOrderProcessingData {
    /// Create SHA-256 hash of the order data (exactly mirrors Solana hook executor logic)
    /// Uses the same algorithm as solana_program::hash::hash() which is SHA-256
    pub fn hash_order_data(&self) -> [u8; 32] {
        // Use Borsh serialization (same as Solana's try_to_vec())
        let serialized = borsh::to_vec(self).expect("Failed to serialize order data");

        // Use SHA-256 (same as Solana's hash function)
        // Note: solana_program::hash::hash() uses SHA-256 internally, same as sha2::Sha256
        let mut hasher = Sha256::new();
        hasher.update(&serialized);
        let result = hasher.finalize();
        result.into()
    }

    /// Attempt to reconstruct LocalOrderProcessingData from hook_executor_orders table
    pub async fn from_order_hash(
        db_client: &Arc<tokio_postgres::Client>,
        order_hash: &str,
    ) -> Result<Option<Self>, Box<dyn std::error::Error + Send + Sync>> {
        debug!(
            "Attempting to reconstruct LocalOrderProcessingData for order_hash: {}",
            order_hash
        );

        // TODO: remove the order hash picking
        // Debug: Check if hook_executor_orders table has any data at all
        let count_query = "SELECT COUNT(*) FROM hook_executor_orders";
        match db_client.query_one(count_query, &[]).await {
            Ok(row) => {
                let count: i64 = row.get(0);
                debug!(
                    "hook_executor_orders table contains {} records total",
                    count
                );

                if count > 0 {
                    // Sample a few order hashes for debugging
                    let sample_query = "SELECT order_hash FROM hook_executor_orders LIMIT 3";
                    match db_client.query(sample_query, &[]).await {
                        Ok(rows) => {
                            debug!("Sample order hashes in hook_executor_orders:");
                            for (i, row) in rows.iter().enumerate() {
                                let sample_hash: String = row.get(0);
                                debug!("  {}: {}", i + 1, sample_hash);
                            }
                        }
                        Err(e) => debug!("Could not fetch sample hashes: {}", e),
                    }
                }
            }
            Err(e) => debug!("Could not count hook_executor_orders records: {}", e),
        }

        // Debug: Verify table schema exists and has expected columns
        let schema_query = "SELECT column_name FROM information_schema.columns WHERE table_name = 'hook_executor_orders' ORDER BY ordinal_position";
        match db_client.query(schema_query, &[]).await {
            Ok(rows) => {
                debug!("hook_executor_orders table columns:");
                for (i, row) in rows.iter().enumerate() {
                    let column_name: String = row.get(0);
                    debug!("  {}: {}", i + 1, column_name);
                }
            }
            Err(e) => debug!("Could not fetch table schema: {}", e),
        }

        // Try multiple formats of the order hash to handle potential formatting differences
        let queries = vec![
            // Exact match
            format!("SELECT protocol_id, order_hash, order_id, recipient, token, amount, timeout_timestamp, destination_chain_id, additional_data FROM hook_executor_orders WHERE order_hash = $1 LIMIT 1"),
            // Case insensitive match
            format!("SELECT protocol_id, order_hash, order_id, recipient, token, amount, timeout_timestamp, destination_chain_id, additional_data FROM hook_executor_orders WHERE LOWER(order_hash) = LOWER($1) LIMIT 1"),
            // Without 0x prefix
            format!("SELECT protocol_id, order_hash, order_id, recipient, token, amount, timeout_timestamp, destination_chain_id, additional_data FROM hook_executor_orders WHERE LOWER(order_hash) = LOWER($1) OR LOWER(order_hash) = LOWER('0x' || $1) OR LOWER('0x' || order_hash) = LOWER($1) LIMIT 1"),
        ];

        // Try each query format until we find a match
        for (i, query) in queries.iter().enumerate() {
            debug!("Trying query format {} for hash: {}", i + 1, order_hash);
            match db_client.query_opt(query, &[&order_hash]).await {
                Ok(Some(row)) => {
                    info!("Found matching record in hook_executor_orders for hash: {} using query format {}", order_hash, i + 1);

                    let protocol_id: i32 = row.get(0);
                    let order_hash_str: String = row.get(1);
                    let order_id: i64 = row.get(2);
                    let recipient_str: String = row.get(3);
                    let token_str: String = row.get(4);
                    let amount_numeric: rust_decimal::Decimal = row.get(5);
                    let timeout_timestamp: i64 = row.get(6);
                    let destination_chain_id: i64 = row.get(7);
                    let additional_data_opt: Option<String> = row.get(8);

                    // Parse order hash
                    let order_hash_clean = order_hash_str.trim_start_matches("0x");
                    let order_hash_bytes =
                        hex::decode(order_hash_clean).map_err(|_| "Invalid order hash format")?;
                    if order_hash_bytes.len() != 32 {
                        return Err("Invalid order hash length".into());
                    }
                    let mut order_hash_array = [0u8; 32];
                    order_hash_array.copy_from_slice(&order_hash_bytes);

                    // Convert addresses to 32-byte format (assuming Solana pubkeys)
                    let recipient_clean = recipient_str.trim_start_matches("0x");
                    let recipient_bytes = if recipient_clean.len() == 64 {
                        hex::decode(recipient_clean).map_err(|_| "Invalid recipient format")?
                    } else {
                        // If it's shorter (Ethereum address), pad with zeros
                        let mut padded = vec![0u8; 32];
                        let decoded =
                            hex::decode(recipient_clean).map_err(|_| "Invalid recipient format")?;
                        padded[32 - decoded.len()..].copy_from_slice(&decoded);
                        padded
                    };
                    let mut recipient_array = [0u8; 32];
                    recipient_array.copy_from_slice(&recipient_bytes);

                    let token_clean = token_str.trim_start_matches("0x");
                    let token_bytes = if token_clean.len() == 64 {
                        hex::decode(token_clean).map_err(|_| "Invalid token format")?
                    } else {
                        let mut padded = vec![0u8; 32];
                        let decoded =
                            hex::decode(token_clean).map_err(|_| "Invalid token format")?;
                        padded[32 - decoded.len()..].copy_from_slice(&decoded);
                        padded
                    };
                    let mut token_array = [0u8; 32];
                    token_array.copy_from_slice(&token_bytes);

                    // Parse additional data
                    let additional_data = match additional_data_opt {
                        Some(data_str) => {
                            let clean_data = data_str.trim_start_matches("0x");
                            if clean_data.is_empty() {
                                Vec::new()
                            } else {
                                hex::decode(clean_data).unwrap_or_else(|_| Vec::new())
                            }
                        }
                        None => Vec::new(),
                    };

                    // Convert amount from Decimal to u64
                    let amount = amount_numeric
                        .to_string()
                        .parse::<u64>()
                        .map_err(|_| "Invalid amount format")?;

                    return Ok(Some(LocalOrderProcessingData {
                        protocol_id: protocol_id as u8,
                        order_hash: order_hash_array,
                        order_id: order_id as u64,
                        recipient: recipient_array,
                        token: token_array,
                        amount,
                        timeout_timestamp,
                        destination_chain_id: destination_chain_id as u32,
                        additional_data,
                    }));
                }
                Ok(None) => {
                    debug!(
                        "Query format {} found no matching record for hash: {}",
                        i + 1,
                        order_hash
                    );
                    continue; // Try next query format
                }
                Err(e) => {
                    warn!(
                        "Database error with query format {} for hash {}: {}",
                        i + 1,
                        order_hash,
                        e
                    );
                    continue; // Try next query format
                }
            }
        }

        // No query format found a match
        debug!("No matching record found in hook_executor_orders for hash: {} (tried {} query formats)", order_hash, queries.len());
        Ok(None)
    }
}

pub async fn start_vaa_processor(db_client: Arc<tokio_postgres::Client>) {
    info!("Starting VAA processor thread");

    let chains = [SOLANA_CHAIN_ID];
    // let chains = [ETHEREUM_CHAIN_ID, BASE_CHAIN_ID];
    let mut missing_sequences: std::collections::HashMap<u16, Vec<u64>> =
        std::collections::HashMap::new();

    // Initialize missing sequences for each chain
    for &chain_id in &chains {
        missing_sequences.insert(chain_id, Vec::new());
    }

    loop {
        // Process missing sequences first for each chain
        for &chain_id in &chains {
            if let Some(sequences) = missing_sequences.get_mut(&chain_id) {
                if !sequences.is_empty() {
                    info!(
                        "Processing {} missing sequences for chain {}",
                        sequences.len(),
                        chain_id
                    );
                    let batch: Vec<u64> = sequences
                        .drain(0..std::cmp::min(sequences.len(), BATCH_SIZE))
                        .collect();

                    for sequence in batch {
                        if let Err(e) =
                            process_single_vaa_sequence(&db_client, sequence, chain_id).await
                        {
                            error!(
                                "Failed to process missing sequence {} for chain {}: {}",
                                sequence, chain_id, e
                            );
                            // Re-add to missing sequences to try again later
                            sequences.push(sequence);
                        }
                    }
                }
            }
        }

        // Process new VAAs for each chain
        for &chain_id in &chains {
            let last_sequence = get_latest_processed_sequence(&db_client, chain_id)
                .await
                .unwrap_or(get_default_start_sequence(chain_id));

            info!(
                "Last processed sequence for chain {}: {}",
                chain_id, last_sequence
            );

            if let Some(sequences) = missing_sequences.get_mut(&chain_id) {
                match fetch_vaas_batch(&db_client, last_sequence + 1, sequences, chain_id).await {
                    Ok(processed_count) => {
                        if processed_count > 0 {
                            info!(
                                "Processed {} new VAAs for chain {}",
                                processed_count, chain_id
                            );
                        } else {
                            debug!("No new VAAs to process for chain {}", chain_id);
                        }
                    }
                    Err(e) => {
                        error!("Error processing VAA batch for chain {}: {}", chain_id, e);
                    }
                }
            }
        }

        sleep(PROCESSING_DELAY).await;
    }
}

async fn get_latest_processed_sequence(
    db_client: &Arc<tokio_postgres::Client>,
    chain_id: u16,
) -> Option<u64> {
    let query = "SELECT MAX(sequence) FROM mayan_vaas WHERE chain_id = $1";

    match db_client.query_opt(query, &[&(chain_id as i16)]).await {
        Ok(Some(row)) => {
            let sequence: Option<i64> = row.get(0);
            sequence.map(|s| s as u64)
        }
        Ok(None) => None,
        Err(e) => {
            error!(
                "Error fetching latest sequence for chain {}: {}",
                chain_id, e
            );
            None
        }
    }
}

async fn fetch_vaas_batch(
    db_client: &Arc<tokio_postgres::Client>,
    start_sequence: u64,
    missing_sequences: &mut Vec<u64>,
    chain_id: u16,
) -> Result<usize, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let mut processed_count = 0;
    let mut page = 0;
    let mut all_sequences_in_batch: Vec<u64> = Vec::new();
    let mut latest_sequence_reached = false;

    loop {
        let url = format!(
            "{}/api/v1/vaas/{}/{}?pageSize={}&page={}",
            WORMHOLE_EXPLORER_API, chain_id, MAYAN_EMITTER_ADDRESS_SOLANA, BATCH_SIZE, page
        );

        debug!("Fetching VAAs from: {} (page {})", url, page);

        let response = client.get(&url).send().await?;

        if !response.status().is_success() {
            return Err(format!("API request failed with status: {}", response.status()).into());
        }

        let explorer_response: WormholeExplorerResponse = response.json().await?;

        // If no data returned, we've reached the end
        if explorer_response.data.is_empty() {
            debug!(
                "No more VAAs available for chain {} at page {}",
                chain_id, page
            );
            break;
        }

        let mut sequences_in_page: Vec<u64> = Vec::new();

        for vaa_data in explorer_response.data {
            sequences_in_page.push(vaa_data.sequence);
            all_sequences_in_batch.push(vaa_data.sequence);

            // Check if we've reached our target sequence
            if vaa_data.sequence >= start_sequence {
                match process_vaa_data(db_client, &vaa_data, chain_id).await {
                    Ok(_) => {
                        processed_count += 1;
                        debug!(
                            "Processed VAA sequence: {} for chain {}",
                            vaa_data.sequence, chain_id
                        );
                    }
                    Err(e) => {
                        error!(
                            "Failed to process VAA sequence {} for chain {}: {}",
                            vaa_data.sequence, chain_id, e
                        );
                    }
                }
            } else {
                // If we encounter a sequence lower than start_sequence, we've reached the bottom
                latest_sequence_reached = true;
            }
        }

        debug!(
            "Page {} for chain {}: processed {} sequences, range: {:?} to {:?}",
            page,
            chain_id,
            sequences_in_page.len(),
            sequences_in_page.iter().min(),
            sequences_in_page.iter().max()
        );

        // Check for missing sequences in this page
        if let Some(min_seq) = sequences_in_page.iter().min() {
            if let Some(max_seq) = sequences_in_page.iter().max() {
                for seq in *min_seq..=*max_seq {
                    if !sequences_in_page.contains(&seq) && seq >= start_sequence {
                        missing_sequences.push(seq);
                        warn!("Detected missing sequence: {} on page {}", seq, page);
                    }
                }
            }
        }

        // If we've reached sequences older than our start_sequence, stop pagination
        if latest_sequence_reached {
            debug!(
                "Reached sequences older than start_sequence {} for chain {}, stopping pagination",
                start_sequence, chain_id
            );
            break;
        }

        // Move to next page
        page += 1;

        // Safety check: don't fetch too many pages
        if page > 100 {
            warn!(
                "Reached maximum page limit (100) for chain {}, stopping pagination",
                chain_id
            );
            break;
        }
    }

    debug!(
        "Completed fetching VAAs for chain {}: {} pages processed, {} VAAs processed",
        chain_id,
        page + 1,
        processed_count
    );

    Ok(processed_count)
}

async fn process_single_vaa_sequence(
    db_client: &Arc<tokio_postgres::Client>,
    sequence: u64,
    chain_id: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let url = format!(
        "{}/api/v1/vaas/{}/{}/{}",
        WORMHOLE_EXPLORER_API, chain_id, MAYAN_EMITTER_ADDRESS_SOLANA, sequence
    );

    let client = reqwest::Client::new();
    let response = client.get(&url).send().await?;

    if response.status().is_success() {
        let explorer_response: WormholeSingleVaaResponse = response.json().await?;

        process_vaa_data(db_client, &explorer_response.data, chain_id).await?;
        info!(
            "Processed missing VAA sequence: {} for chain {}",
            sequence, chain_id
        );
    } else if response.status().as_u16() == 404 {
        debug!("VAA sequence {} not found (404)", sequence);
    } else {
        return Err(format!("API request failed with status: {}", response.status()).into());
    }

    Ok(())
}

async fn process_vaa_data(
    db_client: &Arc<tokio_postgres::Client>,
    vaa_data: &WormholeVaaData,
    chain_id: u16,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let vaa_bytes = general_purpose::STANDARD.decode(&vaa_data.vaa)?;

    let vaa = wormhole_raw_vaas::Vaa::parse(&vaa_bytes)
        .map_err(|e| format!("Failed to parse VAA: {}", e))?;

    let payload = vaa.body().payload();
    let payload_bytes = payload.as_ref();

    let (order_hashes, vaa_action) = parse_mayan_message(payload_bytes)?;

    let insert_query = r#"
        INSERT INTO mayan_vaas (sequence, order_hash, vaa_action, timestamp, vaa_data, chain_id, solana_order_hash)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        ON CONFLICT (sequence, order_hash, chain_id) DO NOTHING
    "#;

    let timestamp_system_time: SystemTime = vaa_data.timestamp.into();

    // Insert a row for each order hash with the same sequence
    for order_hash in order_hashes.iter() {
        debug!("Processing VAA order hash: {} (format check)", order_hash);

        // Try to generate the Solana-style hash by looking up order details
        let solana_order_hash =
            match LocalOrderProcessingData::from_order_hash(db_client, order_hash).await {
                Ok(Some(local_order_data)) => {
                    let hash_bytes = local_order_data.hash_order_data();
                    let solana_hash = hex::encode(hash_bytes);
                    info!(
                        "Successfully generated Solana hash for order {}: {}",
                        order_hash, solana_hash
                    );
                    Some(solana_hash)
                }
                Ok(None) => {
                    debug!(
                        "No order details found for hash {} to generate Solana hash",
                        order_hash
                    );
                    None
                }
                Err(e) => {
                    warn!(
                        "Failed to generate Solana hash for order {}: {}",
                        order_hash, e
                    );
                    None
                }
            };

        db_client
            .execute(
                insert_query,
                &[
                    &(vaa_data.sequence as i64),
                    &order_hash,
                    &vaa_action,
                    &timestamp_system_time,
                    &Some(vaa_data.vaa.clone()), // Store the base64 VAA data for debugging
                    &(chain_id as i16),
                    &solana_order_hash,
                ],
            )
            .await?;

        debug!(
            "Inserted VAA: sequence={}, action={}, order_hash={}, chain_id={}, solana_hash={:?}",
            vaa_data.sequence, vaa_action, order_hash, chain_id, solana_order_hash
        );
    }

    Ok(())
}

fn parse_mayan_message(
    payload_bytes: &[u8],
) -> Result<(Vec<String>, i16), Box<dyn std::error::Error + Send + Sync>> {
    if payload_bytes.is_empty() {
        return Err("Empty payload".into());
    }

    let action = payload_bytes[0];

    match action {
        action if action == SwiftVaaAction::Unlock as u8 => {
            let unlock_msg = UnlockMessage::parse_unchecked(payload_bytes);
            let order_hash = hex::encode(unlock_msg.hash());
            Ok((vec![order_hash], SwiftVaaAction::Unlock as i16))
        }
        action if action == SwiftVaaAction::Cancel as u8 => {
            let cancel_msg = CancelMessage::parse_unchecked(payload_bytes);
            let order_hash = hex::encode(cancel_msg.hash());
            Ok((vec![order_hash], SwiftVaaAction::Cancel as i16))
        }
        action if action == SwiftVaaAction::UnlockBatch as u8 => {
            let batch_msg = BatchUnlockMessage::parse_unchecked(payload_bytes);
            let items_count = batch_msg.items_count();

            if items_count == 0 {
                return Err("Empty batch unlock message".into());
            }

            // Extract all hashes from the batch
            let mut order_hashes = Vec::new();
            for i in 0..items_count {
                let order_hash = hex::encode(batch_msg.hash(i));
                order_hashes.push(order_hash);
            }

            Ok((order_hashes, SwiftVaaAction::UnlockBatch as i16))
        }
        _ => {
            // Unknown action, still store it but mark as None
            warn!("Unknown VAA action: {}", action);
            Ok((vec!["unknown".to_string()], SwiftVaaAction::None as i16))
        }
    }
}
