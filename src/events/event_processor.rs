use alloy::primitives::{Address, FixedBytes};
use alloy::providers::{Provider, RootProvider};
use alloy::pubsub::PubSubFrontend;
use alloy::rpc::types::{Log, TransactionReceipt};
use alloy::sol_types::SolValue;
use alloy::sol_types::SolEvent;
use anyhow::bail;
use futures_util::stream::StreamExt;
use futures_util::Stream;
use log::{debug, error, info, warn};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio_postgres::Client;

use crate::events::evm_handlers::acknowledgement_received::handle_acknowledgement_received_event;
use crate::events::evm_handlers::debridge_order_created::handle_debridge_order_created_event;
use crate::events::evm_handlers::deposit_received::handle_deposit_received_event;
use crate::events::evm_handlers::intent_fees::handle_intent_fees_event;
use crate::events::evm_handlers::intent_submitted::handle_intent_submitted_event;
use crate::events::evm_handlers::message_dispatched_from_vault::handle_message_dispatched_from_vault_event;
use crate::events::evm_handlers::order_created::handle_order_created_event;
use crate::events::evm_handlers::received_message_on_vault::handle_received_message_on_vault_event;
use crate::events::evm_handlers::solution_submitted::handle_solution_submitted_event;
use crate::events::evm_handlers::uniswap_burn::handle_uniswap_burn_event;
use crate::events::evm_handlers::uniswap_mint::handle_uniswap_mint_event;
use crate::events::evm_handlers::uniswap_mint_by_project_manager::handle_uniswap_mint_by_pm_event;
use crate::events::evm_handlers::uniswap_poolcreated::handle_uniswap_pool_created_event;
use crate::events::evm_handlers::uniswap_swap::handle_uniswap_swap_event;
use crate::solidity_structs::token::Token::Transfer;
use crate::solidity_structs::uniswap_v3_factory_lib::UniswapV3FactoryLib;
use crate::solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib;
use crate::solidity_structs::{
    self, token, AmountTypes, QuoteApiResponse, ResultCosts, ThirdPartyFeeResult,
};
use crate::solidity_structs::{
    intent_lib_v2::IntentLibV2, intent_processor::IntentProcessorV2, vault::Vault,
};
use crate::utils::get_native_token_cmc_id;

pub enum IntentVersions {
    IntentSubmitted,
    SolutionSubmitted,
    OrderCreated,
    ReceivedMessageOnVault,
    MessageDispatchedFromVault,
    AcknowledgementReceived,
}

#[derive(Debug)]
pub enum IntentStage {
    Initialized,
    Processing,
    Done,
    Failed,
}

#[derive(Debug)]
pub enum DepositStatus {
    Initialized,
    Done,
}

impl ToString for IntentStage {
    fn to_string(&self) -> String {
        match self {
            IntentStage::Initialized => "Initialized".to_string(),
            IntentStage::Processing => "Processing".to_string(),
            IntentStage::Done => "Done".to_string(),
            IntentStage::Failed => "Failed".to_string(),
        }
    }
}

#[derive(Debug, Deserialize)]
struct ApiResponse {
    data: HashMap<String, TokenInfo>,
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

// =========== Historical
#[derive(Debug, Deserialize)]
struct ApiResponseHistory {
    data: HashMap<String, TokenInfoHistory>,
}

#[derive(Debug, Deserialize)]
struct TokenInfoHistory {
    quotes: Vec<QuoteEntryHistory>,
}

#[derive(Debug, Deserialize)]
struct QuoteEntryHistory {
    quote: HashMap<String, QuoteDetail>,
}

#[derive(Debug, Deserialize)]
struct QuoteDetail {
    price: Option<f64>,
}

pub async fn get_usd_value_of_native(
    chain_id: &i64,
    transaction_cost: &u128,
    cmc_id: Option<String>,
    symbol: Option<String>,
    decimals: Option<i32>,
    timestamp: Option<String>,
) -> String {
    let cmc_api_key = std::env::var("CMC_API_KEY")
        .expect("CMC_API_KEY must be set")
        .parse::<String>()
        .unwrap();

    let token_symbol;
    let token_decimals;

    let cmc_api = match cmc_id.clone() {
        Some(id) => {
            if symbol == None || decimals == None {
                error!("CMC_ID not found");
                return String::from("0");
            }
            token_symbol = symbol.unwrap();
            token_decimals = decimals.unwrap() as u32;
            let cmc_api_url = match timestamp {
                Some(ref time) => {
                    format!("https://pro-api.coinmarketcap.com/v3/cryptocurrency/quotes/historical?id={}&time_start={}&count=1", id, time)
                }
                None => {
                    format!(
                        "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id={}",
                        id
                    )
                }
            };
            cmc_api_url
        }
        None => {
            (token_symbol, token_decimals) = get_native_token_cmc_id(*chain_id);
            let cmc_api_url = match timestamp {
                Some(ref time) => {
                    format!("https://pro-api.coinmarketcap.com/v3/cryptocurrency/quotes/historical?id={}&time_start={}&count=1", token_symbol, time)
                }
                None => {
                    format!(
                        "https://pro-api.coinmarketcap.com/v2/cryptocurrency/quotes/latest?id={}",
                        token_symbol
                    )
                }
            };
            cmc_api_url
        }
    };

    let mut headers = HeaderMap::new();
    match HeaderValue::from_str(&cmc_api_key) {
        Ok(header_value) => {
            headers.insert("X-CMC_PRO_API_KEY", header_value);
        }
        Err(e) => {
            error!("Invalid header value: {}", e);
            return String::from("0");
        }
    }

    headers.insert(CONTENT_TYPE, HeaderValue::from_static("application/json"));

    let api_client = reqwest::Client::new();
    let mut transaction_fees_usd = String::from("0");

    let response_result = api_client.get(&cmc_api).headers(headers).send().await;

    match response_result {
        Ok(response) => {
            let token_getter;
            if cmc_id == None {
                token_getter = token_symbol;
            } else {
                token_getter = cmc_id.unwrap();
            }
            match timestamp {
                Some(_time) => {
                    let json_result = response.json::<ApiResponseHistory>().await;

                    match json_result {
                        Ok(api_response) => {
                            if let Some(tokens) = api_response.data.get(&token_getter) {
                                if tokens.quotes.len() > 0 {
                                    if let Some(quote) = tokens.quotes[0].quote.get("USD") {
                                        match quote.price {
                                            Some(price) => {
                                                info!("Price of {}: ${}", token_getter, price);
                                                transaction_fees_usd = ((*transaction_cost as f64
                                                    / 10_f64.powf(token_decimals as f64))
                                                    * price)
                                                    .to_string();
                                                info!(
                                                    "Transaction fee in usd {:?}",
                                                    transaction_fees_usd
                                                );
                                            }
                                            None => {
                                                error!("Price not available for {}", token_getter)
                                            }
                                        }
                                    } else {
                                        error!("USD quote not available.");
                                    }
                                } else {
                                    error!("CMC Api failed to fetch historical price of the token with cmc_id {:?}", token_getter);
                                }
                            } else {
                                error!("No data found in response.");
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse JSON: {}", e);
                        }
                    };
                }
                None => {
                    let json_result = response.json::<ApiResponse>().await;

                    match json_result {
                        Ok(api_response) => {
                            if let Some(tokens) = api_response.data.get(&token_getter) {
                                if let Some(quote) = tokens.quote.get("USD") {
                                    match quote.price {
                                        Some(price) => {
                                            info!("Price of {}: ${}", token_getter, price);
                                            transaction_fees_usd = ((*transaction_cost as f64
                                                / 10_f64.powf(token_decimals as f64))
                                                * price)
                                                .to_string();
                                            info!(
                                                "Transaction fee in usd {:?}",
                                                transaction_fees_usd
                                            );
                                        }
                                        None => error!("Price not available for {}", token_getter),
                                    }
                                } else {
                                    error!("USD quote not available.");
                                }
                            } else {
                                error!("No data found in response.");
                            }
                        }
                        Err(e) => {
                            error!("Failed to parse JSON: {}", e);
                        }
                    };
                }
            }
        }
        Err(e) => {
            error!("Request failed: {}", e);
        }
    };

    transaction_fees_usd
}

pub async fn update_intent_state(
    intent_id: &i64,
    version: i32,
    stage: &str,
    transaction_hash: FixedBytes<32>,
    client: &Arc<Client>,
    provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend>,
    order_id: &i64,
    chain_id: i64,
    initiator_address: String,
) {
    let query =
        "INSERT INTO intent_state VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, DEFAULT, $7, $8, $9, $10, $11) ON CONFLICT (intent_id, version, transaction_hash) DO NOTHING";
    let timestamp = std::time::SystemTime::now();

    let (gas_used, transaction_cost) =
        match provider.get_transaction_receipt(transaction_hash).await {
            Ok(receipt) => {
                let (gas_used, gas_price) = match receipt {
                    Some(tx_receipt) => {
                        let gas_used = tx_receipt.gas_used as i64;
                        let gas_price = tx_receipt.effective_gas_price;
                        (gas_used, gas_price)
                    }
                    None => {
                        error!("Transaction receipt not found");
                        (0 as i64, 0u128)
                    }
                };
                let transaction_cost = (gas_used as u128) * gas_price;
                (gas_used, transaction_cost)
            }
            Err(_e) => (0 as i64, 0u128),
        };
    let txn_hash_str = transaction_hash.to_string();

    let transaction_cost_usd =
        get_usd_value_of_native(&chain_id, &transaction_cost, None, None, None, None).await;

    let _intent_state_response = match client
        .execute(
            query,
            &[
                &intent_id,
                &version,
                &txn_hash_str,
                &stage,
                &timestamp,
                &gas_used,
                &order_id,
                &chain_id,
                &initiator_address,
                &transaction_cost.to_string(),
                &transaction_cost_usd,
            ],
        )
        .await
    {
        Ok(_res) => {
            info!(target: "EVM update_intent_state", "Intent State Updated for intent id: {intent_id} to version: {version}");
        }
        Err(e) => {
            error!(target: "EVM update_intent_state", "Failed to update intent state for intent id: {intent_id} to version: {version}: {:?}", e);
        }
    };
}

pub async fn fetch_intent_initiator(intent_id: i64, client: &Arc<Client>) -> String {
    let query = "SELECT owner_address FROM intent WHERE intent_id = $1";
    let initiator_address = match client.query_one(query, &[&intent_id]).await {
        Ok(res) => {
            let owner_address: String = res.get("owner_address");
            owner_address
        }
        Err(e) => {
            error!(target: "EVM Fetch intent owner", "Failed to fetch intent owner for intent id {:?} : {:?}", intent_id, e);
            "".to_string()
        }
    };
    initiator_address
}

pub async fn get_fees_data(
    source_chain_id: &str,
    destination_chain_id: &str,
    token_in: &str,
    token_out: &str,
    amount_in: &str,
    solana_chain_id: &str,
) -> ResultCosts {
    let request_client = reqwest::Client::new();

    let mut payload_token_in = String::from(token_in);
    let mut payload_token_out = String::from(token_out);

    if source_chain_id != solana_chain_id {
        let without_prefix = token_in.trim_start_matches("0x"); // Remove "0x" prefix
        let trimmed = &without_prefix[without_prefix.len().saturating_sub(40)..]; // Keep only the last 40 chars
        payload_token_in = format!("0x{}", trimmed)
    } else {
        if token_in.starts_with("0x") {
            let without_prefix = token_in.trim_start_matches("0x"); // Remove "0x" prefix
            if let Ok(token_bytes) = hex::decode(without_prefix) {
                if token_bytes.len() == 32 {
                    if let Ok(array) = <[u8; 32]>::try_from(token_bytes) {
                        let pubkey_token = solana_sdk::pubkey::Pubkey::from(array);
                        info!("pubkey token {:?}", pubkey_token.to_string());
                        payload_token_in = pubkey_token.to_string();
                    }
                }
            }
        }
    }

    if destination_chain_id != solana_chain_id {
        let without_prefix = token_out.trim_start_matches("0x"); // Remove "0x" prefix
        let trimmed = &without_prefix[without_prefix.len().saturating_sub(40)..]; // Keep only the last 40 chars
        payload_token_out = format!("0x{}", trimmed)
    } else {
        if token_out.starts_with("0x") {
            let without_prefix = token_out.trim_start_matches("0x"); // Remove "0x" prefix
            if let Ok(token_bytes) = hex::decode(without_prefix) {
                if token_bytes.len() == 32 {
                    if let Ok(array) = <[u8; 32]>::try_from(token_bytes) {
                        let pubkey_token = Pubkey::from(array);
                        info!("pubkey token {:?}", pubkey_token.to_string());
                        payload_token_out = pubkey_token.to_string();
                    }
                }
            }
        }
    }

    let fees_request_payload = json!({
        "from_chain": source_chain_id,
        "to_chain": destination_chain_id,
        "from_token": payload_token_in, // bytes32
        "to_token": payload_token_out, // bytes32
        "from_amount": amount_in,
        "from_address": ""
    });
    let url = "https://price-feed.inclusivelayer.com/quote";
    let fee_data_response = match request_client
        .post(url)
        .json(&fees_request_payload)
        .send()
        .await
    {
        Ok(res) => {
            let fee_data = match res.json::<QuoteApiResponse>().await {
                Ok(data) => data.fee_data,
                Err(_e) => {
                    error!("Error in parsing response of fetching fees from the quotation service {:?}", _e);
                    ResultCosts {
                        destination_cost: AmountTypes {
                            value: None,
                            value_type: None,
                        },
                        inclusive_layer_fee: AmountTypes {
                            value: None,
                            value_type: None,
                        },
                        provider_fee: ThirdPartyFeeResult {
                            flat_fee: AmountTypes {
                                value: None,
                                value_type: None,
                            },
                            provider: None,
                            solver_fee: AmountTypes {
                                value: None,
                                value_type: None,
                            },
                            variable_fee: AmountTypes {
                                value: None,
                                value_type: None,
                            },
                        },
                        source_cost: AmountTypes {
                            value: None,
                            value_type: None,
                        },
                    }
                }
            };
            fee_data
        }
        Err(_e) => {
            info!("Error in fetching fees from the quotation service {:?}", _e);
            ResultCosts {
                destination_cost: AmountTypes {
                    value: None,
                    value_type: None,
                },
                inclusive_layer_fee: AmountTypes {
                    value: None,
                    value_type: None,
                },
                provider_fee: ThirdPartyFeeResult {
                    flat_fee: AmountTypes {
                        value: None,
                        value_type: None,
                    },
                    provider: None,
                    solver_fee: AmountTypes {
                        value: None,
                        value_type: None,
                    },
                    variable_fee: AmountTypes {
                        value: None,
                        value_type: None,
                    },
                },
                source_cost: AmountTypes {
                    value: None,
                    value_type: None,
                },
            }
        }
    };
    fee_data_response
}

pub async fn get_amount_usd_value(
    token_in: String,
    chain_id: String,
    amount: Option<String>,
    client: &Arc<Client>,
    timestamp: Option<String>,
) -> String {
    let tokens_query = "SELECT t.id, t.cmc_id, t.ticker, tc.decimals, cm.chain_id, tc.network, LOWER(tc.address_bytes32) AS token_address_bytes32
        FROM tokens t
        JOIN token_chains tc ON t.id = tc.token_id
        JOIN chain_metadata cm ON cm.network_name = tc.network
        WHERE LOWER(tc.address_bytes32) = LOWER($1) AND chain_id = $2";

    let log_target = "Amount USD Value";

    let inclusive_layer_fee_usd = match &amount {
        Some(fee) => {
            let token_data = match client
                .query_one(tokens_query, &[&token_in, &chain_id])
                .await
            {
                Ok(res) => {
                    let cmc_id: String = res.get("cmc_id");
                    let symbol: String = res.get("ticker");
                    let decimals: i32 = res.get("decimals");
                    let inclusive_layer_fee_usd = get_usd_value_of_native(
                        &chain_id.parse::<i64>().unwrap(),
                        &fee.parse::<u128>().unwrap(),
                        Some(cmc_id),
                        Some(symbol),
                        Some(decimals),
                        timestamp,
                    )
                    .await;
                    inclusive_layer_fee_usd
                }
                Err(_e) => {
                    error!(target: log_target, "Failed to get inclusive_layer_fee_usd, error: {:?}", _e);
                    String::from("0.0")
                }
            };
            token_data
        }
        None => String::from("0.0"),
    };
    inclusive_layer_fee_usd
}

// Function to listen to events from a specific RPC and contract
pub async fn process_evm_events<S>(
    mut stream: S,
    client: Arc<Client>,
    chain_id: i64,
    chain_provider: alloy::providers::RootProvider<alloy::pubsub::PubSubFrontend>,
    solana_chain_id: &str,
) where
    S: Stream<Item = Log> + Unpin,
{
    while let Some(log) = stream.next().await {
        if let Err(e) = try_parse_evm_event(
            client.clone(),
            chain_id,
            chain_provider.clone(),
            solana_chain_id,
            log,
        )
        .await
        {
            error!(target: "process_evm_events",  "Error processing evm event: {:?}", e);
        }
    }
}

async fn try_parse_evm_event(
    client: Arc<Client>,
    chain_id: i64,
    chain_provider: RootProvider<PubSubFrontend>,
    solana_chain_id: &str,
    log: Log,
) -> anyhow::Result<()> {
    match log.topic0() {
        Some(&IntentLibV2::IntentSubmitted::SIGNATURE_HASH) => {
            handle_intent_submitted_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&IntentProcessorV2::IntentFees::SIGNATURE_HASH) => {
            handle_intent_fees_event(log, &client, chain_id).await?;
        }
        Some(&IntentLibV2::SolutionSubmitted::SIGNATURE_HASH) => {
            handle_solution_submitted_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&IntentLibV2::OrderCreated::SIGNATURE_HASH) => {
            handle_order_created_event(log, &client, chain_id, chain_provider, solana_chain_id).await?;
        }
        Some(&IntentProcessorV2::AcknowledgementReceived::SIGNATURE_HASH) => {
            handle_acknowledgement_received_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&IntentProcessorV2::DepositReceived::SIGNATURE_HASH) => {
            handle_deposit_received_event(log, &client, chain_provider).await?;
        }
        Some(&Vault::ReceivedMessageOnVault::SIGNATURE_HASH) => {
            handle_received_message_on_vault_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&Vault::MessageDispatchedFromVault::SIGNATURE_HASH) => {
            handle_message_dispatched_from_vault_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&solidity_structs::DebridgeOrderCreated::SIGNATURE_HASH) => {
            handle_debridge_order_created_event(log, &client).await?;
        }
        Some(&UniswapV3FactoryLib::PoolCreated::SIGNATURE_HASH) => {
            handle_uniswap_pool_created_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&UniswapV3PoolLib::Swap::SIGNATURE_HASH) => {
            handle_uniswap_swap_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&UniswapV3PoolLib::Mint::SIGNATURE_HASH) => {
            handle_uniswap_mint_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&UniswapV3PoolLib::MintByProjectManager::SIGNATURE_HASH) => {
            handle_uniswap_mint_by_pm_event(log, &client, chain_id, chain_provider).await?;
        }
        Some(&UniswapV3PoolLib::Burn::SIGNATURE_HASH) => {
            handle_uniswap_burn_event(log, &client, chain_id, chain_provider).await?;
        }
        _ => {
            warn!("\ndidn't match any event, {:?}", log);
        }
    }
    Ok(())
}

pub async fn insert_liquidity_event(
    client: &Arc<Client>,
    pool_address: String,
    user_address: String,
    is_add: bool,
    is_manager: bool,
    position_id: Option<String>,
    amount_token0: Decimal,
    amount_token1: Decimal,
    liquidity_amount: i64,
    fee_amount_0: Option<i64>,
    fee_amount_1: Option<i64>,
    transaction_hash: String,
    _block_number: i64,
    timestamp: SystemTime,
    chain_id: i64,
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

    match client
        .execute(
            query,
            &[
                &pool_address,
                &user_address,
                &is_add,
                &position_id,
                &Decimal::from(amount_token0),
                &Decimal::from(amount_token1),
                &chain_id,
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
            bail!("Failed to insert liquidity data: {:?}", e);
        }
    }
}

pub async fn get_token_data(token_address: Address, chain_provider: RootProvider<PubSubFrontend>) -> (i32, String, String) {
    let log_target = "EVM get_token_data";
    let token_instance = token::Token::new(token_address, chain_provider.clone());
    let decimals = match token_instance.decimals().call().await {
        Ok(decimals) => decimals._0 as i32,
        Err(e) => {
            error!(target: log_target, "Error fetching token decimals for {}: {:?}", token_address, e);
            18 // Default to 18 if fetching fails
        }
    };

    let ticker = match token_instance.symbol().call().await {
        Ok(symbol) => symbol._0,
        Err(e) => {
            error!(target: log_target, "Error fetching token symbol for {}: {:?}", token_address, e);
            "Unknown".to_string() // Default to "Unknown" if fetching fails
        }
    };

    let full_name = match token_instance.name().call().await {
        Ok(name) => name._0,
        Err(e) => {
            error!(target: log_target, "Error fetching token name for {}: {:?}", token_address, e);
            "Unknown".to_string() // Default to "Unknown" if fetching fails
        }
    };

    info!(target: log_target, "Token data for {}: Decimals: {}, Ticker: {}, Full Name: {}", token_address, decimals, ticker, full_name);
    
    (decimals, ticker, full_name)
}

pub async fn get_liquidity_provider_address_evm(
    transaction_receipt: Result<Option<TransactionReceipt>, alloy::transports::RpcError<alloy::transports::TransportErrorKind>>, 
    client: &Arc<Client>, 
    periphery_address: String
) -> Option<String> {
    let log_target = "EVM get_liquidity_provider_address_evm";
    let mut liquidity_user_address: Option<String> = None;

    match transaction_receipt {
        Ok(Some(receipt)) => {
            let receipt_logs = receipt.inner.logs();
            for log in receipt_logs {
                let primitive_log: alloy::primitives::Log = alloy::primitives::Log {
                    address: log.address(),
                    data: log.data().clone(),
                };
                let decoded = Transfer::decode_log(&primitive_log, true).unwrap();
                if decoded.address.to_string().to_lowercase() == periphery_address.to_lowercase() {
                    liquidity_user_address = Some(decoded.to.to_string());
                    break;
                }
            }
        }
        Ok(None) => {
            error!(target: log_target, "Transaction receipt not found");
        }
        Err(e) => {
            error!(target: log_target, "Error fetching transaction receipt: {:?}", e);
        }
    }
    liquidity_user_address
}

pub async fn fallback_fetch_pm_address(
    client: &Arc<Client>,
    pool_address: &str,
    log_target: &str,
) -> String {
    let query = "SELECT project_manager FROM pools WHERE LOWER(pool_address) = LOWER($1)";
    match client.query_one(query, &[&pool_address]).await {
        Ok(row) => row.get::<_, String>("project_manager"),
        Err(e) => {
            error!(target: log_target, "Failed to fetch project manager address from pools table: {:?}", e);
            String::new() // Fallback if query fails
        }
    }
}

pub async fn check_vault_initiated_transaction(
    chain_provider: RootProvider<PubSubFrontend>, 
    db_client: &Arc<Client>,
    transaction_hash: FixedBytes<32>
) -> bool {
    let log_target = "EVM check_vault_initiated_transaction";
    let query = "SELECT transaction_hash FROM received_message_on_vault WHERE LOWER(transaction_hash) = LOWER($1)";
    match db_client.query_one(query, &[&transaction_hash.to_string()]).await {
        Ok(row) => {
            let db_transaction_hash: String = row.get("transaction_hash");
            if db_transaction_hash.to_lowercase() == transaction_hash.to_string().to_lowercase() {
                info!(target: log_target, "Transaction {} is initiated by vault", transaction_hash);
                return true;
            } else {
                check_vault_initiated_transaction_log(chain_provider, transaction_hash).await
            }
        }
        Err(e) => {
            error!(target: log_target, "Failed to check if transaction is initiated by vault: {:?}", e);
            // fallback decode the transaction receipt and check for received_message_on_vault event
            check_vault_initiated_transaction_log(chain_provider, transaction_hash).await
        }
    }
}

async fn check_vault_initiated_transaction_log(chain_provider: RootProvider<PubSubFrontend>, transaction_hash: FixedBytes<32>) -> bool {
    let log_target = "EVM check_vault_initiated_transaction_log";
    let transaction_receipt = chain_provider.get_transaction_receipt(transaction_hash).await;
    match transaction_receipt {
        Ok(Some(receipt)) => {
            let receipt_logs = receipt.inner.logs();
            for log in receipt_logs {
                if log.topics()[0] == Vault::ReceivedMessageOnVault::SIGNATURE_HASH {
                    info!(target: log_target, "Transaction {} is initiated by vault based on receipt logs", transaction_hash);
                    return true;
                }
            }
            return false;
        }
        Ok(None) => {
            error!(target: log_target, "Transaction receipt not found for {}", transaction_hash);
            return false;
        }
        Err(e) => {
            error!(target: log_target, "Error fetching transaction receipt for {}: {:?}", transaction_hash, e);
            return false;
        }
    }
}
