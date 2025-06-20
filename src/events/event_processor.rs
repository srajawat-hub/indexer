use alloy::primitives::{Address, FixedBytes};
use alloy::providers::{Provider, RootProvider};
use alloy::pubsub::PubSubFrontend;
use alloy::rpc::types::Log;
use alloy::sol_types::SolValue;
use alloy::sol_types::SolEvent;
use anyhow::{bail, Context};
use futures_util::stream::StreamExt;
use futures_util::Stream;
use log::{debug, error, info};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use rust_decimal::prelude::FromPrimitive;
use rust_decimal::Decimal;
use serde::Deserialize;
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use std::collections::HashMap;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_postgres::Client;

use crate::solidity_structs::uniswap_v3_factory_lib::UniswapV3FactoryLib;
use crate::solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib;
use crate::solidity_structs::uniswap_v3_pool_lib::UniswapV3PoolLib::UniswapV3PoolLibInstance;
use crate::solidity_structs::{
    self, token, AcknowledgementMetadataLaunchpadAddLiquidity, AcknowledgementMetadataLaunchpadRemoveLiquidity, AcknowledgementMetadataLaunchpadSwap, AcknowledgementMetadataStake, AcknowledgementMetadataTransact, AmountTypes, CreatedOrder, DispatchId, IntentProcessorBoundMessageAcknowledgementData, IntentProcessorBoundMessageDepositData, PoolType, ProcessId, QuoteApiResponse, ReceiverUserAddressData, ReceiverVaultData, ResultCosts, SolidityAcknowledgementMetadata, SolidityIntentProcessorBoundMessage, SolidityOrder, SolidityVaultBoundMessage, ThirdPartyFeeResult, TokenLaunchType, VaultBoundMessagePlaceOrderData
};
use crate::solidity_structs::{
    intent_lib_v2::IntentLibV2, intent_processor::IntentProcessorV2, vault::Vault,
};
use crate::utils::{chain_id_to_chain_name, unix_to_system_time};

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

fn get_native_token_cmc_id(chain_id: i64) -> (String, u32) {
    match chain_id {
        137 => (String::from("28321"), 18),      // pol
        1399811149 => (String::from("5426"), 9), // sol
        18082 => (String::from("3408"), 18),     // usdc
        _ => (String::from("1027"), 18),         // ETH
    }
}

fn get_wrapped_native_token_address(chain_id: i64) -> String {
    match chain_id {
        1 => "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(), // Ethereum Mainnet - WETH
        137 => "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270".to_string(), // Polygon Mainnet - WMATIC
        42161 => "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1".to_string(), // Arbitrum One - WETH
        10 => "0x4200000000000000000000000000000000000006".to_string(), // Optimism Mainnet - WETH
        8453 => "0x4200000000000000000000000000000000000006".to_string(), // Base Mainnet - WETH
        18083 => "0x6a79ca29282AC295679a14A8274300e5842e45e5".to_string(), // Inclusive Layer Testnet
        18082 => "0x0000000000000000000000000000000000000000".to_string(), // Inclusive Layer
        _ => "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(), // Default to Ethereum WETH
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

async fn get_fees_data(
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
        }
        Some(&IntentProcessorV2::IntentFees::SIGNATURE_HASH) => {
            let IntentProcessorV2::IntentFees {
                intentId,
                feeAmount,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "IntentFees";
            info!(target: log_target, "IntentProcessorV2::IntentFees with intent_id {intentId} and feeAmount {feeAmount}");

            let fee_amount = feeAmount.to_string();
            let intent_id = intentId as i64;
            let query = "UPDATE intent SET feeamount = $1 WHERE intent_id = $2";

            let intent_rows_updated = match client.execute(query, &[&fee_amount, &intent_id]).await
            {
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
        }
        Some(&IntentLibV2::SolutionSubmitted::SIGNATURE_HASH) => {
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
        }
        Some(&IntentLibV2::OrderCreated::SIGNATURE_HASH) => {
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
        }
        Some(&IntentProcessorV2::AcknowledgementReceived::SIGNATURE_HASH) => {
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
            let intent_id_query = "SELECT intent_id, token_out, destination_chain_id, timestamp FROM order_created WHERE order_id = $1";
            let intent_id_response = match client.query_one(intent_id_query, &[&order_id]).await {
                Ok(row) => row,
                Err(e) => {
                    error!(target: log_target, "Error in IntentProcessorV2::AcknowledgementReceived for order_id {:?}: {:?}", order_id, e);
                    bail!("Error in IntentProcessorV2::AcknowledgementReceived for order_id {:?}: {:?}", order_id, e);
                }
            };
            let intent_id: i64 = intent_id_response.get("intent_id");
            let token_out: String = intent_id_response.get("token_out");
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

            let decoded_ack_metadata =
                SolidityAcknowledgementMetadata::abi_decode(metadata.as_ref(), true).unwrap();
            if decoded_ack_metadata.data.len() > 0 {
                let mut actual_amount = String::new();
                let metadata_variant = decoded_ack_metadata.enumVariant as u8;
                // TODO: Add launchpad ack variants as well
                if metadata_variant == 1 {
                    // stake
                    let metadata_data = AcknowledgementMetadataStake::abi_decode(
                        decoded_ack_metadata.data.as_ref(),
                        true,
                    )
                    .unwrap();
                    actual_amount = metadata_data.amountCredited.to_string();
                } else if metadata_variant == 2 {
                    let metadata_data = AcknowledgementMetadataLaunchpadSwap::abi_decode(
                        decoded_ack_metadata.data.as_ref(), true
                    ).unwrap();
                    actual_amount = metadata_data.receivedAmount.to_string();
                } else if metadata_variant == 3 {
                    let metadata_data = AcknowledgementMetadataLaunchpadAddLiquidity::abi_decode(
                        decoded_ack_metadata.data.as_ref(), true
                    ).unwrap();
                    actual_amount = metadata_data.amount1.to_string();
                } else if metadata_variant == 4 {
                    let metadata_data = AcknowledgementMetadataLaunchpadRemoveLiquidity::abi_decode(
                        decoded_ack_metadata.data.as_ref(), true
                    ).unwrap();
                    actual_amount = metadata_data.amount1.to_string();
                } else {
                    // transact
                    let metadata_data = AcknowledgementMetadataTransact::abi_decode(
                        decoded_ack_metadata.data.as_ref(),
                        true,
                    )
                    .unwrap();
                    actual_amount = metadata_data.amount.to_string();
                }

                let update_order_query = "
                    UPDATE order_created
                    SET amount_out = $1, amount_out_usd = $2
                    WHERE order_id = $3
                    AND (SELECT COUNT(*) FROM order_created WHERE order_id = $3) = 1
                    ";

                // TODO: update amount_out_usd here as well in order_created
                let amount_out_usd = get_amount_usd_value(
                    token_out,
                    destination_chain_id,
                    Some(actual_amount.clone()),
                    &client,
                    None,
                )
                .await;

                let order_rows_updated = client
                    .execute(
                        update_order_query,
                        &[&actual_amount, &amount_out_usd, &order_id],
                    )
                    .await
                    .unwrap();
                info!(target: log_target,
                    "updated actual amount for order, updated rows count {:?}",
                    order_rows_updated
                );
            }
        }
        Some(&IntentProcessorV2::DepositReceived::SIGNATURE_HASH) => {
            let IntentProcessorV2::DepositReceived {
                userAddress,
                tokenAddress,
                chainId,
                amount,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "DepositReceived";
            info!(target: log_target, "IntentProcessorV2::DepositReceived from user {userAddress}");
            let chain_id = chainId.to_string();
            let amount = amount.to_string();
            let user_address = userAddress.to_string();
            let token_address = tokenAddress.to_string();
            let timestamp = std::time::SystemTime::now();

            let transaction_hash = log.transaction_hash.unwrap();

            // get message_id from hyperlane process event; get the event log from the transaction receipt
            let process_message_id = match chain_provider
                .get_transaction_receipt(transaction_hash)
                .await
            {
                Ok(receipt) => match receipt {
                    Some(tx_receipt) => {
                        let receipt_logs = tx_receipt.inner.logs();
                        let process_id_log = receipt_logs
                            .iter()
                            .find(|log| log.topics()[0] == ProcessId::SIGNATURE_HASH);
                        let message_process_id = match process_id_log {
                            Some(log) => {
                                let primitive_log = alloy::primitives::Log {
                                    address: log.address(),
                                    data: log.data().clone(),
                                };
                                let decoded = ProcessId::decode_log(&primitive_log, true).unwrap();
                                let process_message_id = decoded.messageId;
                                Some(process_message_id)
                            } // convert log into primite log
                            None => None,
                        };
                        message_process_id
                    }
                    None => None,
                },
                Err(_e) => None,
            };
            let deposit_status = DepositStatus::Done as i32;
            match process_message_id {
                Some(id) => {
                    let message_id = id.to_string();
                    let query = "INSERT INTO deposit_received VALUES (DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (message_id) DO UPDATE SET status = EXCLUDED.status WHERE deposit_received.status <> 1;";
                    let _deposit_transaction = match client
                        .execute(
                            query,
                            &[
                                &user_address,
                                &token_address,
                                &chain_id,
                                &amount,
                                &timestamp,
                                &transaction_hash.to_string(),
                                &message_id,
                                &deposit_status,
                            ],
                        )
                        .await
                    {
                        Ok(_row) => {
                            info!(target: log_target, "Successfully updated deposit status with message_id {:?}", message_id);
                        }
                        Err(_e) => {
                            error!(target: log_target, "Error updating deposit status with message_id {:?} with error {:?}", message_id, _e);
                        }
                    };
                }
                None => {
                    // if not, we will have to add this into history
                    let query = "INSERT INTO deposit_received VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
                    let response = client
                        .execute(
                            query,
                            &[
                                &user_address,
                                &token_address,
                                &chain_id,
                                &amount,
                                &timestamp,
                                &transaction_hash.to_string(),
                                &deposit_status,
                            ],
                        )
                        .await
                        .unwrap();
                    info!(
                        target: log_target, "IntentProcessorV2::DepositReceived inserted a fallback response {:?}",
                        response
                    );
                }
            }
        }
        Some(&Vault::ReceivedMessageOnVault::SIGNATURE_HASH) => {
            let Vault::ReceivedMessageOnVault {
                origin,
                sender,
                message,
                provider,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "EVM Vault::ReceivedMessageOnVault";
            info!(target: log_target, "Vault::ReceivedMessageOnVault from id {origin} by {sender}, message = {message}, using provider = {provider}");

            let message_slice = message.as_ref();
            let decoded_message = match SolidityVaultBoundMessage::abi_decode(message_slice, true) {
                Ok(res) => res,
                Err(e) => {
                    error!(target: log_target, "Failed to decode SolidityVaultBoundMessage in Vault::ReceivedMessageOnVault: {:?}", e);
                    bail!("Failed to decode SolidityVaultBoundMessage in Vault::ReceivedMessageOnVault: {:?}", e);
                }
            };

            let decoded_message_data = match VaultBoundMessagePlaceOrderData::abi_decode(
                decoded_message.data.as_ref(),
                true,
            ) {
                Ok(data) => data,
                Err(e) => {
                    error!(target: log_target, "Failed to decode VaultBoundMessagePlaceOrderData in Vault::ReceivedMessageOnVault: {:?}", e);
                    bail!("Failed to decode VaultBoundMessagePlaceOrderData in Vault::ReceivedMessageOnVault: {:?}", e);
                }
            };

            let timeout_unix_timestamp_in_sec =
                decoded_message_data.order.timeoutUnixTimestampInSec as i64;

            let block_number = log.block_number.unwrap() as i64;
            let intent_id = decoded_message_data.order.intentId as i64;
            let order_id = decoded_message_data.order.orderId as i64;
            let origin_domain_id = origin as i32;
            let sender_address = decoded_message_data.order.initiatorAddress.to_string();
            let provider = provider as i32;
            let message = message.to_string();
            let timestamp = std::time::SystemTime::now();

            let transaction_hash = log.transaction_hash.unwrap();

            let dln_order_id = match chain_provider
                .get_transaction_receipt(transaction_hash)
                .await
            {
                Ok(receipt) => match receipt {
                    Some(tx_receipt) => {
                        let receipt_logs = tx_receipt.inner.logs();
                        let order_id_log = receipt_logs
                            .iter()
                            .find(|log| log.topics()[0] == CreatedOrder::SIGNATURE_HASH);
                        let dln_order_id = match order_id_log {
                            Some(log) => {
                                let primitive_log = alloy::primitives::Log {
                                    address: log.address(),
                                    data: log.data().clone(),
                                };
                                let decoded =
                                    CreatedOrder::decode_log(&primitive_log, true).unwrap();
                                let debridge_order_id = decoded.orderId;
                                Some(debridge_order_id)
                            } // convert log into primite log
                            None => None,
                        };
                        dln_order_id
                    }
                    None => None,
                },
                Err(_e) => None,
            };

            let query = "INSERT INTO received_message_on_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12) ON CONFLICT (transaction_hash) DO NOTHING";
            match client
                .execute(
                    query,
                    &[
                        &intent_id,
                        &origin_domain_id,
                        &sender_address,
                        &message,
                        &provider,
                        &transaction_hash.to_string(),
                        &block_number,
                        &timestamp,
                        &chain_id,
                        &order_id,
                        &dln_order_id.map(hex::encode),
                        &timeout_unix_timestamp_in_sec,
                    ],
                )
                .await
            {
                Ok(res) => {
                    info!(
                        target: log_target,
                        "Vault::ReceivedMessageOnVault inserted response {:?}",
                        res
                    );
                }
                Err(e) => {
                    error!(target: log_target, "Failed to add data into data: {e}");
                }
            };

            let initiator_address: String = fetch_intent_initiator(intent_id, &client).await;

            update_intent_state(
                &intent_id,
                IntentVersions::ReceivedMessageOnVault as i32,
                &IntentStage::Processing.to_string(),
                log.transaction_hash.unwrap(),
                &client,
                chain_provider.clone(),
                &order_id,
                chain_id,
                initiator_address,
            )
            .await;
        }
        Some(&Vault::MessageDispatchedFromVault::SIGNATURE_HASH) => {
            let Vault::MessageDispatchedFromVault {
                sender,
                destinationDomain,
                provider,
                message,
            } = log.log_decode().unwrap().inner.data;
            debug!(target: "EVM Vault::MessageDispatchedFromVault", "\nMessageDispatchedFromVault log - {:?}", log);
            // have to get intentId here.
            info!(target: "EVM Vault::MessageDispatchedFromVault", "Vault::MessageDispatchedFromVault received from {sender} to destination {destinationDomain}");

            let decoded_message = match SolidityIntentProcessorBoundMessage::abi_decode(
                message.as_ref(),
                true,
            ) {
                Ok(res) => res,
                Err(e) => {
                    error!(target: "EVM Vault::MessageDispatchedFromVault", "Failed to decode SolidityIntentProcessorBoundMessage in Vault::MessageDispatchedFromVault: {:?}", e);
                    bail!("Failed to decode SolidityIntentProcessorBoundMessage in Vault::MessageDispatchedFromVault: {:?}", e);
                }
            };

            let message_variant = decoded_message.enumVariant as u8;

            match message_variant {
                2 => {
                    let log_target = "EVM Vault::MessageDispatchedFromVault Ack";
                    // ack
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
                        error!(target: log_target, "Error decoding IntentProcessorBoundMessageAcknowledgement message data");
                        bail!("Error decoding IntentProcessorBoundMessageAcknowledgement message data");
                    };

                    let order_id = decoded_message_data.orderId as i64;
                    let sender_address = log.address().to_string();
                    let destination_domain_id = destinationDomain as i32;
                    let provider = provider as i32;
                    let message = message.to_string();
                    let transaction_hash = log.transaction_hash.unwrap();

                    let block_number = log.block_number.unwrap() as i64;
                    let timestamp = std::time::SystemTime::now();

                    // fetch intent_id
                    let intent_id_query = "SELECT intent_id FROM order_created WHERE order_id = $1";
                    let intent_id_response = match client
                        .query_one(intent_id_query, &[&order_id])
                        .await
                    {
                        Ok(row) => row,
                        Err(e) => {
                            error!(target: log_target, "Failed to fetch intent_id for event Vault::MessageDispatchedFromVault for order_id {:?}: {:?}", order_id, e);
                            bail!("Failed to fetch intent_id for event Vault::MessageDispatchedFromVault for order_id {:?}: {:?}", order_id, e);
                        }
                    };
                    let intent_id: i64 = intent_id_response.get("intent_id");

                    let query =
                        "INSERT INTO message_dispatched_from_vault VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (transaction_hash) DO NOTHING";
                    match client
                        .execute(
                            query,
                            &[
                                &intent_id,
                                &sender_address,
                                &destination_domain_id,
                                &provider,
                                &message,
                                &transaction_hash.to_string(),
                                &block_number,
                                &timestamp,
                                &order_id,
                            ],
                        )
                        .await
                    {
                        Ok(res) => {
                            info!(
                                target: log_target,
                                "Vault::MessageDispatchedFromVault inserted response {:?}",
                                res
                            );
                        }
                        Err(e) => {
                            error!(target: log_target, "Failed to add data into data");
                        }
                    };

                    let initiator_address: String =
                        fetch_intent_initiator(intent_id, &client).await;

                    update_intent_state(
                        &intent_id,
                        IntentVersions::MessageDispatchedFromVault as i32,
                        &IntentStage::Processing.to_string(),
                        log.transaction_hash.unwrap(),
                        &client,
                        chain_provider.clone(),
                        &order_id,
                        chain_id,
                        initiator_address,
                    )
                    .await;
                }
                3 => {
                    let log_target = "EVM Vault::MessageDispatchedFromVault Deposit";
                    let deposit_message_data = IntentProcessorBoundMessageDepositData::abi_decode(
                        &decoded_message.data,
                        true,
                    )
                    .unwrap();
                    let deposit_user_address = deposit_message_data.userAddress;
                    let user_address = deposit_user_address.to_string();
                    let amount = deposit_message_data.amount.to_string();
                    let token_address = deposit_message_data.tokenAddress.to_string();

                    // deposit message
                    let transaction_hash = log.transaction_hash.unwrap(); // source_transaction_hash
                    let message_id = match chain_provider
                        .get_transaction_receipt(transaction_hash)
                        .await
                    {
                        Ok(receipt) => match receipt {
                            Some(tx_receipt) => {
                                let receipt_logs = tx_receipt.inner.logs();

                                let dispatch_id_log = receipt_logs
                                    .iter()
                                    .find(|log| log.topics()[0] == DispatchId::SIGNATURE_HASH);

                                let dispatch_id = match dispatch_id_log {
                                    Some(log) => {
                                        let primitive_log = alloy::primitives::Log {
                                            address: log.address(),
                                            data: log.data().clone(),
                                        };
                                        let decoded =
                                            DispatchId::decode_log(&primitive_log, true).unwrap();
                                        let message_dispatch_id = decoded.messageId;
                                        Some(message_dispatch_id.to_string())
                                    }
                                    None => None,
                                };
                                dispatch_id
                            }
                            None => None,
                        },
                        Err(_e) => None,
                    };

                    info!(target: log_target, "message_id from source {:?}", message_id);
                    info!(target: log_target, "user_address from source {:?}", user_address);

                    let chain_id = match chain_provider.get_chain_id().await {
                        Ok(id) => id.to_string(),
                        Err(_e) => {
                            error!(target: log_target, "Error fetching the chain id for deposit message source {:?}", _e);
                            String::new()
                        }
                    };
                    let timestamp = std::time::SystemTime::now();
                    let status = DepositStatus::Initialized as i32;

                    let query = "INSERT INTO deposit_received VALUES(DEFAULT, $1, $2, $3, $4, $5, $6, $7, $8)";
                    match client
                        .execute(
                            query,
                            &[
                                &user_address,
                                &token_address,
                                &chain_id,
                                &amount,
                                &timestamp,
                                &transaction_hash.to_string(),
                                &message_id,
                                &status,
                            ],
                        )
                        .await
                    {
                        Ok(res) => {
                            info!(
                                target: log_target,
                                "IntentLib::DepositedFunds inserted response {:?}",
                                res
                            );
                        }
                        Err(e) => {
                            error!(target: log_target, "Failed to insert data for deposit with message_id: {:?}, error: {e}", &message_id);
                        }
                    };
                }
                v => bail!("Unknown message variant {v} in MessageDispatchedFromVault"),
            }
        }
        Some(&solidity_structs::DebridgeOrderCreated::SIGNATURE_HASH) => {
            let log_target = "EVM DebridgeOrderCreated";
            let solidity_structs::DebridgeOrderCreated {
                orderId,
                debridgeOrderId,
            } = log.log_decode().unwrap().inner.data;
            info!(target: log_target, "solidity_structs::DebridgeOrderCreated from {orderId} with debridgeOrderId {debridgeOrderId}");

            let order_id = orderId as i64;

            let debridge_order_id = hex::encode(debridgeOrderId);

            let query =
                "UPDATE received_message_on_vault SET dln_order_id = $1 WHERE order_id = $2";
            let response = client
                .execute(query, &[&debridge_order_id, &order_id])
                .await
                .unwrap();
            info!(target: log_target,
                "solidity_structs::DebridgeOrderCreated updated response {:?}",
                response
            );
        }
        Some(&UniswapV3FactoryLib::PoolCreated::SIGNATURE_HASH) => {
            let UniswapV3FactoryLib::PoolCreated {
                token0,
                token1,
                fee,
                tickSpacing,
                pool,
                launchParams,
            } = log.log_decode().unwrap().inner.data;
            let log_target = "PoolCreated";
            info!(target: log_target, "UniswapV3FactoryLib::PoolCreated: {pool}, {token0}, {token1}");

            let timestamp = SystemTime::now();
            let block_number_i64 = log.block_number.unwrap() as i64;

            let pool_instance = UniswapV3PoolLibInstance::new(pool, chain_provider.clone());
            info!("pool instance {:?}", pool_instance);
            let slot = match pool_instance.slot0().call().await {
                Ok(slot) => {
                    info!("slot {:?}", slot);
                    slot
                },
                Err(e) => {
                    error!(target: log_target, "Error fetching slot0 for pool {}: {:?}", pool, e);
                    bail!("Error fetching slot0 for pool {}: {:?}", pool, e);
                }
            };
            let pool_address = pool.to_string();
            let chain_id_i64 = chain_provider.get_chain_id().await? as i64;
            let token0_addr = token0.to_string();
            let token1_addr = token1.to_string();
            let fee_decimal = fee.to::<i64>();
            let tick_spacing_i64 = tickSpacing.as_i64();
            let pool_type = PoolType::EVM;
            let project_manager = launchParams.projectManager.to_string();
            let metadata_json = serde_json::Value::Null;
            let etp_start_time = unix_to_system_time(launchParams.exclusiveTradingPeriodStart.to());
            let etp_end_time = unix_to_system_time(launchParams.exclusiveTradingPeriodEnd.to());
            let launch_type = if launchParams.tokenLaunchType == 0 {
                TokenLaunchType::FAIR
            }else {
                TokenLaunchType::CURATED
            };
            let initial_sqrt = slot.sqrtPriceX96.to_string();
            let initial_tick_i32 = slot.tick.to_string();
            info!("initial_tick {:?}", initial_tick_i32);
            let token = token::Token::new(token1, chain_provider.clone());
            let token_supply_i64 = match token.totalSupply().call().await {
                Ok(supply) => supply._0.to_string(),
                Err(e) => {
                    error!("Error in fetching token supply from its contract, defaulting to 0");
                    "0".to_string()
                }
            };

            info!(target: log_target, "UniswapV3FactoryLib::PoolCreated - pool_address: {}, chain_id: {}, token_0_address: {}, token_1_address: {}, fee: {}, tick_spacing: {}, pool_type: {:?}, project_manager: {}, block_number: {}, created_at: {:?}, metadata: {:?}, etp_start_time: {:?}, etp_end_time: {:?}, launch_type: {:?}, initial_sqrt_price: {}, initial_tick: {}, token_supply: {}",
                pool_address, chain_id_i64, token0_addr, token1_addr, fee_decimal, tick_spacing_i64, pool_type, project_manager, block_number_i64, timestamp, metadata_json, etp_start_time, etp_end_time, launch_type, initial_sqrt, initial_tick_i32, token_supply_i64);

            // --- perform the INSERT ---
            let query = r#"
                  INSERT INTO pools
                    (pool_address, chain_id, token_0_address, token_1_address, fee,
                     tick_spacing, pool_type, project_manager, block_number, created_at,
                     metadata, etp_start_time, etp_end_time, launch_type, initial_sqrt_price,
                     initial_tick, token_supply, launchpad_token)
                  VALUES
                    ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14::token_launch_type,$15,$16,$17,$18)
                  ON CONFLICT (pool_address) DO NOTHING
                "#;

            let rows = match client
                .execute(
                    query,
                    &[
                        &pool_address,
                        &chain_id_i64,
                        &token0_addr,
                        &token1_addr,
                        &Decimal::from(fee_decimal),
                        &tick_spacing_i64,
                        &pool_type,
                        &project_manager,
                        &block_number_i64,
                        &timestamp,
                        &metadata_json,
                        &etp_start_time,
                        &etp_end_time,
                        &launch_type,
                        &initial_sqrt,
                        &initial_tick_i32.parse::<i32>().unwrap(),
                        &token_supply_i64,
                        &token1_addr, // token1 is the launchpad token for evm pools
                    ],
                )
                .await {
                Ok(rows) => rows,
                Err(e) => {
                    error!(target: log_target, "Error inserting UniswapV3FactoryLib::PoolCreated data: {:?}", e);
                    bail!("Error inserting UniswapV3FactoryLib::PoolCreated data: {:?}", e);
                }};
            info!(
                target: log_target, "UniswapV3FactoryLib::PoolCreated inserted a fallback response {:?}",
                rows
            );

            let token_chains_query = r#"
                INSERT INTO token_chains (token_id, address, decimals, network, address_bytes32) VALUES($1, $2, $3, $4, $5)
            "#;

            let tokens_query = "INSERT INTO tokens (ticker, full_name, is_stable, is_tradable, price_usd, description, launch_date, website, cmc_id) VALUES($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (address, network) DO NOTHING";

            let token_id = &pool_address;
            let network = chain_id_to_chain_name(chain_id);
            let address_bytes32 = format!("{:0>64}", hex::encode(token1));

            let (decimals, ticker, full_name) = get_token_data(token1.clone(), chain_provider.clone()).await;
            let is_stable = false;
            let is_tradable = false;
            let price_usd = Decimal::from(0_i64);
            let description = String::new();
            let launch_date = timestamp.clone();
            let website = String::new();
            let cmc_id = String::new();

            // -- add in the tokens table
            match client
                .execute(
                    token_chains_query,
                    &[
                        &token_id,
                        &token1_addr,
                        &decimals,
                        &network,
                        &address_bytes32
                    ],
                )
                .await
            {
                Ok(rows) => {
                    info!(target: log_target, "Inserted token data into token_chains table: {:?}", rows);
                    match client.execute(tokens_query, &[
                        &ticker,
                        &full_name,
                        &is_stable,
                        &is_tradable,
                        &price_usd,
                        &description,
                        &launch_date,
                        &website,
                        &cmc_id
                    ]).await {
                        Ok(rows) => {
                            info!(target: log_target, "Inserted token data into tokens table: {:?}", rows);
                        }
                        Err(e) => {
                            error!(target: log_target, "Error inserting token chain data into token_chains table: {:?}", e);
                            bail!("Error inserting token chain data into token_chains table: {:?}", e);
                        }
                    }
                }
                Err(e) => {
                    error!(target: log_target, "Error inserting token data into tokens table: {:?}", e);
                    bail!("Error inserting token data into tokens table: {:?}", e);
                }
            };
        }
        Some(&UniswapV3PoolLib::Swap::SIGNATURE_HASH) => {
            let UniswapV3PoolLib::Swap {
                sender,
                recipient,
                amount0,
                amount1,
                sqrtPriceX96,
                liquidity,
                tick,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "Swap";
            info!(target: log_target, "UniswapV3PoolLib::Swap from {sender} to {recipient}");

            let pool_address = log.address().to_string();
            let transaction_hash = log.transaction_hash.unwrap().to_string();
            let block_number = log.block_number.unwrap() as i64;
            let timestamp = match log.block_timestamp {
                Some(ts) => unix_to_system_time(ts),
                None => {
                    error!(target: log_target, "Block timestamp is missing for log: {:?}", log);
                    SystemTime::now() // Fallback to current time if timestamp is missing
                }
            };;
            let sqrt_price = sqrtPriceX96.to_string();
            let liquidity_i64 = liquidity as i64;
            let tick_i32 = tick.as_i32();
            let initiator_user_address = sender.to_string();

            // Query pool to get token addresses
            let pool_query =
                "SELECT token_0_address, token_1_address FROM pools WHERE pool_address = $1";
            let pool_row = client
                .query_one(pool_query, &[&pool_address])
                .await
                .context("Failed to fetch pool tokens for swap")?;
            let token0_address: String = pool_row.get("token_0_address");
            let token1_address: String = pool_row.get("token_1_address");

            // Determine token flow based on amount signs
            let (token_in, token_out, amount_in, amount_out) = if amount0.is_negative() {
                (
                    token1_address,
                    token0_address,
                    amount1.to_string(),
                    (-amount0).to_string(),
                )
            } else {
                (
                    token0_address,
                    token1_address,
                    amount0.to_string(),
                    (-amount1).to_string(),
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

            // Calculate USD values for amounts
            // For EVM chains, we currently only have ETH/XXX ppols - determine which token is native
            let wrapped_native_address = get_wrapped_native_token_address(chain_id);
            let zero_address = "0x0000000000000000000000000000000000000000";

            let (native_token, _other_token, native_amount, other_amount) =
                if token_in.to_lowercase() == wrapped_native_address.to_lowercase()
                    || token_in.to_lowercase() == zero_address
                {
                    (
                        token_in.clone(),
                        token_out.clone(),
                        amount_in.clone(),
                        amount_out.clone(),
                    )
                } else if token_out.to_lowercase() == wrapped_native_address.to_lowercase()
                    || token_out.to_lowercase() == zero_address
                {
                    (
                        token_out.clone(),
                        token_in.clone(),
                        amount_out.clone(),
                        amount_in.clone(),
                    )
                } else {
                    // If neither token is the native/wrapped native token, we can't calculate USD values accurately
                    (
                        "0".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                        "0".to_string(),
                    )
                };

            let (amount_in_usd, amount_out_usd) = if native_token != "0" {
                let decimals = get_native_token_cmc_id(chain_id).1;
                // Get ETH price in USD
                let eth_price_usd = get_usd_value_of_native(
                    &chain_id,
                    &(10_u128.pow(decimals)),
                    None,
                    None,
                    None,
                    None,
                )
                .await;

                let eth_price_f64 = eth_price_usd.parse::<f64>().unwrap_or(0.0);

                if eth_price_f64 > 0.0 && price.is_some() {
                    let swap_price = price.unwrap();

                    // Calculate USD values using the extracted variables
                    let native_amount_eth =
                        native_amount.parse::<f64>().unwrap_or(0.0) / 10_f64.powf(decimals as f64);
                    let other_amount_tokens = other_amount.parse::<f64>().unwrap_or(0.0);

                    let native_usd_val = native_amount_eth * eth_price_f64;
                    let other_usd_val = if swap_price != 0.0 {
                        other_amount_tokens / swap_price * eth_price_f64
                    } else {
                        0.0
                    };

                    // Map back to amount_in_usd and amount_out_usd based on which is which
                    if token_in.to_lowercase() == native_token.to_lowercase() {
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

            // TODO: Implement proper vault detection logic by checking if the transaction
            // originates from a known vault contract address
            let is_vault_initiated = false;

            let query = r#"
                INSERT INTO ammswap
                    (pool_address, token_in, token_out, amount_in, amount_out, amount_in_usd,
                     amount_out_usd, initiator_user_address, price, transaction_hash, block_number,
                     timestamp, chain_id, is_vault_initiated, sqrt_price, liquidity, tick)
                VALUES
                    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17) ON CONFLICT (transaction_hash) DO NOTHING
            "#;

            let rows = client
                .execute(
                    query,
                    &[
                        &pool_address,
                        &token_in,
                        &token_out,
                        &Decimal::from_str(&amount_in).unwrap(),
                        &Decimal::from_str(&amount_out).unwrap(),
                        &Decimal::from_str(&amount_in_usd).unwrap(),
                        &Decimal::from_str(&amount_out_usd).unwrap(),
                        &initiator_user_address,
                        &Decimal::from_f64(price.unwrap_or(0.0)),
                        &transaction_hash,
                        &block_number,
                        &timestamp,
                        &chain_id,
                        &is_vault_initiated,
                        &sqrt_price,
                        &liquidity_i64,
                        &tick_i32,
                    ],
                )
                .await
                .context("Failed to insert swap data")?;
            info!(target: log_target, "UniswapV3PoolLib::Swap inserted: {:?} rows", rows);
        }
        Some(&UniswapV3PoolLib::Mint::SIGNATURE_HASH) => {
            let UniswapV3PoolLib::Mint {
                sender,
                owner,
                tickLower,
                tickUpper,
                amount,
                amount0,
                amount1,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "Mint";
            info!(target: log_target, "UniswapV3PoolLib::Mint by {sender} for owner {owner}");

            let pool_address = log.address().to_string();
            let user_address = owner.to_string();
            let transaction_hash = log.transaction_hash.unwrap().to_string();
            let block_number = log.block_number.unwrap() as i64;
            let timestamp = match log.block_timestamp {
                Some(ts) => unix_to_system_time(ts),
                None => {
                    error!(target: log_target, "Block timestamp is missing for log: {:?}", log);
                    SystemTime::now() // Fallback to current time if timestamp is missing
                }
            };

            let amount_token0 = amount0.to::<i64>();
            let amount_token1 = amount1.to::<i64>();
            let liquidity_amount = amount as i64;
            let position_id = format!("{}:{}:{}", owner, tickLower, tickUpper);

            insert_liquidity_event(
                &client,
                pool_address,
                user_address,
                true,  // is_add = true for mint
                false, // is_manager = false for regular mint
                Some(position_id),
                amount_token0,
                amount_token1,
                liquidity_amount,
                None, // fee_amount_0
                None, // fee_amount_1
                transaction_hash,
                block_number,
                timestamp,
                chain_id,
                Some(false), // is_vault = false for regular mint
                log_target,
            )
            .await?;
        }
        Some(&UniswapV3PoolLib::MintByProjectManager::SIGNATURE_HASH) => {
            let UniswapV3PoolLib::MintByProjectManager {
                sender,
                owner,
                tickLower,
                tickUpper,
                amount,
                amount0,
                amount1,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "MintByProjectManager";
            info!(target: log_target, "UniswapV3PoolLib::MintByProjectManager by {sender} for owner {owner}");

            let pool_address = log.address().to_string();
            let user_address = owner.to_string();
            let transaction_hash = log.transaction_hash.unwrap().to_string();
            let block_number = log.block_number.unwrap() as i64;
            let timestamp = match log.block_timestamp {
                Some(ts) => unix_to_system_time(ts),
                None => {
                    error!(target: log_target, "Block timestamp is missing for log: {:?}", log);
                    SystemTime::now() // Fallback to current time if timestamp is missing
                }
            };;

            let amount_token0 = amount0.to::<i64>();
            let amount_token1 = amount1.to::<i64>();
            let liquidity_amount = amount as i64;

            let position_id = format!("{}:{}:{}", owner, tickLower, tickUpper);

            insert_liquidity_event(
                &client,
                pool_address,
                user_address,
                true, // is_add = true for mint
                true, // is_manager = true for project manager mint
                Some(position_id),
                amount_token0,
                amount_token1,
                liquidity_amount,
                None, // fee_amount_0
                None, // fee_amount_1
                transaction_hash,
                block_number,
                timestamp,
                chain_id,
                Some(false), // is_vault = false
                log_target,
            )
            .await?;
        }
        Some(&UniswapV3PoolLib::Burn::SIGNATURE_HASH) => {
            let UniswapV3PoolLib::Burn {
                owner,
                tickLower,
                tickUpper,
                amount,
                amount0,
                amount1,
            } = log.log_decode().unwrap().inner.data;

            let log_target = "Burn";
            info!(target: log_target, "UniswapV3PoolLib::Burn by owner {owner}");

            let pool_address = log.address().to_string();
            let user_address = owner.to_string();
            let transaction_hash = log.transaction_hash.unwrap().to_string();
            let block_number = log.block_number.unwrap() as i64;
            let timestamp = match log.block_timestamp {
                Some(ts) => unix_to_system_time(ts),
                None => {
                    error!(target: log_target, "Block timestamp is missing for log: {:?}", log);
                    SystemTime::now() // Fallback to current time if timestamp is missing
                }
            };

            let amount_token0 = amount0.to::<i64>();
            let amount_token1 = amount1.to::<i64>();
            let liquidity_amount = amount as i64;

            let position_id = format!("{}:{}:{}", owner, tickLower, tickUpper);

            let is_manager = false; // TODO: implement manager detection logic

            insert_liquidity_event(
                &client,
                pool_address,
                user_address,
                false, // is_add = false for burn
                is_manager,
                Some(position_id),
                amount_token0,
                amount_token1,
                liquidity_amount,
                None, // fee_amount_0
                None, // fee_amount_1
                transaction_hash,
                block_number,
                timestamp,
                chain_id,
                Some(false), // is_vault = false
                log_target,
            )
            .await?;
        }
        _ => {
            info!("\ndidn't match any event, {:?}", log);
        }
    }
    Ok(())
}

async fn insert_liquidity_event(
    client: &Arc<Client>,
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

async fn get_token_data(token_address: Address, chain_provider: RootProvider<PubSubFrontend>) -> (i32, String, String) {
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