use std::{str::FromStr, time::SystemTime};

use alloy::{primitives::{Address, B256}, providers::{ProviderBuilder, RootProvider}, transports::http::Http};
use chrono::DateTime;
use log::{error, info};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{bs58, program_pack::Pack, pubkey::Pubkey};
use spl_token::state::Mint;

use crate::{constants::SOLANA_CHAIN_ID, solidity_structs::token, structs::{SwaggerTokenUsdPriceData, TokenUsdPriceData}};


// Add this helper function to handle number deserialization
pub fn deserialize_number<'de, D>(deserializer: D) -> Result<Option<f64>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value: serde_json::Value = serde::Deserialize::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(n) => Ok(n.as_f64()),
        serde_json::Value::Null => Ok(None),
        _ => Err(serde::de::Error::custom("expected number or null")),
    }
}

pub(crate) fn unix_to_system_time(timestamp_secs: u64) -> SystemTime {
    let dt_utc =
        DateTime::from_timestamp(timestamp_secs as i64, 0).expect("timestamp out of range");
    dt_utc.into()
}

pub fn chain_id_to_chain_name(chain_id: i64) -> String {
    match chain_id {
        1 => "ethereum".to_string(),
        10 => "optimism".to_string(),
        137 => "polygon".to_string(),
        42161 => "arbitrum".to_string(),
        8453 => "base".to_string(),
        1399811149 => "solana".to_string(),
        _ => format!("Unknown Chain ID: {}", chain_id),
    }
}

pub fn get_wrapped_native_token_address(chain_id: i64) -> String {
    match chain_id {
        1 => "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(), // Ethereum Mainnet - WETH
        137 => "0x0d500B1d8E8eF31E21C99d1Db9A6444d3ADf1270".to_string(), // Polygon Mainnet - WMATIC
        42161 => "0x82aF49447D8a07e3bd95BD0d56f35241523fBab1".to_string(), // Arbitrum One - WETH
        10 => "0x4200000000000000000000000000000000000006".to_string(), // Optimism Mainnet - WETH
        8453 => "0x4200000000000000000000000000000000000006".to_string(), // Base Mainnet - WETH
        18083 => "0x6a79ca29282AC295679a14A8274300e5842e45e5".to_string(), // Inclusive Layer Testnet
        18082 => "0x0000000000000000000000000000000000000000".to_string(), // Inclusive Layer
        999 => "0x5555555555555555555555555555555555555555".to_string(), // Hyperevm - WHYPE
        1399811149 => "So11111111111111111111111111111111111111112".to_string(),
        _ => "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(), // Default to Ethereum WETH
    }
}

pub fn get_rpc_url(chain_id: &str) -> String {
    match chain_id {
        "18083" => String::from("https://rpc.inclusive-layer-test.t.raas.gelato.cloud"),
        "18082" => String::from("https://rpc.inclusive-layer.raas.gelato.cloud"),
        "1" => String::from("https://dawn-radial-flower.quiknode.pro/e74a7637af460c3bb348ebdc7cd59f4374e4ece7"),
        "42161" => String::from("https://smart-young-dust.arbitrum-mainnet.quiknode.pro/81fb7bc3cb557c5f57ce646e35de91ed8dae5557"),
        "10" => String::from("https://blue-neat-mountain.optimism.quiknode.pro/794dff6716a069d1d4d0c6b5bf6d4456f3848618"),
        "8453" => String::from("https://light-quaint-sound.base-mainnet.quiknode.pro/1be86f76089a93cdec8402a55cf83d30d1091cef"),
        "137" => String::from("https://prettiest-few-pine.matic.quiknode.pro/cb499925dd6d5b0649febdd489afce406924d074"),
        SOLANA_CHAIN_ID => String::from("https://mainnet.helius-rpc.com/?api-key=d4d3c545-bd81-405c-9e51-3f600e9c25ad"),
        &_ => String::new()
    }
}

pub fn get_native_token_cmc_id(chain_id: i64) -> (String, u32) {
    match chain_id {
        137 => (String::from("28321"), 18),      // pol
        1399811149 => (String::from("5426"), 9), // sol
        18082 => (String::from("3408"), 18),     // usdc
        999 => (String::from("32196"), 18), // hyperevm whype
        _ => (String::from("1027"), 18),         // ETH
    }
}

pub async fn get_usd_value_of_token(token_address: Option<&str>, chain_id: &str) -> f64 {
    let request_client = reqwest::Client::new();
    let query_token_address = match token_address {
        Some(address) => String::from(address),
        None => get_wrapped_native_token_address(chain_id.parse::<i64>().unwrap()), // giving None for native tokens and fetching for wrapped token
    };
    if query_token_address.len() == 0 {
        return 0.0;
    }
    let caishen_swagger_api_url = std::env::var("CAISHEN_SWAGGER_API_URL").expect("CAISHEN_SWAGGER_API_URL must be set");

    let token_data_url = format!("{}/v1/tokens/token-by-contract?chainId={}&contract={}", caishen_swagger_api_url, chain_id, query_token_address);
    let token_price = match request_client.get(&token_data_url).send().await {
        Ok(data) => {
            let result = match data.json::<TokenUsdPriceData>().await {
                Ok(token_data) => {
                    let token_id = token_data.data.token_id;
                    let price_url = format!("{}/v1/tokens/rates/{}", caishen_swagger_api_url, token_id);
                    let price_response = match request_client.get(&price_url).send().await {
                        Ok(price_data) => {
                            let price_result = match price_data.json::<SwaggerTokenUsdPriceData>().await {
                                Ok(price_data) => price_data.data.rate.parse::<f64>().unwrap_or(0.0),
                                Err(_e) => 0.0,
                            };
                            price_result
                        }
                        Err(_e) => 0.0,
                    };
                    price_response
                },
                Err(_e) => 0.0,
            };
            result
        },
        Err(_e) => 0.0,
    };

    token_price
}

pub async fn get_token_decimals(
    token_address: &str,
    chain_id: &str
) -> f64 {
    let mut decimal_value: f64 = 0.0;
    let rpc_url = get_rpc_url(chain_id);
    if chain_id == SOLANA_CHAIN_ID {
        let solana_client = RpcClient::new(rpc_url);
        info!(target: "get_token_decimals", "Fetching decimals for token address: {}", token_address);
        let formatted_token_address = hex_to_base58(token_address);
        info!(target: "get_token_decimals", "Formatted token address: {}", formatted_token_address);
        let mint_pubkey = formatted_token_address.parse::<Pubkey>().unwrap();
        let account_data = solana_client.get_account_data(&mint_pubkey).await.unwrap();
        let mint = Mint::unpack(&account_data).unwrap();
        let decimals = mint.decimals;
        decimals as f64
    } else {
        let provider = ProviderBuilder::new().on_http(rpc_url.parse().unwrap());
        let formatted_address = bytes32_to_address_str(token_address);
        let token = Address::from_str(&formatted_address).unwrap();
        let erc20 = token::Token::new(token, provider);

        // Call the decimals function
        let decimals = match erc20.decimals().call().await {
            Ok(res) => res._0,
            Err(e) => {
                error!(target: "get_token_decimals", "Error fetching decimals for token {}: {:?}", token_address, e);
                18 // Default to 18 if fetching fails
            }
        };
        decimals as f64
    }
}

pub async fn get_token_data(token_address: Address, chain_provider: RootProvider<Http<reqwest::Client>>) -> (i32, String, String) {
    let log_target = "EVM get_token_data";
    let token_instance = token::Token::new(token_address, chain_provider.clone());
    let decimals = 18 as i32;

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

fn bytes32_to_address_str(input: &str) -> String {
    // Strip optional 0x prefix
    let trimmed = input.trim_start_matches("0x");

    // Check hex length = 64 chars (32 bytes)
    if trimmed.len() != 64 {
        return input.to_string();
    }

    // Try parsing to B256
    if let Ok(b256) = B256::from_str(input) {
        let addr = Address::from_slice(&b256.as_slice()[12..]);
        return format!("{:#x}", addr); // adds 0x prefix
    }

    // If parsing fails
    input.to_string()
}

fn hex_to_base58(hex: &str) -> String {
    let res = if hex.starts_with("0x") {
        let without_prefix = &hex[2..]; // Remove "0x" prefix
        let bytes = hex::decode(without_prefix).expect("Invalid hex string");
        bs58::encode(bytes).into_string()
    } else {
        hex.to_string()
    };
    res
}
 

/// Displays the error if present, waits for few seconds and
/// retries execution.
///
/// The error is usually due to load on rpc which is solved
/// by waiting a few seconds.
#[macro_export]
macro_rules! skip_fail {
    ($res:expr) => {
        match $res {
            Ok(val) => val,
            Err(e) => {
                log::error!("{:?}", e);
                sleep(Duration::from_secs(2));
                continue;
            }
        }
    };
}
