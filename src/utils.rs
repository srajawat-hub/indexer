use std::time::SystemTime;

use chrono::DateTime;

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
        _ => "0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2".to_string(), // Default to Ethereum WETH
    }
}

pub fn get_native_token_cmc_id(chain_id: i64) -> (String, u32) {
    match chain_id {
        137 => (String::from("28321"), 18),      // pol
        1399811149 => (String::from("5426"), 9), // sol
        18082 => (String::from("3408"), 18),     // usdc
        _ => (String::from("1027"), 18),         // ETH
    }
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
