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
