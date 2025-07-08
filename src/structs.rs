use std::fmt;
use crate::utils::deserialize_number;
use serde::{Deserialize, Serialize};
use tokio_postgres::types::{IsNull, ToSql, Type};

#[derive(Debug, Deserialize, Serialize)]
pub struct ResultCosts {
    pub destination_cost: AmountTypes,
    pub inclusive_layer_fee: AmountTypes,
    pub provider_fee: ThirdPartyFeeResult,
    pub source_cost: AmountTypes,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AmountTypes {
    pub value: Option<String>,
    pub value_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ThirdPartyFeeResult {
    pub flat_fee: AmountTypes,
    pub provider: Option<String>,
    pub solver_fee: AmountTypes,
    pub variable_fee: AmountTypes,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QuoteApiResponse {
    pub fee_data: ResultCosts,
}

// Add these structs near other struct definitions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderId {
    pub bytesValue: Option<String>,
    pub bytesArrayValue: Option<String>,
    pub stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChainId {
    bytesValue: Option<String>,
    bytesArrayValue: Option<String>,
    #[serde(deserialize_with = "deserialize_number")]
    bigIntegerValue: Option<f64>,
    stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TokenAddress {
    Base64Value: Option<String>,
    bytesArrayValue: Option<String>,
    stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Amount {
    bytesValue: Option<String>,
    bytesArrayValue: Option<String>,
    #[serde(deserialize_with = "deserialize_number")]
    bigIntegerValue: Option<f64>,
    stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TokenMetadata {
    decimals: Option<i32>,
    name: Option<String>,
    symbol: Option<String>,
    logoURI: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OfferWithMetadata {
    chainId: ChainId,
    tokenAddress: TokenAddress,
    amount: Amount,
    finalAmount: Amount,
    metadata: TokenMetadata,
    decimals: Option<i32>,
    name: Option<String>,
    symbol: Option<String>,
    logoURI: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AffiliateFee {
    beneficiarySrc: Option<TokenAddress>,
    amount: Option<Amount>,
}

#[derive(Debug, Serialize, Clone)]
pub struct LiquidityDecodedLogData {
    pub liquidity_user_address: Option<String>,
    pub liquidity_token_id: Option<String>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub orderId: OrderId,
    pub creationTimestamp: i64,
    pub giveOfferWithMetadata: OfferWithMetadata,
    pub takeOfferWithMetadata: OfferWithMetadata,
    pub state: String,
    pub externalCallState: String,
    pub finalPercentFee: Amount,
    pub fixFee: Amount,
    pub affiliateFee: AffiliateFee,
    pub unlockAuthorityDst: Option<TokenAddress>,
    pub createEventTransactionHash: Option<TokenAddress>,
    pub preswapData: Option<serde_json::Value>,
    pub orderMetadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrdersResponse {
    pub orders: Vec<Order>,
}

#[derive(Debug, Deserialize)]
pub struct SwaggerTokenData {
    #[serde(rename = "id")]
    pub token_id: String,
}

#[derive(Debug, Deserialize)]
pub struct TokenUsdPriceData {
    pub data: SwaggerTokenData
}

#[derive(Debug, Deserialize)]
pub struct SwaggerTokenUsdRate {
    pub rate: String
}

#[derive(Debug, Deserialize)]
pub struct SwaggerTokenUsdPriceData {
    pub data: SwaggerTokenUsdRate
}

#[derive(Debug)]
pub enum PoolType {
    EVM,
    SOLANA
}

impl fmt::Display for PoolType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match self {
            PoolType::EVM => "EVM",
            PoolType::SOLANA => "SOLANA",
        })
    }
}

impl ToSql for PoolType {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let s = self.to_string();
        s.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "pool_type_enum"
    }

    tokio_postgres::types::to_sql_checked!();
}

#[derive(Debug)]
pub enum TokenLaunchType {
    FAIR,
    CURATED
}

impl fmt::Display for TokenLaunchType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", match self {
            TokenLaunchType::FAIR => "FAIR",
            TokenLaunchType::CURATED => "CURATED",
        })
    }
}

impl ToSql for TokenLaunchType {
    fn to_sql(
        &self,
        ty: &Type,
        out: &mut tokio_postgres::types::private::BytesMut,
    ) -> Result<IsNull, Box<dyn std::error::Error + Sync + Send>> {
        let s = self.to_string();
        s.to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "token_launch_type"
    }

    tokio_postgres::types::to_sql_checked!();
}