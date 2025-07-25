use borsh::{BorshDeserialize, BorshSerialize};
use solana_sdk::pubkey::Pubkey;

#[derive(Clone, Debug, BorshDeserialize, BorshSerialize, PartialEq)]
#[borsh(crate = "::borsh")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct InitOrderParams {
    pub amount_in_min: u64,
    pub native_input: bool,
    pub fee_submit: u64,
    pub addr_dest: [u8; 32],
    pub chain_dest: u16,
    pub token_out: [u8; 32],
    pub amount_out_min: u64,
    pub gas_drop: u64,
    pub fee_cancel: u64,
    pub fee_refund: u64,
    pub deadline: u64,
    pub addr_ref: [u8; 32],
    pub fee_rate_ref: u8,
    pub fee_rate_mayan: u8,
    pub auction_mode: u8,
    pub key_rnd: [u8; 32],
}
#[derive(Clone, Debug, BorshDeserialize, BorshSerialize, PartialEq)]
#[borsh(crate = "::borsh")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct FulfillInfo {
    pub winner: Pubkey,
    pub amount_promised: u64,
    pub amount_output: u64,
    pub patch_version: u8,
    pub time_fulfill: u64,
    pub addr_unlocker: [u8; 32],
}
#[derive(Clone, Debug, BorshDeserialize, BorshSerialize, PartialEq)]
#[borsh(crate = "::borsh")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SwiftDestSolanaStatus {
    None,
    Created,
    Fulfilled,
    Settled,
    Posted,
    Cancelled,
    Closed,
}
#[derive(Clone, Debug, BorshDeserialize, BorshSerialize, PartialEq)]
#[borsh(crate = "::borsh")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SwiftSourceSolanaStatus {
    None,
    Locked,
    Unlocked,
    Refunded,
}
#[derive(Clone, Debug, BorshDeserialize, BorshSerialize, PartialEq)]
#[borsh(crate = "::borsh")]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct OrderInfo {
    pub trader: [u8; 32],
    pub chain_source: u16,
    pub token_in: [u8; 32],
    pub addr_dest: [u8; 32],
    pub chain_dest: u16,
    pub token_out: [u8; 32],
    pub amount_out_min: u64,
    pub gas_drop: u64,
    pub fee_cancel: u64,
    pub fee_refund: u64,
    pub deadline: u64,
    pub addr_ref: [u8; 32],
    pub fee_rate_ref: u8,
    pub fee_rate_mayan: u8,
    pub auction_mode: u8,
    pub key_rnd: [u8; 32],
}
