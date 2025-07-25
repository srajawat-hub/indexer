mod unlock_batch;

pub use unlock_batch::*;
mod unlock;
pub use unlock::*;

mod cancel;
pub use cancel::*;

pub const WSOL_MINT: solana_sdk::pubkey::Pubkey =
    solana_sdk::pubkey!("So11111111111111111111111111111111111111112");

#[derive(Debug, Clone, Copy)]
#[repr(u8)]
pub enum SwiftVaaAction {
    None,
    Fulfill,
    Unlock,
    Cancel,
    UnlockBatch,
}

#[derive(Debug)]
pub enum SwiftError {
    InvalidUnlockVAA,
    InvalidUnlockBatchVAA,
    InvalidCancelVAA,
}

type Result<T, E = SwiftError> = std::result::Result<T, E>;
