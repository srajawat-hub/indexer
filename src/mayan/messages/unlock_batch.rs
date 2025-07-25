use super::super::wormhole::SOLANA_CHAIN;
use super::Result;
use super::{SwiftError, SwiftVaaAction, WSOL_MINT};
use solana_sdk::msg;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct BatchUnlockMessage<'a>(&'a [u8]);

impl<'a> AsRef<[u8]> for BatchUnlockMessage<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

const BATCH_UNLOCK_HEADER_LENGTH: u16 = 3;
const BATCH_UNLOCK_ITEM_LENGTH: u16 = 98;

fn get_slice_index(item_index: u16, offset: u16, size: u16) -> (usize, usize) {
    let start = BATCH_UNLOCK_HEADER_LENGTH as usize
        + item_index as usize * BATCH_UNLOCK_ITEM_LENGTH as usize
        + offset as usize;
    let end = start + size as usize;
    (start, end)
}

impl<'a> BatchUnlockMessage<'a> {
    pub fn items_count(&self) -> u16 {
        u16::from_be_bytes(self.0[1..3].try_into().unwrap())
    }

    pub fn hash(&self, item_index: u16) -> [u8; 32] {
        let (start, end) = get_slice_index(item_index, 0, 32);
        self.0[start..end].try_into().unwrap()
    }

    pub fn chain_source(&self, item_index: u16) -> u16 {
        let (start, end) = get_slice_index(item_index, 32, 2);
        u16::from_be_bytes(self.0[start..end].try_into().unwrap())
    }

    pub fn token_in(&'a self, item_index: u16) -> [u8; 32] {
        let (start, end) = get_slice_index(item_index, 34, 32);
        self.0[start..end].try_into().unwrap()
    }

    pub fn addr_unlocker(&'a self, item_index: u16) -> [u8; 32] {
        let (start, end) = get_slice_index(item_index, 66, 32);
        self.0[start..end].try_into().unwrap()
    }

    pub fn parse(payload: &'a [u8], item_index: u16) -> Result<Self> {
        if payload.len() < 101 {
            msg!("payload length is too short");
            return Err(SwiftError::InvalidUnlockBatchVAA);
        }

        if payload[0] != SwiftVaaAction::UnlockBatch as u8 {
            msg!("payload action is not UnlockBatch");
            return Err(SwiftError::InvalidUnlockBatchVAA);
        }

        let unlock_message = Self(payload);
        let items_count = unlock_message.items_count();

        if payload.len() != 3 + items_count as usize * 98 {
            msg!("payload length is too short for items count");
            return Err(SwiftError::InvalidUnlockBatchVAA);
        }

        if item_index >= items_count {
            msg!("item index is out of bounds");
            return Err(SwiftError::InvalidUnlockBatchVAA);
        }

        if unlock_message.chain_source(item_index) != SOLANA_CHAIN {
            msg!("item chain source is not Solana");
            return Err(SwiftError::InvalidUnlockBatchVAA);
        }

        Ok(unlock_message)
    }

    pub fn parse_unchecked(payload: &'a [u8]) -> Self {
        Self(payload)
    }

    pub fn mint_from(&self, item_index: u16) -> [u8; 32] {
        let token_in = self.token_in(item_index);
        if token_in == [0; 32] {
            return WSOL_MINT.to_bytes();
        }
        token_in
    }
}
