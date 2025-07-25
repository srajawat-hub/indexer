use super::super::wormhole::SOLANA_CHAIN;
use super::Result;
use super::{SwiftError, SwiftVaaAction, WSOL_MINT};
use solana_sdk::msg;

#[derive(Debug, Copy, Clone, PartialEq)]
pub struct CancelMessage<'a>(&'a [u8]);

impl<'a> AsRef<[u8]> for CancelMessage<'a> {
    fn as_ref(&self) -> &[u8] {
        self.0
    }
}

const CANCEL_PAYLOAD_LENGTH: u16 = 147;

impl<'a> CancelMessage<'a> {
    pub fn hash(&self) -> [u8; 32] {
        self.0[1..33].try_into().unwrap()
    }

    pub fn chain_source(&self) -> u16 {
        u16::from_be_bytes(self.0[33..35].try_into().unwrap())
    }

    pub fn token_in(self) -> [u8; 32] {
        self.0[35..67].try_into().unwrap()
    }

    pub fn trader(self) -> [u8; 32] {
        self.0[67..99].try_into().unwrap()
    }

    pub fn relayer_cancel(self) -> [u8; 32] {
        self.0[99..131].try_into().unwrap()
    }

    pub fn fee_cancel(&self) -> u64 {
        u64::from_be_bytes(self.0[131..139].try_into().unwrap())
    }

    pub fn fee_refund(&self) -> u64 {
        u64::from_be_bytes(self.0[139..147].try_into().unwrap())
    }

    pub fn parse(payload: &'a [u8]) -> Result<Self> {
        if payload.len() != CANCEL_PAYLOAD_LENGTH as usize {
            msg!("payload length is wrong");
            return Err(SwiftError::InvalidCancelVAA);
        }

        if payload[0] != SwiftVaaAction::Cancel as u8 {
            msg!("payload action is not Unlock");
            return Err(SwiftError::InvalidCancelVAA);
        }

        let cancel_message = Self(payload);

        if cancel_message.chain_source() != SOLANA_CHAIN {
            msg!("chain source is not solana");
            return Err(SwiftError::InvalidCancelVAA);
        }

        Ok(cancel_message)
    }

    pub fn parse_unchecked(payload: &'a [u8]) -> Self {
        Self(payload)
    }

    pub fn mint_from(&self) -> [u8; 32] {
        let token_in = self.token_in();
        if token_in == [0; 32] {
            return WSOL_MINT.to_bytes();
        }
        token_in
    }
}
