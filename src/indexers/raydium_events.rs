use anchor_attribute_event::event;
use anchor_lang::zero_copy;
use anchor_lang::{account, Discriminator};
use anchor_lang::{AnchorDeserialize, AnchorSerialize};
use borsh_0_10 as borsh;
use solana_sdk::pubkey::Pubkey;

pub const REWARD_NUM: usize = 3;
pub const AMM_CONFIG_SEED: &str = "amm_config";

#[event]
#[derive(Clone, Debug)]
pub struct IncreaseLiquidityEvent {
    /// The ID of the token for which liquidity was increased
    pub position_nft_mint: Pubkey,

    /// The amount by which liquidity for the NFT position was increased
    pub liquidity: u128,

    /// The amount of token_0 that was paid for the increase in liquidity
    pub amount_0: u64,

    /// The amount of token_1 that was paid for the increase in liquidity
    pub amount_1: u64,

    /// The token transfer fee for amount_0
    pub amount_0_transfer_fee: u64,

    /// The token transfer fee for amount_1
    pub amount_1_transfer_fee: u64,
}

#[event]
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct DecreaseLiquidityEvent {
    /// The ID of the token for which liquidity was decreased
    pub position_nft_mint: Pubkey,
    /// The amount by which liquidity for the position was decreased
    pub liquidity: u128,
    /// The amount of token_0 that was paid for the decrease in liquidity
    pub decrease_amount_0: u64,
    /// The amount of token_1 that was paid for the decrease in liquidity
    pub decrease_amount_1: u64,
    // The amount of token_0 fee
    pub fee_amount_0: u64,
    /// The amount of token_1 fee
    pub fee_amount_1: u64,
    /// The amount of rewards
    pub reward_amounts: [u64; REWARD_NUM],
    /// The amount of token_0 transfer fee
    pub transfer_fee_0: u64,
    /// The amount of token_1 transfer fee
    pub transfer_fee_1: u64,
}

#[event]
#[derive(Clone, Debug)]
pub struct CollectPersonalFeeEvent {
    /// The ID of the token for which underlying tokens were collected
    pub position_nft_mint: Pubkey,

    /// The token account that received the collected token_0 tokens
    pub recipient_token_account_0: Pubkey,

    /// The token account that received the collected token_1 tokens
    pub recipient_token_account_1: Pubkey,

    /// The amount of token_0 owed to the position that was collected
    pub amount_0: u64,

    /// The amount of token_1 owed to the position that was collected
    pub amount_1: u64,
}

#[event]
#[derive(Clone, Debug)]
pub struct CollectProtocolFeeEvent {
    /// The pool whose protocol fee is collected
    pub pool_state: Pubkey,

    /// The address that receives the collected token_0 protocol fees
    pub recipient_token_account_0: Pubkey,

    /// The address that receives the collected token_1 protocol fees
    pub recipient_token_account_1: Pubkey,

    /// The amount of token_0 protocol fees that is withdrawn
    pub amount_0: u64,

    /// The amount of token_0 protocol fees that is withdrawn
    pub amount_1: u64,
}

#[event]
#[derive(Clone, Debug)]
pub struct PoolCreatedEvent {
    /// The first token of the pool by address sort order
    pub token_mint_0: Pubkey,

    /// The second token of the pool by address sort order
    pub token_mint_1: Pubkey,

    /// The minimum number of ticks between initialized ticks
    pub tick_spacing: u16,

    /// The address of the created pool
    pub pool_state: Pubkey,

    /// The initial sqrt price of the pool, as a Q64.64
    pub sqrt_price_x64: u128,

    /// The initial tick of the pool, i.e. log base 1.0001 of the starting price of the pool
    pub tick: i32,

    /// Vault of token_0
    pub token_vault_0: Pubkey,

    /// Vault of token_1
    pub token_vault_1: Pubkey,

    /// Fee tier index
    pub fee_tier_index: u8,
}

#[derive(Clone, Debug)]
pub struct PoolCreatedEventWithState {
    pub inner: PoolCreatedEvent,
    pub pool_state: PoolState,
    pub amm_config: AmmConfig,
}

#[event]
#[derive(Clone, Debug)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct SwapEvent {
    /// The pool for which token_0 and token_1 were swapped
    pub pool_state: Pubkey,

    /// The address that initiated the swap call, and that received the callback
    pub sender: Pubkey,

    /// The payer token account in zero for one swaps, or the recipient token account
    /// in one for zero swaps
    pub token_account_0: Pubkey,

    /// The payer token account in one for zero swaps, or the recipient token account
    /// in zero for one swaps
    pub token_account_1: Pubkey,

    /// The real delta amount of the token_0 of the pool or user
    pub amount_0: u64,

    /// The transfer fee charged by the withheld_amount of the token_0
    pub transfer_fee_0: u64,

    /// The real delta of the token_1 of the pool or user
    pub amount_1: u64,

    /// The transfer fee charged by the withheld_amount of the token_1
    pub transfer_fee_1: u64,

    /// if true, amount_0 is negtive and amount_1 is positive
    pub zero_for_one: bool,

    /// The sqrt(price) of the pool after the swap, as a Q64.64
    pub sqrt_price_x64: u128,

    /// The liquidity of the pool after the swap
    pub liquidity: u128,

    /// The log base 1.0001 of price of the pool after the swap
    pub tick: i32,

    /// Swap was initiate by the Vault program
    pub via_vault: bool,
}

#[zero_copy(unsafe)]
#[repr(C, packed)]
#[derive(Default, Debug, PartialEq, Eq)]
pub struct RewardInfo {
    /// Reward state
    pub reward_state: u8,
    /// Reward open time
    pub open_time: u64,
    /// Reward end time
    pub end_time: u64,
    /// Reward last update time
    pub last_update_time: u64,
    /// Q64.64 number indicates how many tokens per second are earned per unit of liquidity.
    pub emissions_per_second_x64: u128,
    /// The total amount of reward emissioned
    pub reward_total_emissioned: u64,
    /// The total amount of claimed reward
    pub reward_claimed: u64,
    /// Reward token mint.
    pub token_mint: Pubkey,
    /// Reward vault token account.
    pub token_vault: Pubkey,
    /// The owner that has permission to set reward param
    pub authority: Pubkey,
    /// Q64.64 number that tracks the total tokens earned per unit of liquidity since the reward
    /// emissions were turned on.
    pub reward_growth_global_x64: u128,
}

#[account(zero_copy(unsafe))]
#[repr(C, packed)]
#[derive(Default, Debug)]
pub struct PoolState {
    /// Bump to identify PDA
    pub bump: [u8; 1],
    // Which config the pool belongs
    pub amm_config: Pubkey,
    // Pool creator
    pub owner: Pubkey,

    /// Token pair of the pool, where token_mint_0 address < token_mint_1 address
    pub token_mint_0: Pubkey,
    pub token_mint_1: Pubkey,

    /// Token pair vault
    pub token_vault_0: Pubkey,
    pub token_vault_1: Pubkey,

    /// observation account key
    pub observation_key: Pubkey,

    /// mint0 and mint1 decimals
    pub mint_decimals_0: u8,
    pub mint_decimals_1: u8,

    /// The minimum number of ticks between initialized ticks
    pub tick_spacing: u16,
    /// The currently in range liquidity available to the pool.
    pub liquidity: u128,
    /// The current price of the pool as a sqrt(token_1/token_0) Q64.64 value
    pub sqrt_price_x64: u128,
    /// The current tick of the pool, i.e. according to the last tick transition that was run.
    pub tick_current: i32,

    pub padding3: u16,
    pub padding4: u16,

    /// The fee growth as a Q64.64 number, i.e. fees of token_0 and token_1 collected per
    /// unit of liquidity for the entire life of the pool.
    pub fee_growth_global_0_x64: u128,
    pub fee_growth_global_1_x64: u128,

    /// The amounts of token_0 and token_1 that are owed to the protocol.
    pub protocol_fees_token_0: u64,
    pub protocol_fees_token_1: u64,

    /// The amounts in and out of swap token_0 and token_1
    pub swap_in_amount_token_0: u128,
    pub swap_out_amount_token_1: u128,
    pub swap_in_amount_token_1: u128,
    pub swap_out_amount_token_0: u128,

    /// Bitwise representation of the state of the pool
    /// bit0, 1: disable open position and increase liquidity, 0: normal
    /// bit1, 1: disable decrease liquidity, 0: normal
    /// bit2, 1: disable collect fee, 0: normal
    /// bit3, 1: disable collect reward, 0: normal
    /// bit4, 1: disable swap, 0: normal
    pub status: u8,
    /// Leave blank for future use
    pub padding: [u8; 7],

    pub reward_infos: [RewardInfo; REWARD_NUM],

    /// Packed initialized tick array state
    pub tick_array_bitmap: [u64; 16],

    /// except protocol_fee and fund_fee
    pub total_fees_token_0: u64,
    /// except protocol_fee and fund_fee
    pub total_fees_claimed_token_0: u64,
    pub total_fees_token_1: u64,
    pub total_fees_claimed_token_1: u64,

    pub fund_fees_token_0: u64,
    pub fund_fees_token_1: u64,

    // The timestamp allowed for swap in the pool.
    // Note: The open_time is disabled for now.
    pub open_time: u64,
    // account recent update epoch
    pub recent_epoch: u64,

    pub exclusive_trading_period_start_time: u64,
    pub exclusive_trading_period_end_time: u64,

    // The NFT that the project uses to provide liquidity to the pool
    // If value is 0, it means the liquidity is not provided by the project yet.
    pub project_liquidity_provider_nft_mint: Pubkey,

    // The account that the project uses to provide liquidity to the pool
    pub project_liquidity_provider: Pubkey,

    // whether the reserve token mint is token_mint_0 or token_mint_1
    pub reserve_token_mint_index: u8,

    // the index of the fee which the current pool is using
    pub fee_tier_index: u8,

    // decides how much amount of liquidity needs to be locked by the project
    pub launch_type: u8,

    // Unused bytes for future upgrades.
    pub padding1: [u64; 18],
    pub padding2: [u64; 32],
}

impl PoolState {
    pub fn get_reserve_token_mint<'a>(
        &self,
        token_mint_0: &'a Pubkey,
        token_mint_1: &'a Pubkey,
    ) -> &'a Pubkey {
        if self.reserve_token_mint_index == 0 {
            &token_mint_0
        } else {
            &token_mint_1
        }
    }
}

#[derive(AnchorSerialize, AnchorDeserialize, Debug, Clone, Default, Copy)]
pub struct FeedIdToPubkey {
    pub feed_id: [u8; 32],
    pub pubkey: Pubkey,
}

#[event]
#[derive(Debug, Clone)]
pub struct ConfigChangeEvent {
    pub index: u16,
    pub owner: Pubkey,
    pub tick_spacing: u16,
    pub fund_owner: Pubkey,
    pub pool_creator_authority: Pubkey,
    pub vault_address: Pubkey,
    pub feed_id_to_pubkey: Vec<FeedIdToPubkey>,
    pub fee_tiers: Vec<FeeTier>,
    pub project_liquidity_requirements: [ProjectLiquidityRequirements; 2],
}

/// Holds the current owner of the factory
#[account]
#[derive(Default, Debug)]
pub struct AmmConfig {
    /// Bump to identify PDA
    pub bump: u8,
    pub index: u16,
    /// Address of the protocol owner
    pub owner: Pubkey,
    /// The tick spacing
    pub tick_spacing: u16,
    // padding space for upgrade
    pub padding_u32: u32,
    pub fund_owner: Pubkey,
    pub pool_creator_authority: Pubkey,
    // The address of the vault that is allowed to trade during the exclusive trading period
    pub vault_address: Pubkey,
    pub feed_id_to_pubkey: [FeedIdToPubkey; 10],

    /// Project liquidity requirements for each type
    pub project_liquidity_requirements: [ProjectLiquidityRequirements; 2],

    // Different fees for different type of pools
    pub fee_tiers: [FeeTier; 5],

    pub padding: [u64; 2],
}

#[derive(AnchorSerialize, AnchorDeserialize, Debug, Clone, Default, Copy)]
pub struct ProjectLiquidityRequirements {
    /// Amount of liquidity that the project needs to deposit when pool is created
    pub minimum_project_liquidity_to_deposit: u64,
    /// Amount of liquidity that must be locked by the project and cannot be removed until the period is over
    pub minimum_locked_liquidity_by_project: u64,
    /// The period of time that the liquidity is locked after the exclusive trading period is over
    pub lock_liquidity_period: u64,
}

#[derive(AnchorSerialize, AnchorDeserialize, Debug, Clone, Default, Copy)]
pub struct FeeTier {
    pub protocol_fee_rate: u32,
    pub trade_fee_rate: u32,
    pub fund_fee_rate: u32,

    pub etp_protocol_fee_rate: u32,
    pub etp_trade_fee_rate: u32,
    pub etp_fund_fee_rate: u32,
}

impl AmmConfig {
    pub fn get_reserve_token_mint<'a>(
        &self,
        token_mint_0: &'a Pubkey,
        token_mint_1: &'a Pubkey,
    ) -> anyhow::Result<&'a Pubkey> {
        let reserve_token_mint_index =
            self.feed_id_to_pubkey.iter().find_map(|feed_id_to_pubkey| {
                if feed_id_to_pubkey.pubkey == Pubkey::default() {
                    return None;
                }
                if token_mint_0 == &feed_id_to_pubkey.pubkey {
                    Some(token_mint_0)
                } else if token_mint_1 == &feed_id_to_pubkey.pubkey {
                    Some(token_mint_1)
                } else {
                    None
                }
            });

        reserve_token_mint_index.ok_or_else(|| anyhow::anyhow!("reserve token not found"))
    }
}

/// Emitted when create a new position
#[event]
#[derive(Debug, Clone)]
pub struct CreatePersonalPositionEvent {
    /// The pool for which liquidity was added
    pub pool_state: Pubkey,

    /// The address that create the position
    pub minter: Pubkey,

    /// The owner of the position and recipient of any minted liquidity
    pub nft_owner: Pubkey,

    /// The mint token account of the position
    pub nft_mint: Pubkey,

    /// The account holding the position NFT
    pub nft_account: Pubkey,

    /// The lower tick of the position
    pub tick_lower_index: i32,

    /// The upper tick of the position
    pub tick_upper_index: i32,

    /// The amount of liquidity minted to the position range
    pub liquidity: u128,

    /// The amount of token_0 was deposit for the liquidity
    pub deposit_amount_0: u64,

    /// The amount of token_1 was deposit for the liquidity
    pub deposit_amount_1: u64,

    /// The token transfer fee for deposit_amount_0
    pub deposit_amount_0_transfer_fee: u64,

    /// The token transfer fee for deposit_amount_1
    pub deposit_amount_1_transfer_fee: u64,

    /// Liquidity deposited by the project manager
    pub is_deposited_by_project: bool,

    /// Liquidity deposited by the vault program
    pub is_deposited_by_vault: bool,
}

#[derive(PartialEq, Eq, AnchorSerialize, AnchorDeserialize)]
#[repr(u8)]
pub enum LaunchType {
    Fair,
    Curated,
}

impl From<LaunchType> for u8 {
    fn from(value: LaunchType) -> Self {
        match value {
            LaunchType::Fair => 0,
            LaunchType::Curated => 1,
        }
    }
}

impl From<u8> for LaunchType {
    fn from(value: u8) -> Self {
        match value {
            0 => LaunchType::Fair,
            1 => LaunchType::Curated,
            _ => panic!("Invalid launch type"),
        }
    }
}

impl ToString for LaunchType {
    fn to_string(&self) -> String {
        match self {
            LaunchType::Fair => "FairLaunch".to_string(),
            LaunchType::Curated => "CuratedLaunch".to_string(),
        }
    }
}

/// Emitted pool liquidity change when increase and decrease liquidity
#[event]
#[derive(Debug, Clone)]
pub struct LiquidityChangeEvent {
    /// The pool for swap
    pub pool_state: Pubkey,

    /// The tick of the pool
    pub tick: i32,

    /// The tick lower of position
    pub tick_lower: i32,

    /// The tick lower of position
    pub tick_upper: i32,

    /// The liquidity of the pool before liquidity change
    pub liquidity_before: u128,

    /// The liquidity of the pool after liquidity change
    pub liquidity_after: u128,
}
