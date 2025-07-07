// @generated automatically by Diesel CLI.

pub mod sql_types {
    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "pool_type_enum"))]
    pub struct PoolTypeEnum;

    #[derive(diesel::query_builder::QueryId, Clone, diesel::sql_types::SqlType)]
    #[diesel(postgres_type(name = "token_launch_type"))]
    pub struct TokenLaunchType;
}

diesel::table! {
    acknowledgement (id) {
        id -> Int8,
        intent_id -> Int8,
        #[max_length = 44]
        sender_address -> Varchar,
        result -> Bool,
        error_message -> Nullable<Text>,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
        order_id -> Int8,
        metadata -> Nullable<Text>,
    }
}

diesel::table! {
    ammswap (id) {
        id -> Int8,
        pool_address -> Text,
        token_in -> Text,
        token_out -> Text,
        amount_in -> Numeric,
        amount_out -> Numeric,
        chain_id -> Int8,
        amount_in_usd -> Numeric,
        amount_out_usd -> Numeric,
        initiator_user_address -> Text,
        price -> Numeric,
        timestamp -> Timestamp,
        transaction_hash -> Text,
        block_number -> Int8,
        is_vault_initiated -> Bool,
        sqrt_price -> Text,
        liquidity -> Int8,
        tick -> Int4,
    }
}

diesel::table! {
    chain_metadata (chain_id) {
        chain_id -> Text,
        network_name -> Text,
        latest_block -> Int8,
    }
}

diesel::table! {
    deposit_received (id) {
        id -> Int8,
        user_address -> Text,
        token_address -> Text,
        chain_id -> Text,
        amount -> Text,
        timestamp -> Timestamp,
        source_transaction_hash -> Nullable<Text>,
        message_id -> Nullable<Text>,
        status -> Nullable<Int4>,
    }
}

diesel::table! {
    hook_executor_orders (id) {
        id -> Int8,
        protocol_id -> Int4,
        #[max_length = 66]
        order_hash -> Varchar,
        order_id -> Int8,
        #[max_length = 66]
        recipient -> Varchar,
        #[max_length = 66]
        token -> Varchar,
        amount -> Numeric,
        timeout_timestamp -> Int8,
        reason -> Nullable<Text>,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
        status -> Int4,
        destination_chain_id -> Int8,
        additional_data -> Nullable<Text>,
    }
}

diesel::table! {
    intent (id) {
        id -> Int8,
        intent_id -> Int8,
        #[max_length = 66]
        owner_address -> Varchar,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
        feeamount -> Nullable<Text>,
    }
}

diesel::table! {
    intent_fees (id) {
        id -> Int8,
        intent_id -> Int8,
        fees -> Jsonb,
    }
}

diesel::table! {
    intent_state (id) {
        id -> Int8,
        intent_id -> Int8,
        version -> Int4,
        #[max_length = 88]
        transaction_hash -> Varchar,
        stage -> Text,
        timestamp -> Timestamp,
        gas_fees -> Nullable<Int8>,
        gas_token -> Nullable<Text>,
        order_id -> Nullable<Int8>,
        chain_id -> Int8,
        #[max_length = 66]
        initiator_address -> Varchar,
        transaction_cost -> Nullable<Text>,
        transaction_cost_usd -> Nullable<Text>,
    }
}

diesel::table! {
    liquidity (id) {
        id -> Int8,
        pool_address -> Text,
        user_address -> Text,
        is_add -> Bool,
        position_id -> Text,
        token_0_amount -> Numeric,
        token_1_amount -> Numeric,
        chain_id -> Int8,
        timestamp -> Timestamp,
        transaction_hash -> Text,
        is_manager -> Bool,
        liquidity -> Int8,
        is_vault -> Bool,
        token_id -> Nullable<Text>,
    }
}

diesel::table! {
    message_dispatched_from_vault (id) {
        id -> Int8,
        intent_id -> Int8,
        #[max_length = 66]
        sender_address -> Varchar,
        destination_domain_id -> Int4,
        provider -> Int4,
        message -> Text,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
        order_id -> Int8,
    }
}

diesel::table! {
    ohlc_price_tables (id) {
        id -> Int8,
        token_address -> Text,
        chain_id -> Int8,
        interval -> Text,
        open_price -> Numeric,
        high_price -> Numeric,
        low_price -> Numeric,
        close_price -> Numeric,
        volume_token -> Numeric,
        volume_usd -> Nullable<Numeric>,
        timestamp_bucket -> Timestamp,
        pool_address -> Text,
    }
}

diesel::table! {
    order_created (id) {
        id -> Int8,
        intent_id -> Int8,
        #[max_length = 66]
        creator_address -> Varchar,
        #[max_length = 66]
        token_in -> Varchar,
        #[max_length = 66]
        token_out -> Varchar,
        amount_in -> Text,
        amount_out -> Text,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
        order_id -> Int8,
        source_chain_id -> Text,
        destination_chain_id -> Text,
        multi_leg -> Bool,
        order_payload -> Text,
        solution_type -> Nullable<Int4>,
        receiver_type -> Int4,
        receiver_address -> Nullable<Text>,
        amount_in_usd -> Nullable<Text>,
        amount_out_usd -> Nullable<Text>,
    }
}

diesel::table! {
    use diesel::sql_types::*;
    use super::sql_types::PoolTypeEnum;
    use super::sql_types::TokenLaunchType;

    pools (id) {
        id -> Int8,
        pool_address -> Text,
        chain_id -> Int8,
        token_0_address -> Text,
        token_1_address -> Text,
        fee -> Numeric,
        tick_spacing -> Int8,
        pool_type -> PoolTypeEnum,
        project_manager -> Text,
        block_number -> Int8,
        created_at -> Timestamp,
        metadata -> Nullable<Jsonb>,
        etp_start_time -> Timestamp,
        etp_end_time -> Timestamp,
        launch_type -> TokenLaunchType,
        initial_sqrt_price -> Text,
        initial_tick -> Int4,
        token_supply -> Text,
        launchpad_token -> Text,
        liquidity_lock_end_timestamp -> Nullable<Timestamp>,
    }
}

diesel::table! {
    received_message_on_vault (id) {
        id -> Int8,
        intent_id -> Int8,
        origin_domain_id -> Int4,
        #[max_length = 44]
        sender_address -> Varchar,
        message -> Text,
        provider -> Int4,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
        chain_id -> Int8,
        order_id -> Int8,
        #[max_length = 255]
        dln_order_id -> Nullable<Varchar>,
        timeout_unix_timestamp_in_sec -> Nullable<Int8>,
    }
}

diesel::table! {
    sanction_address_list (id) {
        id -> Int4,
        address -> Text,
    }
}

diesel::table! {
    solution (id) {
        id -> Int8,
        intent_id -> Int8,
        #[max_length = 44]
        solver_address -> Varchar,
        #[max_length = 88]
        transaction_hash -> Varchar,
        block_number -> Int8,
        timestamp -> Timestamp,
    }
}

diesel::table! {
    token_chains (id) {
        id -> Int4,
        token_id -> Nullable<Text>,
        address -> Nullable<Text>,
        decimals -> Nullable<Int4>,
        network -> Nullable<Text>,
        address_bytes32 -> Nullable<Text>,
    }
}

diesel::table! {
    tokens (id) {
        id -> Text,
        ticker -> Nullable<Text>,
        full_name -> Nullable<Text>,
        is_stable -> Nullable<Bool>,
        is_tradable -> Nullable<Bool>,
        price_usd -> Nullable<Numeric>,
        description -> Nullable<Text>,
        launch_date -> Nullable<Timestamp>,
        website -> Nullable<Text>,
        cmc_id -> Nullable<Text>,
    }
}

diesel::allow_tables_to_appear_in_same_query!(
    acknowledgement,
    ammswap,
    chain_metadata,
    deposit_received,
    hook_executor_orders,
    intent,
    intent_fees,
    intent_state,
    liquidity,
    message_dispatched_from_vault,
    ohlc_price_tables,
    order_created,
    pools,
    received_message_on_vault,
    sanction_address_list,
    solution,
    token_chains,
    tokens,
);
