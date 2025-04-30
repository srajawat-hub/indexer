// src/main.rs
mod events;
mod indexers;
pub mod solidity_structs;
mod utils;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use alloy::hex;
use alloy::primitives::{Address, Bytes, U256};
use alloy::sol_types::SolValue;
use chrono::{DateTime, Utc};
use dotenv::dotenv;
use indexers::{BlockchainIndexer, EvmIndexer, SolanaIndexer};
use log::{error, info};
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::commitment_config::CommitmentConfig;
use solana_sdk::pubkey::Pubkey;
use solidity_structs::{
    AcknowledgementMetadataStake, IntentPayloadEnum, IntentProcessorBoundMessageAcknowledgementData, IntentProcessorBoundMessageEnum, ResultCosts, SolidityIntentProcessorBoundMessage, SolidityOrder
};
use std::collections::HashMap;
use std::fs;
use std::str::FromStr;
use std::sync::Arc;
use std::time::SystemTime;
use tokio::signal;
use tokio_postgres::NoTls;
use utils::{get_api_url, structure_intent_orders};

#[derive(Deserialize)]
struct IndexerConfig {
    url: String,
    contract: String,
    #[serde(default)]
    chain_id: i64,
    execution_environment: String,
    #[serde(default)]
    mockln_contract: String,
}

#[derive(Deserialize)]
struct Config {
    indexers: Vec<IndexerConfig>,
}

impl Config {
    fn from_file(file_path: &str) -> Self {
        let content = fs::read_to_string(file_path).expect("Failed to read configuration file");
        toml::from_str(&content).expect("Failed to parse configuration file")
    }
}

fn create_evm_indexer(url: &str, address: &str) -> Box<dyn BlockchainIndexer + Send + Sync> {
    Box::new(EvmIndexer::new(url.to_string(), address.to_string()))
}

fn create_solana_indexer(
    url: &str,
    program_id: &str,
    chain_id: &i64,
) -> Box<dyn BlockchainIndexer + Send + Sync> {
    Box::new(SolanaIndexer::new(
        url.to_string(),
        url.to_string(),
        *chain_id,
        program_id.to_string(),
    ))
}

#[derive(Deserialize)]
struct PaginationParams {
    id: Option<i64>,
    per_page: Option<u32>,
}

#[derive(Deserialize)]
struct CalculateStakeInitialDepositParams {
    token_address: String,
    chain_id: String,
    user_address: String,
}

#[derive(Deserialize)]
struct UnbondingBalance {
    user_address: String,
}

const SOLANA_ACCOUNT_RENT: u64 = 890880;

#[derive(Deserialize)]
struct TimeoutParams {
    chain_id: Option<i64>,
    limit: Option<i64>,
}

#[derive(Serialize)]
struct TimedOutOrder {
    intent_id: i64,
    order_id: i64,
    dln_order_id: Option<String>,
    timeout_timestamp: i64,
    current_timestamp: i64,
    elapsed_seconds: i64,
    chain_id: i64,
    destination_chain_id: Option<String>,
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    env_logger::builder()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{} - Thread: {:?} - Target: {}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                std::thread::current().id(),
                record.target(),
                record.args()
            )
        })
        .init();

        let connect_statement = std::env::var("DB_CONNECTION_STRING")
        .expect("DB_CONNECTION_STRING must be set");
    
        let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

        builder.set_verify(openssl::ssl::SslVerifyMode::NONE);
    
        let mut connector = MakeTlsConnector::new(builder.build());

        let (client, connection) =
            tokio_postgres::connect(&connect_statement, connector)
                .await
                .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("DB Connection error {:?}", e);
        }
    });

    let db_client = Arc::new(client);
    let db_server_client = Arc::clone(&db_client);

    let config_path = std::env::var("CONFIG_PATH").unwrap_or("config.toml".to_string());
    let config = Config::from_file(&config_path);

    let mut indexers: Vec<Box<dyn BlockchainIndexer + Send + Sync>> = vec![];
    config.indexers.iter().for_each(|conf| {
        if conf.execution_environment == "EVM" {
            if !conf.mockln_contract.is_empty() {
                indexers.push(create_evm_indexer(&conf.url, &conf.mockln_contract));
            }
            indexers.push(create_evm_indexer(&conf.url, &conf.contract));
        } else if conf.execution_environment == "SVM" {
            indexers.push(create_solana_indexer(
                &conf.url,
                &conf.contract,
                &conf.chain_id,
            ));
        } else {
            panic!(
                "Invalid execution environment: {}",
                conf.execution_environment
            );
        }
    });

    let mut handles = vec![];
    for indexer in indexers {
        let client_clone = Arc::clone(&db_client);
        let handle = tokio::spawn(async move {
            if let Err(err) = indexer.listen_for_events(client_clone).await {
                error!("Error listening to events: {}", err);
            }
        });
        handles.push(handle);
    }

    info!("All tasks started. Press Ctrl+C to exit.");
    info!("total threads {:?}", handles.len());

    // Start the API server
    let _api_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Arc::clone(&db_server_client)))
            .route("/intents/{intent_id}", web::get().to(fetch_intents))
            .route(
                "/intents/history/{initiator_address}",
                web::get().to(fetch_transaction_history),
            )
            .route("/transactions", web::get().to(fetch_transactions))
            .route(
                "/initialdeposit",
                web::get().to(calculate_stake_initial_deposit),
            )
            .route(
                "/contract_balance/{network}",
                web::post().to(fetch_contract_balance),
            )
            .route("/get_ack_metadata", web::get().to(fetch_ack_metadata))
            .route("/timed_out_orders", web::get().to(fetch_timed_out_orders))
            .route(
                "/get_deposit_history/{user_address}",
                web::get().to(get_deposit_history),
            )
            .route("/check_ofac_list/{user_address}", web::get().to(check_ofac_list))
            .route("/get_fee_data", web::get().to(get_fee_data))
    })
    .bind("0.0.0.0:8085")
    .unwrap()
    .run()
    .await;

    // Wait for Ctrl+C signal to exit
    signal::ctrl_c().await.unwrap();
    info!("Shutting down...");
}

#[derive(serde::Serialize, Debug)]
struct TransactionData {
    id: i64,
    intentId: i64,
    createdAt: DateTime<Utc>,
    status: String,
    isDeposit: Option<bool>, // we don't have this currently
    senderAddress: String,
    solverAddress: Option<String>,
    source: Option<OrderTransactionData>,
    destination: Option<OrderTransactionData>,
    initial_data: Option<InitialData>,
    intent_type: Option<String>,
}

// both for source and destination txn data
#[derive(serde::Serialize, Debug)]
struct OrderTransactionData {
    chainId: String,
    tokenIn: String,
    tokenOut: String,
    txHash: Option<String>,
    explorerLink: Option<String>,
    amountIn: String,
    amountOut: String,
    order_payload: Option<String>,
    orderId: Option<i64>,
}

#[derive(serde::Serialize, Debug)]
struct InitialData {
    id: i64,
    intent_id: i64,
    origin_chain: Option<String>, // source
    target_chain: Option<String>, // destination
    token_in: Option<String>,
    amount_in: Option<String>,
    token_out: Option<String>,
    amount_out: Option<String>,
    initiator_address: String,
    solver_address: Option<String>,
    ack_result: Option<bool>,
    ack_tx_status: String,
    ack_error_message: Option<String>,
    solver_tx_hash: Option<String>,
    ack_tx_hash: Option<String>,
    fulfill_tx_hash: Option<String>,
    intent_version: i32, // check if more needed
    receiver_address: Option<String>,
    feeAmount: Option<String>
}

// Handler to fetch data from the database
async fn fetch_intents(
    client: web::Data<Arc<tokio_postgres::Client>>,
    intent_id: web::Path<i64>,
) -> impl Responder {
    let query_intent = "SELECT * FROM intent WHERE intent_id = $1";
    let query_intent_state =
        "SELECT * FROM intent_state WHERE intent_id = $1 ORDER BY version DESC LIMIT 1"; // get state by intent id
    let query_orders = "SELECT * FROM order_created WHERE intent_id = $1"; // we will get 2 orders for 1 intent id

    let query_solution = "SELECT * FROM solution WHERE intent_id = $1";
    let query_ack = "SELECT * FROM acknowledgement WHERE intent_id = $1";

    match client.query_one(query_intent, &[intent_id.as_ref()]).await {
        Ok(intent_rows) => {
            let intent_row = intent_rows.clone();
            let intent_id: i64 = intent_row.get("intent_id");
            let intent_state = client
                .query_one(query_intent_state, &[&intent_id])
                .await
                .unwrap();
            let stage: String = intent_state.get("stage");
            let intent_version: i32 = intent_state.get("version");
            let sender_address: String = intent_row.get("owner_address");

            let (solver_address, solver_transaction_hash) =
                match client.query_one(query_solution, &[&intent_id]).await {
                    Ok(solution) => {
                        let solver_address: String = solution.get("solver_address");
                        let solver_transaction_hash: String = solution.get("transaction_hash");
                        (Some(solver_address), Some(solver_transaction_hash))
                    }
                    Err(_) => (None, None),
                };

            let intent_orders = match intent_version > 1 {
                true => {
                    let intent_order_rows = match client.query(query_orders, &[&intent_id]).await {
                        Ok(rows) => Some(rows),
                        Err(_e) => None,
                    };
                    intent_order_rows
                }
                false => None,
            };

            let ack_row = match intent_version {
                5 => match client.query_one(query_ack, &[&intent_id]).await {
                    Ok(row) => Some(row),
                    Err(_e) => None,
                },
                _ => None,
            };

            let (source_transaction_data, destination_transaction_data, initial_data, intent_type) =
                structure_intent_orders(
                    &client,
                    &intent_row,
                    intent_orders,
                    ack_row,
                    solver_address.clone(),
                    solver_transaction_hash,
                    intent_version,
                    &sender_address,
                )
                .await;

            let created_at: SystemTime = intent_row.get("timestamp");
            let datetime: DateTime<Utc> = created_at.into();

            let transaction_data: TransactionData = TransactionData {
                id: intent_row.get("id"),
                intentId: intent_id,
                createdAt: datetime,
                status: stage,
                isDeposit: None,
                senderAddress: intent_row.get("owner_address"),
                solverAddress: solver_address,
                source: source_transaction_data,
                destination: destination_transaction_data,
                initial_data: initial_data,
                intent_type,
            };

            HttpResponse::Ok().json(transaction_data)
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[derive(serde::Serialize, Debug)]
struct TransactionHistory {
    id: i64,
    intent_id: i64,
    status: String,
    version: i32,
    timestamp: Option<DateTime<Utc>>,
}

#[derive(serde::Serialize, Debug, Deserialize)]
struct FilterTokens {
    tokens: Option<String>,
}

async fn fetch_transaction_history(
    client: web::Data<Arc<tokio_postgres::Client>>,
    initiator_address: web::Path<String>,
    pagination_query: web::Query<PaginationParams>,
    query: web::Query<FilterTokens>,
) -> impl Responder {
    let per_page = pagination_query.per_page.unwrap_or(10) as i64;
    let id = pagination_query.id;

    let token_address_list: Option<Vec<String>> = query
        .tokens
        .as_ref()
        .map(|tokens| tokens.split(',').map(|s| s.trim().to_string()).collect());

    let sender_address = initiator_address.to_string();
    let query_orders = "SELECT * FROM order_created WHERE intent_id = $1";
    let query_intent_state =
        "SELECT * FROM intent_state WHERE intent_id = $1 ORDER BY version DESC LIMIT 1"; // get state by intent id
    let query_solution = "SELECT * FROM solution WHERE intent_id = $1";
    let query_ack = "SELECT * FROM acknowledgement WHERE intent_id = $1";

    let mut data: Vec<TransactionData> = vec![];

    let (query_intents, params): (&str, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) = match id
    {
        Some(ref cursor_id) => (
            "SELECT * FROM intent WHERE owner_address = $1 AND id < $2 ORDER BY id DESC LIMIT $3",
            vec![&sender_address, cursor_id, &per_page],
        ),
        None => (
            "SELECT * FROM intent WHERE owner_address = $1 ORDER BY id DESC LIMIT $2",
            vec![&sender_address, &per_page],
        ),
    };

    let intent_rows = match client.query(query_intents, &params).await {
        Ok(rows) => rows,
        Err(_) => Vec::default(),
    };

    if intent_rows.is_empty() {
        HttpResponse::InternalServerError().finish();
    }

    for intent_row in intent_rows {
        let intent_id: i64 = intent_row.get("intent_id");
        let intent_state = match client
            .query_one(query_intent_state, &[&intent_id])
            .await {
                Ok(state) => state,
                Err(_e) => {
                    error!("Error fetching intent state for intent_id: {}, error {:?}", intent_id, _e);
                    continue
                },
            };
        let stage: String = intent_state.get("stage");
        let intent_version: i32 = intent_state.get("version");

        let (solver_address, solver_transaction_hash) =
            match client.query_one(query_solution, &[&intent_id]).await {
                Ok(solution) => {
                    let solver_address: String = solution.get("solver_address");
                    let solver_transaction_hash: String = solution.get("transaction_hash");
                    (Some(solver_address), Some(solver_transaction_hash))
                }
                Err(_) => (None, None),
            };

        let intent_orders = match client.query(query_orders, &[&intent_id]).await {
            Ok(rows) => {
                if rows.len() == 0 {
                    continue;
                }
                let source_token_in: String = rows[0].get("token_in");
                match token_address_list {
                    Some(ref token_addresses) => {
                        if token_addresses.contains(&source_token_in) {
                            Some(rows)
                        } else {
                            continue;
                        }
                    }
                    None => Some(rows),
                }
            }
            Err(_e) => continue,
        };

        let ack_row = match intent_version {
            5 => match client.query_one(query_ack, &[&intent_id]).await {
                Ok(row) => Some(row),
                Err(_e) => None,
            },
            _ => None,
        };

        //
        let (source_transaction_data, destination_transaction_data, initial_data, intent_type) =
            structure_intent_orders(
                &client,
                &intent_row,
                intent_orders,
                ack_row,
                solver_address.clone(),
                solver_transaction_hash,
                intent_version,
                &sender_address,
            )
            .await;

        let created_at: SystemTime = intent_row.get("timestamp");
        let datetime: DateTime<Utc> = created_at.into();

        let transaction_data: TransactionData = TransactionData {
            id: intent_row.get("id"),
            intentId: intent_id,
            createdAt: datetime,
            status: stage,
            isDeposit: None,
            senderAddress: intent_row.get("owner_address"),
            solverAddress: solver_address,
            source: source_transaction_data,
            destination: destination_transaction_data,
            initial_data: initial_data,
            intent_type,
        };

        data.push(transaction_data);
    }
    HttpResponse::Ok().json(data) // Return JSON response
}

async fn fetch_transactions(
    client: web::Data<Arc<tokio_postgres::Client>>,
    query: web::Query<PaginationParams>,
) -> impl Responder {
    let per_page = query.per_page.unwrap_or(10) as i64;
    let id = query.id;

    let query_intent_state =
        "SELECT * FROM intent_state WHERE intent_id = $1 ORDER BY version DESC LIMIT 1";

    let (query_intent, params): (&str, Vec<&(dyn tokio_postgres::types::ToSql + Sync)>) = match id {
        Some(ref cursor_id) => (
            "SELECT id, intent_id, timestamp FROM intent WHERE id < $1 ORDER BY id DESC LIMIT $2",
            vec![cursor_id, &per_page],
        ),
        None => (
            "SELECT id, intent_id, timestamp FROM intent ORDER BY id DESC LIMIT $1",
            vec![&per_page],
        ),
    };

    match client.query(query_intent, &params).await {
        Ok(intent_row) => {
            let mut data: Vec<TransactionHistory> = vec![];
            for row in intent_row {
                let intent_id: i64 = row.get("intent_id");
                let timestamp: SystemTime = row.get("timestamp");
                let datetime: DateTime<Utc> = timestamp.into();

                match client.query_one(query_intent_state, &[&intent_id]).await {
                    Ok(state) => {
                        let txn: TransactionHistory = TransactionHistory {
                            id: row.get("id"),
                            intent_id,
                            status: state.get("stage"),
                            version: state.get("version"),
                            timestamp: Some(datetime),
                        };
                        data.push(txn);
                    }
                    Err(_) => continue,
                }
            }
            HttpResponse::Ok().json(data) // Return JSON response
        }
        Err(_e) => HttpResponse::InternalServerError().finish(),
    }
}

async fn calculate_stake_initial_deposit(
    client: web::Data<Arc<tokio_postgres::Client>>,
    query: web::Query<CalculateStakeInitialDepositParams>,
) -> impl Responder {
    let querydb = "SELECT intent_id, order_payload, amount_in FROM order_created WHERE token_in=$1 AND source_chain_id=$2 AND creator_address=$3 AND solution_type=3";

    let amount = match client
        .query(
            querydb,
            &[&query.token_address, &query.chain_id, &query.user_address],
        )
        .await
    {
        Ok(orders) => {
            let mut order_amount: U256 = U256::from(0);
            for order in orders {
                let intent_id: i64 = order.get("intent_id");
                let intent_state_query =
                    "SELECT MAX(version) as latest_version FROM intent_state WHERE intent_id=$1";
                let intent_version: i32 = match client
                    .query_one(intent_state_query, &[&intent_id])
                    .await
                {
                    Ok(res) => {
                        let version: i32 = res.get("latest_version");
                        version
                    }
                    Err(e) => {
                        error!("Error fetching version {:?}, excluding this intent_id {:?} from calculation", e, intent_id);
                        0
                    }
                };
                if intent_version <= 4 {
                    // no ack started or received
                    continue;
                }
                let order_payload: String = order.get("order_payload");
                let amount_in: String = order.get("amount_in");
                let order_bytes =
                    hex::decode(order_payload.strip_prefix("0x").unwrap_or(&order_payload))
                        .expect("Invalid hex");
                let order_bytes = Bytes::from(order_bytes);

                match SolidityOrder::abi_decode(&order_bytes, true) {
                    Ok(_data) => {
                        order_amount += U256::from_str_radix(amount_in.as_str(), 10).unwrap();
                    }
                    Err(e) => {
                        error!("error {:?}", e);
                        order_amount = U256::from(0);
                        break;
                    }
                };
            }
            order_amount // Return JSON response
        }
        Err(e) => {
            error!("error {:?}", e);
            U256::from(0)
        }
    };

    info!("amount {:?}", amount);
    HttpResponse::Ok().json(amount)
}

async fn fetch_ack_metadata(
    client: web::Data<Arc<tokio_postgres::Client>>,
    query: web::Query<UnbondingBalance>,
) -> impl Responder {
    let request_ids = get_stake_request_ids(&client, &query.user_address).await;
    info!("request_ids {:?}", request_ids);
    HttpResponse::Ok().json(request_ids)
}

async fn get_stake_request_ids(
    client: &Arc<tokio_postgres::Client>,
    user_address: &str,
) -> Vec<U256> {
    let query_order =
        "SELECT intent_id FROM order_created WHERE creator_address=$1 AND solution_type=3";
    let mut request_ids = Vec::new();

    let order_rows = match client.query(query_order, &[&user_address]).await {
        Ok(rows) => rows,
        Err(e) => {
            error!("Error querying orders: {:?}", e);
            return request_ids;
        }
    };

    for order_row in order_rows {
        if let Some(request_id) =
            get_request_id_from_intent(client, order_row.get("intent_id")).await
        {
            request_ids.push(request_id);
        }
    }

    request_ids
}

async fn get_request_id_from_intent(
    client: &Arc<tokio_postgres::Client>,
    intent_id: i64,
) -> Option<U256> {
    let query_vault = "SELECT message FROM message_dispatched_from_vault WHERE intent_id=$1";

    let vault_row = match client.query_one(query_vault, &[&intent_id]).await {
        Ok(row) => row,
        Err(_) => return None,
    };

    let ack_message: String = vault_row.get("message");
    let ack_message_bytes =
        match hex::decode(ack_message.strip_prefix("0x").unwrap_or(&ack_message)) {
            Ok(bytes) => Bytes::from(bytes),
            Err(_) => return None,
        };

    parse_ack_message(&ack_message_bytes)
}

fn parse_ack_message(ack_message_bytes: &Bytes) -> Option<U256> {
    let ipb_message =
        SolidityIntentProcessorBoundMessage::abi_decode(ack_message_bytes, true).ok()?;

    // Return early if not an Acknowledgement message
    if !matches!(
        ipb_message.enumVariant,
        IntentProcessorBoundMessageEnum::Acknowledgement
    ) {
        return None;
    }

    let ack_data =
        IntentProcessorBoundMessageAcknowledgementData::abi_decode(&ipb_message.data, true).ok()?;

    // Return early if not a Stake message
    if !matches!(ack_data.metadata.enumVariant, IntentPayloadEnum::Stake) {
        return None;
    }

    let ack_metadata_struct =
        AcknowledgementMetadataStake::abi_decode(&ack_data.metadata.data, true).ok()?;

    info!("Got request id {:?}", ack_metadata_struct.request_id);
    Some(ack_metadata_struct.request_id)
}

#[derive(Serialize, Debug, Deserialize)]
struct TokenBalance {
    tokenBalance: Option<String>,
    contractAddress: Option<String>,
    decimals: Option<String>,
    name: Option<String>,
    symbol: Option<String>,
}

#[derive(Serialize, Debug, Deserialize)]
struct ContractBalance {
    address: Option<String>,
    message: Option<String>,
    tokenBalances: Vec<TokenBalance>,
    status: Option<String>,
}

#[derive(Serialize, Debug)]
struct ContractBalanceResponse {
    data: HashMap<String, Vec<TokenBalance>>,
}

#[derive(Deserialize, Debug)]
struct BalanceParams {
    contract_address: String,
    chain_id: String,
}

#[derive(Deserialize, Debug)]
struct EVMBalanceParams {
    token_address: Vec<BalanceParams>,
}

#[derive(Deserialize, Debug)]
struct SolanaBalanceParams {
    user_address: Address,
    token_address: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type")]
pub enum ContractBalanceRequest {
    EVM(EVMBalanceParams),
    SVM(SolanaBalanceParams),
}

#[derive(Debug, Deserialize, Serialize)]
struct EVMbalances {
    result: ContractBalance,
}

#[derive(Debug, Deserialize, Serialize)]
struct EVMNativeBalance {
    result: Option<String>,
}

async fn fetch_contract_balance(
    client: web::Data<Arc<tokio_postgres::Client>>,
    network: web::Path<String>,
    req: web::Json<ContractBalanceRequest>,
) -> impl Responder {
    let mut balances: ContractBalanceResponse = ContractBalanceResponse {
        data: HashMap::new(),
    };

    let alchemy_api_key = std::env::var("ALCHEMY_API_KEY")
        .expect("ALCHEMY_API_KEY must be set")
        .parse::<String>()
        .unwrap();

    let SOLANA_PROGRAM_ID: Pubkey =
        Pubkey::from_str("CQTC16KM4XqjVJ8ASMPLxjv3siGAQLVcMauPGu1jMGNz").unwrap();

    match req.0 {
        ContractBalanceRequest::EVM(contracts) => {
            for contract in contracts.token_address {
                let chain_id = contract.chain_id;
                let contract_address = contract.contract_address;
                let api_url = match get_api_url(
                    network.to_string(),
                    chain_id.clone(),
                    alchemy_api_key.clone(),
                ) {
                    Some(url) => url,
                    None => continue,
                };
                let api_client = reqwest::Client::new();
                let mut contract_balances: Vec<TokenBalance> = vec![];
                let payload = json!({
                    "id": 1,
                    "jsonrpc": "2.0",
                    "method": "alchemy_getTokenBalances",
                    "params": [contract_address],
                });

                match api_client
                    .post(api_url)
                    .header("Content-Type", "application/json")
                    .header("accept", "application/json")
                    .json(&payload)
                    .send()
                    .await
                {
                    Ok(result) => {
                        let data: EVMbalances = result.json().await.unwrap();
                        contract_balances = data
                            .result
                            .tokenBalances
                            .into_iter() // Consumes the vector
                            .filter_map(|mut balance| {
                                if balance.tokenBalance != Some(String::from("0x0000000000000000000000000000000000000000000000000000000000000000")) {
                                    // Convert hex to decimal
                                    if let Ok(dec_value) = u128::from_str_radix(balance.tokenBalance.unwrap().trim_start_matches("0x"), 16) {
                                        balance.tokenBalance = Some(dec_value.to_string());
                                        return Some(balance);
                                    }
                                }
                                None
                            })
                            .collect(); // Collects filtered items into a vector
                    }
                    Err(_) => continue,
                };

                let native_api_url = match get_api_url(
                    network.to_string(),
                    chain_id.clone(),
                    alchemy_api_key.clone(),
                ) {
                    Some(url) => url,
                    None => continue,
                };
                let native_payload = json!(
                    {
                        "id": 1,
                        "jsonrpc": "2.0",
                        "method": "eth_getBalance",
                        "params": [contract_address]
                    }
                );
                match api_client
                    .post(native_api_url)
                    .header("Content-Type", "application/json")
                    .json(&native_payload)
                    .send()
                    .await
                {
                    Ok(result) => {
                        let data: EVMNativeBalance = result.json().await.unwrap();
                        let result_balance = data.result;
                        let native_balance: TokenBalance = TokenBalance {
                            tokenBalance: Some(
                                u128::from_str_radix(
                                    result_balance.unwrap().trim_start_matches("0x"),
                                    16,
                                )
                                .unwrap()
                                .to_string(),
                            ),
                            contractAddress: Some(String::from(
                                "0x1111111111111111111111111111111111111111",
                            )),
                            decimals: Some(String::from("18")),
                            name: Some(String::from("Native")),
                            symbol: Some(String::from("Native")),
                        };

                        contract_balances.push(native_balance);

                        balances.data.insert(chain_id, contract_balances);
                    }
                    Err(_) => continue,
                };
            }
        }
        ContractBalanceRequest::SVM(svm_meta_data) => {
            let mut rpc_url = "";
            let mut chain_id = "1399811149";
            match network.to_string().as_ref() {
                "testnet" => {
                    rpc_url = "http://api.devnet.solana.com";
                    chain_id = "4294967295";
                }
                "mainnet" => {
                    rpc_url = "https://mainnet.helius-rpc.com/?api-key=d4d3c545-bd81-405c-9e51-3f600e9c25ad";
                }
                &_ => error!("No Network match"),
            }

            let client =
                RpcClient::new_with_commitment(rpc_url.to_string(), CommitmentConfig::confirmed());

            let user_address: [u8; 20] = svm_meta_data.user_address.try_into().unwrap();

            let mut contract_balances: Vec<TokenBalance> = vec![];

            for token_address in svm_meta_data.token_address {
                if &token_address
                    == "0x1111111111111111111111111111111111111111111111111111111111111111"
                {
                    let native_token_mint = Pubkey::from_str(&String::from(
                        "29d2S7vB453rNYFdR5Ycwt7y9haRT5fwVwL9zTmBhfV2",
                    ))
                    .unwrap(); // bs58 of 0x111...
                    let native_token_account = Pubkey::find_program_address(
                        &[
                            b"user_deposit_address",
                            user_address.as_slice(),
                            native_token_mint.as_ref(),
                        ],
                        &SOLANA_PROGRAM_ID,
                    )
                    .0;

                    let sol_balance = match client
                        .get_balance(&Pubkey::new_from_array(native_token_account.to_bytes()))
                        .await
                    {
                        Ok(balance) => {
                            if balance >= SOLANA_ACCOUNT_RENT {
                                balance - SOLANA_ACCOUNT_RENT
                            } else {
                                if balance < 1000000 {
                                    let res: u64 = 0;
                                    res
                                } else {
                                    balance
                                }
                            }
                        } // subtract solana rent
                        Err(e) => {
                            error!("Error fetching SOL balance: {:?}", e);
                            0 // Default to 0 if there's an error
                        }
                    };

                    info!("SOL Balance: {}", sol_balance);
                    let native_balance: TokenBalance = TokenBalance {
                        tokenBalance: Some(sol_balance.to_string()),
                        contractAddress: Some(String::from(
                            "0x1111111111111111111111111111111111111111111111111111111111111111",
                        )),
                        decimals: Some(String::from("9")),
                        name: Some(String::from("Native")),
                        symbol: Some(String::from("Native")),
                    };

                    contract_balances.push(native_balance);
                } else {
                    let token_mint = Pubkey::from_str(&token_address).unwrap();
                    let token_account = Pubkey::find_program_address(
                        &[
                            b"user_deposit_address",
                            user_address.as_slice(),
                            token_mint.as_ref(),
                        ],
                        &SOLANA_PROGRAM_ID,
                    )
                    .0;

                    match client.get_account(&token_account).await {
                        Ok(account) => {
                            info!("Token account exists {:?}", account);

                            match client.get_token_account_balance(&token_account).await {
                                Ok(balance) => {
                                    let token_balance: TokenBalance = TokenBalance {
                                        tokenBalance: Some(balance.amount),
                                        contractAddress: Some(token_mint.to_string()),
                                        decimals: Some(String::from("9")),
                                        name: None,
                                        symbol: None,
                                    };
                                    contract_balances.push(token_balance);
                                }
                                Err(e) => {
                                    error!("Error in fetching balance of derived account {:?}", e);
                                    continue;
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error in fetching account of derived address {:?}", e);
                            continue;
                        }
                    }
                }
            }
            balances
                .data
                .insert(String::from(chain_id), contract_balances);
        }
    }

    HttpResponse::Ok().json(balances)
}

async fn fetch_timed_out_orders(
    client: web::Data<Arc<tokio_postgres::Client>>,
    query: web::Query<TimeoutParams>,
) -> impl Responder {
    let limit = query.limit.unwrap_or(100);
    let chain_id_filter = query
        .chain_id
        .map(|id| format!("AND r.chain_id = {}", id))
        .unwrap_or_default();

    // Get current timestamp in seconds
    let current_timestamp = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64;

    // Join with order_created to get destination_chain_id and intent_state to get latest version (2 or 4)
    let query = format!(
        "WITH latest_messages AS (
            SELECT DISTINCT ON (intent_id) *
            FROM received_message_on_vault
            WHERE dln_order_id IS NOT NULL
            ORDER BY intent_id, id DESC
         )
         SELECT r.intent_id, r.order_id, r.dln_order_id, r.timeout_unix_timestamp_in_sec, r.chain_id, o.destination_chain_id 
         FROM latest_messages r
         LEFT JOIN order_created o ON r.intent_id = o.intent_id
         JOIN (
             SELECT intent_id FROM intent_state GROUP BY intent_id HAVING MAX(version) = 3 ORDER BY intent_id
         ) i ON r.intent_id = i.intent_id
         WHERE r.timeout_unix_timestamp_in_sec < {} {} 
         ORDER BY r.timeout_unix_timestamp_in_sec DESC 
         LIMIT {}",
        current_timestamp, chain_id_filter, limit
    );

    match client.query(query.as_str(), &[]).await {
        Ok(rows) => {
            let timed_out_orders: Vec<TimedOutOrder> = rows
                .iter()
                .map(|row| {
                    let timeout_timestamp: i64 = row.get("timeout_unix_timestamp_in_sec");
                    let elapsed = current_timestamp - timeout_timestamp;

                    TimedOutOrder {
                        intent_id: row.get("intent_id"),
                        order_id: row.get("order_id"),
                        dln_order_id: row.get("dln_order_id"),
                        timeout_timestamp,
                        current_timestamp,
                        elapsed_seconds: elapsed,
                        chain_id: row.get("chain_id"),
                        destination_chain_id: row.get("destination_chain_id"),
                    }
                })
                .collect();

            HttpResponse::Ok().json(timed_out_orders)
        }
        Err(e) => {
            error!("Error fetching timed out orders: {:?}", e);
            HttpResponse::InternalServerError().json(json!({
                "error": format!("Failed to fetch timed out orders: {}", e)
            }))
        }
    }
}

#[derive(Serialize)]
struct DepositHistory {
    id: i64,
    user_address: String,
    token_address: String,
    chain_id: String,
    amount: String,
    timestamp: Option<DateTime<Utc>>,
    transaction_hash: Option<String>,
    message_id: Option<String>,
    status: Option<i32>
}

async fn get_deposit_history(
    client: web::Data<Arc<tokio_postgres::Client>>,
    user_address: web::Path<String>,
) -> impl Responder {
    let query = "SELECT * FROM deposit_received WHERE LOWER(user_address) = LOWER($1)";

    match client.query(query, &[&user_address.to_string()]).await {
        Ok(deposit_row) => {
            let mut data: Vec<DepositHistory> = vec![];
            for row in deposit_row {
                let id: i64 = row.get("id");
                let timestamp: SystemTime = row.get("timestamp");
                let datetime: DateTime<Utc> = timestamp.into();
                let token_address: String = row.get("token_address");
                let chain_id: String = row.get("chain_id");
                let amount: String = row.get("amount");
                let transaction_hash: Option<String> = row.get("source_transaction_hash");
                let message_id: Option<String> = row.get("message_id");
                let status: Option<i32> = row.get("status");

                let txn: DepositHistory = DepositHistory {
                    id,
                    user_address: user_address.to_string(),
                    token_address,
                    chain_id,
                    amount,
                    timestamp: Some(datetime),
                    transaction_hash,
                    message_id,
                    status
                };
                data.push(txn);
            }
            HttpResponse::Ok().json(data) // Return JSON response
        }
        Err(_e) => HttpResponse::InternalServerError().finish(),
    }
}

#[derive(Serialize)]
struct OFACResponse {
    block_request: bool
}

async fn check_ofac_list(
    client: web::Data<Arc<tokio_postgres::Client>>,
    user_address: web::Path<String>,
) -> impl Responder {
    let query = "SELECT address FROM sanction_address_list WHERE address = $1";
    println!("printing query");
    match client.query(query, &[&user_address.to_string()]).await {
        Ok(ofac_row) => {
            let data: OFACResponse;
            if ofac_row.len() > 0 {
                data = OFACResponse {
                    block_request: true
                }
            } else {
                data = OFACResponse {
                    block_request: false
                } 
            }
            HttpResponse::Ok().json(data) // Return JSON response
        }
        Err(_e) => {
            println!("Error {:?}", _e);
            HttpResponse::InternalServerError().finish()
        },
    }
}

async fn get_fee_data(
    client: web::Data<Arc<tokio_postgres::Client>>
) -> impl Responder {
    // let query_intent = "SELECT intent_id, feeAmount FROM intent";
    let query_fees = "SELECT intent_id, fees FROM intent_fees";
    match client.query(query_fees, &[]).await {
        Ok(rows) => {
            let fee_data: Vec<ResultCosts> = rows
                .into_iter()
                .filter_map(|row| {
                    let json: Option<serde_json::Value> = row.get("fees");
                    json.and_then(|v| serde_json::from_value(v).ok())
                })
                .collect();
            HttpResponse::Ok().json(fee_data) // Return JSON response
        },
        Err(_e) => {
            error!("Error {:?}", _e);
            HttpResponse::NotFound().finish()
        }
    }
}