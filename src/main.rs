#![allow(non_snake_case)] // TODO: fix the identifiers and use `serde(rename_all = "snakeCase")` to properly name the fields

// src/main.rs
mod constants;
mod events;
mod indexers;
pub mod solidity_structs;
mod utils;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use anchor_lang::pubkey;
use dotenv::dotenv;
use indexers::{backfill, BlockchainIndexer, EvmIndexer, SolanaIndexer};
use log::{debug, error, info};
use openssl::ssl::{SslConnector, SslMethod};
use postgres_openssl::MakeTlsConnector;
use serde::{Deserialize, Serialize};
use serde_json::json;
use solana_sdk::pubkey::Pubkey;
use solidity_structs::OrdersResponse;
use std::{fs, sync::Arc, thread::sleep, time::Duration};
use tokio::{signal, sync::broadcast};

use crate::constants::DEFAULT_ORDER_ID;

/// Only needed to satisfy Anchor's proc-macros
pub const ID: Pubkey = pubkey!("XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX");

#[derive(Deserialize, Clone)]
pub struct IndexerConfig {
    pub url: String,
    pub ws_url: String,
    pub contract: String,
    #[serde(default)]
    pub chain_id: i64,
    pub execution_environment: String,
    #[serde(default)]
    pub mockln_contract: String,
    #[serde(default)]
    amm_contract: Option<String>,
}

#[derive(Deserialize, Clone)]
pub struct Config {
    pub indexers: Vec<IndexerConfig>,
    pub solana_chain_id: u64,
    pub solana_vaults_program_id: String,
    pub debridge_order_maker_address: String,
}

impl Config {
    fn from_file(file_path: &str) -> Self {
        let content = fs::read_to_string(file_path).expect("Failed to read configuration file");
        toml::from_str(&content).expect("Failed to parse configuration file")
    }
}

fn create_evm_indexer(
    url: &str,
    ws_url: &str,
    vaults_address: &str,
    amm_address: Option<String>,
    solana_chain_id: u64,
) -> Box<dyn BlockchainIndexer + Send + Sync> {
    Box::new(EvmIndexer::new(
        url.to_string(),
        ws_url.to_string(),
        vaults_address.to_string(),
        amm_address,
        solana_chain_id,
    ))
}

fn create_solana_indexer(
    url: &str,
    vaults_program_id: &str,
    amm_program_id: &str,
    chain_id: &i64,
) -> Box<dyn BlockchainIndexer + Send + Sync> {
    Box::new(SolanaIndexer::new(
        url.to_string(),
        url.to_string(),
        *chain_id,
        vaults_program_id.to_string(),
        amm_program_id.to_string(),
    ))
}

#[derive(Debug, Serialize, Deserialize)]
struct OrderNonceResponse {
    makerOrderNonce: u64,
}

#[derive(Clone)]
struct SolanaConfig {
    solana_chain_id: u64,
    solana_vaults_program_id: String,
    debridge_order_maker_address: String,
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    env_logger::builder()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{} - Thread: {:?} - Target: {}] {} \n",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                std::thread::current().id(),
                record.target(),
                record.args()
            )
        })
        .init();

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let connect_statement =
        std::env::var("DB_CONNECTION_STRING").expect("DB_CONNECTION_STRING must be set");

    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();

    builder.set_verify(openssl::ssl::SslVerifyMode::NONE);

    let connector = MakeTlsConnector::new(builder.build());

    let (client, connection) = tokio_postgres::connect(&connect_statement, connector)
        .await
        .unwrap();

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("DB Connection error {:?}", e);
        }
    });

    let db_client = Arc::new(client);
    let db_client_clone = Arc::clone(&db_client);

    let config_path = std::env::var("CONFIG_PATH").unwrap_or("config.toml".to_string());
    let config = Config::from_file(&config_path).clone();
    let config_clone = Arc::new(config.clone());
    tokio::spawn(async move {
        let client_clone = Arc::clone(&db_client_clone);
        backfill::fill_history_intents(config_clone.clone(), client_clone).await;
        info!("History intents backfill completed.");
    });

    let mut indexers: Vec<Box<dyn BlockchainIndexer + Send + Sync>> = vec![];
    config.indexers.iter().for_each(|conf| {
        if conf.execution_environment == "EVM" {
            if !conf.mockln_contract.is_empty() {
                indexers.push(create_evm_indexer(
                    &conf.url,
                    &conf.ws_url,
                    &conf.mockln_contract,
                    conf.amm_contract.clone(),
                    config.solana_chain_id,
                ));
            }
            indexers.push(create_evm_indexer(
                &conf.url,
                &conf.ws_url,
                &conf.contract,
                conf.amm_contract.clone(),
                config.solana_chain_id,
            ));
        } else if conf.execution_environment == "SVM" {
            let amm_program_id = conf
                .amm_contract
                .as_ref()
                .ok_or_else(|| anyhow::anyhow!("AMM contract not set for Solana chain"))
                .unwrap();
            indexers.push(create_solana_indexer(
                &conf.url,
                &conf.contract,
                amm_program_id,
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
        let mut shutdown_rx = shutdown_tx.subscribe();
        let handle = tokio::spawn(async move {
            tokio::select! {
                _ = async {
                    if let Err(err) = indexer.listen_for_events(client_clone).await {
                        error!("Error listening to events: {}", err);
                    }
                } => {},
                _ = shutdown_rx.recv() => {
                    info!("Indexer received shutdown signal.");
                }
            }
        });
        handles.push(handle);
    }

    let solana_config = SolanaConfig {
        solana_chain_id: config.solana_chain_id,
        solana_vaults_program_id: config.solana_vaults_program_id.clone(),
        debridge_order_maker_address: config.debridge_order_maker_address.clone(),
    };

    let solana_config_clone = Arc::new(solana_config.clone());

    let handle = tokio::spawn(async move {
        fetch_solana_debridge_orders(db_client.clone(), &solana_config.clone()).await
    });
    handles.push(handle);

    info!("All tasks started. Press Ctrl+C to exit.");
    info!("total threads {:?}", handles.len());

    // Start the API server
    let api_server =
        HttpServer::new(move || App::new().route("/health_check", web::get().to(health_check)))
            .bind("0.0.0.0:8085")
            .unwrap()
            .run()
            .await;

    // Wait for Ctrl+C signal to exit
    signal::ctrl_c().await.unwrap();
    info!("Shutting down...");
}

async fn fetch_solana_debridge_orders(
    db_client: Arc<tokio_postgres::Client>,
    config: &SolanaConfig,
) {
    loop {
        sleep(Duration::from_secs(10));
        let orders = skip_fail!(fetch_orders(db_client.clone(), &config).await);
        info!(target: "DLN_ORDER_ID_UPDATES", "Orders: {:?}", orders.orders.len());
    }
}

async fn fetch_orders(
    db_client: Arc<tokio_postgres::Client>,
    config: &SolanaConfig,
) -> Result<OrdersResponse, reqwest::Error> {
    let client = reqwest::Client::new();
    let request_body = json!({
        "giveChainIds": [],
        "takeChainIds": [],
        "filter": config.debridge_order_maker_address,
        "skip": 0,
        "take": 25
    });

    let response = client
        .post("https://stats-api.dln.trade/api/Orders/filteredList")
        .json(&request_body)
        .send()
        .await?;

    let body = response.text().await?;
    let orders: Result<OrdersResponse, serde_json::Error> = serde_json::from_str(&body);
    if orders.is_err() {
        error!(target: "DLN_ORDER_ID_UPDATES", "Error fetching orders: {:?}", orders.err());
        return Ok(OrdersResponse { orders: vec![] });
    }

    let orders = orders.unwrap();

    let default_order_id = DEFAULT_ORDER_ID.to_string();
    let till_order_id = get_latest_solana_dln_order_id(&db_client, &config)
        .await
        .ok()
        .flatten()
        .unwrap_or(default_order_id);

    let mut updates = 0;

    for order in orders.orders.clone() {
        let dln_order_id = if let Some(id) = order.orderId.stringValue {
            id
        } else {
            continue;
        };
        let order_nonce = skip_fail!(fetch_order_nonce(dln_order_id.clone()).await) as i64;
        debug!(target: "DLN_ORDER_ID_UPDATES", "Order nonce: {:?}", order_nonce);
        // Update the received_message_on_vault table with the order nonce
        let update_query =
            "UPDATE received_message_on_vault SET dln_order_id = $1 WHERE order_id = $2";
        match db_client
            .execute(update_query, &[&dln_order_id.to_string(), &order_nonce])
            .await
        {
            Ok(_) => {
                debug!(target: "DLN_ORDER_ID_UPDATES", "Updated order nonce for order_id: {}", dln_order_id)
            }
            Err(e) => error!(target: "DLN_ORDER_ID_UPDATES", "Error updating order nonce: {:?}", e),
        }
        updates += 1;
        if dln_order_id == till_order_id {
            break;
        }
    }

    info!(target: "DLN_ORDER_ID_UPDATES", "Updated {} order nonces", updates);

    Ok(orders)
}

async fn get_latest_solana_dln_order_id(
    client: &Arc<tokio_postgres::Client>,
    config: &SolanaConfig,
) -> Result<Option<String>, tokio_postgres::Error> {
    let query = "SELECT dln_order_id FROM received_message_on_vault WHERE chain_id = $1 AND dln_order_id IS NOT NULL ORDER BY timestamp DESC LIMIT 1";

    let solana_chain_id_i64 = config.solana_chain_id as i64;

    match client.query_opt(query, &[&solana_chain_id_i64]).await {
        Ok(row) => Ok(row.and_then(|r| r.get("dln_order_id"))),
        Err(e) => {
            error!("Error fetching latest dln_order_id: {:?}", e);
            Err(e)
        }
    }
}

async fn fetch_order_nonce(order_id: String) -> Result<u64, String> {
    let client = reqwest::Client::new();
    let response = client
        .get(&format!(
            "https://stats-api.dln.trade/api/Orders/{}",
            order_id
        ))
        .send()
        .await
        .map_err(|e| e.to_string())?;

    let body = response.text().await.map_err(|e| e.to_string())?;
    let order_nonce: Result<OrderNonceResponse, serde_json::Error> = serde_json::from_str(&body);
    if let Err(e) = order_nonce {
        error!(target: "DLN_ORDER_ID_UPDATES", "Error fetching order nonce: {:?}", e);
        return Err(e.to_string());
    }

    let order_nonce = order_nonce.unwrap();

    Ok(order_nonce.makerOrderNonce)
}

async fn health_check() -> impl Responder {
    HttpResponse::Ok().json(json!({
        "status": "ok",
        "message": "Indexer connection is healthy"
    }))
}
