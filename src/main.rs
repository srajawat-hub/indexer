// src/main.rs
mod events;
mod indexers;
pub mod solidity_structs;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use dotenv::dotenv;
use futures_util::future;
use indexers::{BlockchainIndexer, EvmIndexer, SolanaIndexer};
use log::{debug, error, info, trace};
use serde::Deserialize;
use std::fs;
use std::sync::Arc;
use std::{fmt::format, future::Future};
use tokio::signal;
use tokio_postgres::{connect, NoTls};

#[derive(Deserialize)]
struct IndexerConfig {
    url: String,
    contract: String,
}

#[derive(Deserialize)]
struct Config {
    indexers: Vec<IndexerConfig>,
}

impl Config {
    fn from_file(file_path: &str) -> Self {
        let content = fs::read_to_string(file_path)
            .expect("Failed to read configuration file");
        toml::from_str(&content)
            .expect("Failed to parse configuration file")
    }
}

fn create_evm_indexer(url: &str, address: &str) -> Box<dyn BlockchainIndexer + Send + Sync> {
    Box::new(EvmIndexer::new(url.to_string(), address.to_string()))
}

#[tokio::main]
async fn main() {
    dotenv().ok();

    env_logger::builder()
        .format(|buf, record| {
            use std::io::Write;
            writeln!(
                buf,
                "[{} - Thread: {:?}] {}",
                chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
                std::thread::current().id(),
                record.args()
            )
        })
        .init();

    // // make a db connection
    let db_host = std::env::var("DB_HOST")
        .expect("DB_HOST must be set")
        .parse::<String>()
        .unwrap();

    let db_user = std::env::var("DB_USER")
        .expect("DB_USER must be set")
        .parse::<String>()
        .unwrap();

    let db_password = std::env::var("DB_PASSWORD")
        .expect("DB_PASSWORD must be set")
        .parse::<String>()
        .unwrap();

    let db_name = std::env::var("DB_NAME")
        .expect("DB_NAME must be set")
        .parse::<String>()
        .unwrap();

    let connect_statement = format!(
        "host={} user={} password={} dbname={}",
        db_host, db_user, db_password, db_name
    );

    // let connect_statement = std::env::var("DB_CONNECTION_STRING").expect("DB_CONNECTION_STRING must be set").parse::<String>().unwrap();
    let (client, connection) = tokio_postgres::connect(&connect_statement, NoTls)
        .await
        .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("DB Connection error {:?}", e);
        }
    });

    let db_client = Arc::new(client);
    let db_server_client = Arc::clone(&db_client);

    let config = Config::from_file("config.toml");

    let indexers: Vec<Box<dyn BlockchainIndexer + Send + Sync>> = config.indexers.into_iter()
        .map(|conf| create_evm_indexer(&conf.url, &conf.contract))
        .collect();

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
    let api_server = HttpServer::new(move || {
        App::new()
            .app_data(web::Data::new(Arc::clone(&db_server_client)))
            .route("/intents/{intent_id}", web::get().to(fetch_intents))
    })
    .bind("127.0.0.1:8080")
    .unwrap()
    .run()
    .await;

    // Wait for Ctrl+C signal to exit
    signal::ctrl_c().await.unwrap();
    info!("Shutting down...");
}

#[derive(serde::Serialize)]
struct IntentResponse {
    intent_id: i64,
    transaction_hash: String,
    version: i32,
    stage: String,
    owner_address: String,
    block_number: i64,
    order_data: Option<Vec<OrderData>>,
}

#[derive(serde::Serialize)]
struct OrderData {
    order_id: i64,
    transaction_hash: String,
    token_in: String,
    token_out: String,
    amount_in: String,
    amount_out: String,
    source_chain_id: String,
    destination_chain_id: String
}

// Handler to fetch data from the database
async fn fetch_intents(
    client: web::Data<Arc<tokio_postgres::Client>>,
    intent_id: web::Path<i64>,
) -> impl Responder {
    let query_intents = "SELECT * FROM intent WHERE intent_id = $1";
    let query_intent_state =
        "SELECT * FROM intent_state WHERE intent_id = $1 ORDER BY version DESC LIMIT 1"; // get state by intent id
    let query_orders = "SELECT * FROM order_created WHERE intent_id = $1";
    let query_message_on_vaults = "SELECT * FROM received_message_on_vault WHERE intent_id = $1";

    match client.query_one(query_intents, &[intent_id.as_ref()]).await {
        Ok(rows) => {
            let row = rows.clone();
            let intent_id: i64 = row.get("intent_id");
            let intent_state = client
                .query(query_intent_state, &[&intent_id])
                .await
                .unwrap();
            let stage: String = intent_state[0].get("stage");

            let order_data = match client.query(query_orders, &[&intent_id]).await {
                Ok(orders) => {
                    let data: Vec<_> = orders
                        .iter()
                        .map(|order| {
                            let order_id: i64 = order.get("order_id");
                            let transaction_hash: String = order.get("transaction_hash");
                            let order_struct_data = OrderData {
                                order_id,
                                transaction_hash,
                                token_in: order.get("token_in"),
                                token_out: order.get("token_out"),
                                amount_in: order.get("amount_in"),
                                amount_out: order.get("amount_out"),
                                source_chain_id: order.get("source_chain_id"),
                                destination_chain_id: order.get("destination_chain_id")
                            };
                            order_struct_data
                        })
                        .collect();
                    Some(data)
                }
                Err(e) => None,
            };

            let data = IntentResponse {
                intent_id: row.get("intent_id"),
                transaction_hash: row.get("transaction_hash"),
                version: row.get("version"),
                stage: stage.to_string(),
                owner_address: row.get("owner_address"),
                block_number: row.get("block_number"),
                order_data: order_data,
            };

            HttpResponse::Ok().json(data) // Return JSON response
        }
        Err(_) => {
            // Handle the error and return a 500 Internal Server Error response
            HttpResponse::InternalServerError().finish()
        }
    }
}
