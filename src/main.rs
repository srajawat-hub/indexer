// src/main.rs
mod events;
mod indexers;
pub mod solidity_structs;

use actix_web::cookie::time::PrimitiveDateTime;
use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use dotenv::dotenv;
use futures_util::future;
use indexers::{BlockchainIndexer, EvmIndexer, SolanaIndexer};
use log::{debug, error, info, trace};
use serde::Deserialize;
use std::sync::Arc;
use std::time::SystemTime;
use std::{fmt::format, future::Future};
use std::{fs, time};
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
        let content = fs::read_to_string(file_path).expect("Failed to read configuration file");
        toml::from_str(&content).expect("Failed to parse configuration file")
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

    let connect_statement = std::env::var("DB_CONNECTION_STRING")
        .expect("DB_CONNECTION_STRING must be set")
        .parse::<String>()
        .unwrap();
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

    let indexers: Vec<Box<dyn BlockchainIndexer + Send + Sync>> = config
        .indexers
        .into_iter()
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
        .route(
            "/intents/history/{initiator_address}",
            web::get().to(fetch_transaction_history),
        )
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
    createdAt: SystemTime,
    status: String,
    isDeposit: Option<bool>, // we don't have this currently
    senderAddress: String,
    solverAddress: String,
    source: Option<OrderTransactionData>,
    destination: Option<OrderTransactionData>,
    initial_data: Option<InitialData>,
}

// both for source and destination txn data
#[derive(serde::Serialize, Debug)]
struct OrderTransactionData {
    chainId: String,
    chainName: Option<String>, // we don't have this
    tokenIn: String,
    tokenOut: String,
    txHash: String,
    explorerLink: Option<String>,
    ticket: Option<String>,
    amountIn: String,
    amountOut: String,
    status: Option<String>, // might be tricky
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
    // receiver_address: String,
    solver_address: String,
    ack_result: Option<bool>,
    ack_tx_status: String,
    ack_error_message: Option<String>,
    solver_tx_hash: Option<String>,
    ack_tx_hash: Option<String>,
    // order_payload: String,
    intent_version: i32, // check if more needed
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
    let query_message_on_vaults = "SELECT * FROM received_message_on_vault WHERE intent_id = $1"; // we will get 2 rows for 1 intent id, 1 for src_chain, next for dest_chain
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

            let intent_solution = client
                .query_one(query_solution, &[&intent_id])
                .await
                .unwrap();
            let solver_address: String = intent_solution.get("solver_address");

            let intent_orders = match (intent_version > 1) {
                true => {
                    let intent_order_rows = match client.query(query_orders, &[&intent_id]).await {
                        Ok(rows) => Some(rows),
                        Err(e) => None,
                    };
                    intent_order_rows
                }
                false => None,
            };

            let ack_row = match intent_version {
                5 => match client.query_one(query_ack, &[&intent_id]).await {
                    Ok(row) => Some(row),
                    Err(e) => None,
                },
                _ => None,
            };

            /*
               if intent_order length = 2 // i.e 2 orders
               and if tokenIn == bytes32(0); then the orders are of a cross chain swap

               if intent_order length = 1
               and if tokenIn != bytes32(0); then the token exists it could be a local intent, or stake
            */

            let mut source_transaction_data: Option<OrderTransactionData>;
            let mut destination_transaction_data: Option<OrderTransactionData>;
            let mut initial_data: Option<InitialData>;

            let bytes32_zero_address: String =
                String::from("0x0000000000000000000000000000000000000000000000000000000000000000");

            match intent_orders {
                Some(orders) => {
                    match orders.len() {
                        1 => {
                            let token_out: String = orders[0].get("token_out");
                            match token_out {
                                bytes32_zero_address => {
                                    destination_transaction_data = Some(OrderTransactionData {
                                        amountIn: orders[0].get("amount_in"),
                                        amountOut: orders[0].get("amount_out"),
                                        status: None,
                                        chainId: orders[0].get("source_chain_id"), // is the the IP chain id or vault?
                                        chainName: None,
                                        txHash: orders[0].get("transaction_hash"),
                                        tokenIn: orders[0].get("token_in"),
                                        tokenOut: orders[0].get("token_out"),
                                        ticket: None,
                                        explorerLink: None,
                                    });
                                    source_transaction_data = None;

                                    initial_data = Some(InitialData {
                                        id: intent_row.get("id"),
                                        intent_id: intent_id,
                                        origin_chain: None,
                                        target_chain: orders[0].get("source_chain_id"),
                                        token_in: None,
                                        token_out: orders[0].get("token_out"),
                                        amount_in: None,
                                        amount_out: orders[0].get("amount_out"),
                                        initiator_address: sender_address,
                                        // receiver_address:
                                        solver_address: solver_address,
                                        ack_result: match &ack_row {
                                            Some(ack) => {
                                                let ack_result: bool = ack.get("result");
                                                Some(ack_result)
                                            }
                                            None => Some(false),
                                        },
                                        ack_tx_status: match intent_version {
                                            5 => String::from("Success"),
                                            _ => String::from("Not started"),
                                        },
                                        ack_error_message: match &ack_row {
                                            Some(ack) => {
                                                let ack_error: String = ack.get("error_message");
                                                Some(ack_error)
                                            }
                                            None => None,
                                        },
                                        solver_tx_hash: intent_solution.get("transaction_hash"),
                                        ack_tx_hash: match ack_row {
                                            Some(ack) => ack.get("transaction_hash"),
                                            None => None,
                                        },
                                        intent_version: intent_version,
                                    });
                                }
                                _ => {
                                    destination_transaction_data = None;
                                    source_transaction_data = Some(OrderTransactionData {
                                        amountIn: orders[0].get("amount_in"),
                                        amountOut: orders[0].get("amount_out"),
                                        status: None,
                                        chainId: orders[0].get("source_chain_id"), // is the the IP chain id or vault?
                                        chainName: None,
                                        txHash: orders[0].get("transaction_hash"),
                                        tokenIn: orders[0].get("token_in"),
                                        tokenOut: orders[0].get("token_out"),
                                        ticket: None,
                                        explorerLink: None,
                                    });
                                    initial_data = Some(InitialData {
                                        id: intent_row.get("id"),
                                        intent_id: intent_id,
                                        origin_chain: orders[0].get("destination_chain_id"),
                                        target_chain: None,
                                        token_in: orders[0].get("token_in"),
                                        token_out: None,
                                        amount_in: orders[0].get("amount_in"),
                                        amount_out: None,
                                        initiator_address: sender_address,
                                        // receiver_address:
                                        solver_address: solver_address,
                                        ack_result: match &ack_row {
                                            Some(ack) => {
                                                let ack_result: bool = ack.get("result");
                                                Some(ack_result)
                                            }
                                            None => Some(false),
                                        },
                                        ack_tx_status: match intent_version {
                                            5 => String::from("Success"),
                                            _ => String::from("Not started"),
                                        },
                                        ack_error_message: match &ack_row {
                                            Some(ack) => {
                                                let ack_error: String = ack.get("error_message");
                                                Some(ack_error)
                                            }
                                            None => None,
                                        },
                                        solver_tx_hash: intent_solution.get("transaction_hash"),
                                        ack_tx_hash: match ack_row {
                                            Some(ack) => ack.get("transaction_hash"),
                                            None => None,
                                        },
                                        intent_version: intent_version,
                                    });
                                }
                            }
                        }
                        2 => {
                            source_transaction_data = Some(OrderTransactionData {
                                amountIn: orders[0].get("amount_in"),
                                amountOut: orders[0].get("amount_out"),
                                status: None,
                                chainId: orders[0].get("source_chain_id"), // is the the IP chain id or vault?
                                chainName: None,
                                txHash: orders[0].get("transaction_hash"),
                                tokenIn: orders[0].get("token_in"),
                                tokenOut: orders[0].get("token_out"),
                                ticket: None,
                                explorerLink: None,
                            });

                            destination_transaction_data = Some(OrderTransactionData {
                                amountIn: orders[1].get("amount_in"),
                                amountOut: orders[1].get("amount_out"),
                                status: None,
                                chainId: orders[1].get("source_chain_id"),
                                chainName: None,
                                txHash: orders[1].get("transaction_hash"),
                                tokenIn: orders[1].get("token_in"),
                                tokenOut: orders[1].get("token_out"),
                                ticket: None,
                                explorerLink: None,
                            });

                            initial_data = Some(InitialData {
                                id: intent_row.get("id"),
                                intent_id: intent_id,
                                origin_chain: Some(orders[0].get("destination_chain_id")),
                                target_chain: Some(orders[1].get("source_chain_id")),
                                token_in: Some(orders[0].get("token_in")),
                                token_out: Some(orders[1].get("token_out")),
                                amount_in: Some(orders[0].get("amount_in")),
                                amount_out: Some(orders[1].get("amount_out")),
                                initiator_address: sender_address,
                                // receiver_address:
                                solver_address: solver_address,
                                ack_result: match &ack_row {
                                    Some(ack) => {
                                        let ack_result: bool = ack.get("result");
                                        Some(ack_result)
                                    }
                                    None => Some(false),
                                },
                                ack_tx_status: match intent_version {
                                    5 => String::from("Success"),
                                    _ => String::from("Not started"),
                                },
                                ack_error_message: match &ack_row {
                                    Some(ack) => {
                                        let ack_error: String = ack.get("error_message");
                                        Some(ack_error)
                                    }
                                    None => None,
                                },
                                solver_tx_hash: intent_solution.get("transaction_hash"),
                                ack_tx_hash: match ack_row {
                                    Some(ack) => ack.get("transaction_hash"),
                                    None => None,
                                },
                                intent_version: intent_version,
                            });
                        }
                        _ => {
                            source_transaction_data = None;
                            destination_transaction_data = None;
                            initial_data = Some(InitialData {
                                id: intent_row.get("id"),
                                intent_id: intent_id,
                                origin_chain: None,
                                target_chain: None,
                                token_in: None,
                                token_out: None,
                                amount_in: None,
                                amount_out: None,
                                initiator_address: sender_address,
                                // receiver_address:
                                solver_address: solver_address,
                                ack_result: match &ack_row {
                                    Some(ack) => {
                                        let ack_result: bool = ack.get("result");
                                        Some(ack_result)
                                    }
                                    None => Some(false),
                                },
                                ack_tx_status: match intent_version {
                                    5 => String::from("Success"),
                                    _ => String::from("Not started"),
                                },
                                ack_error_message: match &ack_row {
                                    Some(ack) => {
                                        let ack_error: String = ack.get("error_message");
                                        Some(ack_error)
                                    }
                                    None => None,
                                },
                                solver_tx_hash: intent_solution.get("transaction_hash"),
                                ack_tx_hash: match ack_row {
                                    Some(ack) => ack.get("transaction_hash"),
                                    None => None,
                                },
                                intent_version: intent_version,
                            });
                        }
                    }
                }
                None => {
                    source_transaction_data = None;
                    destination_transaction_data = None;
                    initial_data = Some(InitialData {
                        id: intent_row.get("id"),
                        intent_id: intent_id,
                        origin_chain: None,
                        target_chain: None,
                        token_in: None,
                        token_out: None,
                        amount_in: None,
                        amount_out: None,
                        initiator_address: sender_address,
                        // receiver_address:
                        solver_address: solver_address,
                        ack_result: match &ack_row {
                            Some(ack) => {
                                let ack_result: bool = ack.get("result");
                                Some(ack_result)
                            }
                            None => Some(false),
                        },
                        ack_tx_status: match intent_version {
                            5 => String::from("Success"),
                            _ => String::from("Not started"),
                        },
                        ack_error_message: match &ack_row {
                            Some(ack) => {
                                let ack_error: String = ack.get("error_message");
                                Some(ack_error)
                            }
                            None => None,
                        },
                        solver_tx_hash: intent_solution.get("transaction_hash"),
                        ack_tx_hash: match ack_row {
                            Some(ack) => ack.get("transaction_hash"),
                            None => None,
                        },
                        intent_version: intent_version,
                    });
                }
            }

            let created_at: SystemTime = intent_row.get("timestamp");

            let transaction_data: TransactionData = TransactionData {
                id: intent_row.get("id"),
                intentId: intent_id,
                createdAt: created_at,
                status: stage,
                isDeposit: None,
                senderAddress: intent_row.get("owner_address"),
                solverAddress: intent_solution.get("solver_address"),
                source: source_transaction_data,
                destination: destination_transaction_data,
                initial_data: initial_data,
            };

            HttpResponse::Ok().json(transaction_data)
        }
        Err(_) => HttpResponse::InternalServerError().finish(),
    }
}

#[derive(serde::Serialize, Debug)]
struct TransactionHistory {
    intent_id: i64,
    status: String,
    version: i32,
}

async fn fetch_transaction_history(
    client: web::Data<Arc<tokio_postgres::Client>>,
    initiator_address: web::Path<String>,
) -> impl Responder {
    let query_intent = "SELECT * FROM intent WHERE owner_address = $1";
    let query_intent_state =
        "SELECT * FROM intent_state WHERE intent_id = $1 ORDER BY version DESC LIMIT 1"; // get state by intent id

    match client
        .query(query_intent, &[&initiator_address.to_string()])
        .await
    {
        Ok(intent_row) => {
            println!("row len {:?}", intent_row.len());
            let mut data: Vec<TransactionHistory> = vec![];
            for row in intent_row {
                let intent_id: i64 = row.get("intent_id");
                match client
                    .query_one(query_intent_state, &[&intent_id])
                    .await {
                        Ok(state) => {
                            let txn: TransactionHistory = TransactionHistory {
                                intent_id,
                                status: state.get("stage"),
                                version: state.get("version"),
                            };
                            data.push(txn);
                        },
                        Err(_) => continue
                    }
            }
            HttpResponse::Ok().json(data) // Return JSON response
        }
        Err(e) => HttpResponse::InternalServerError().finish(),
    }
}
