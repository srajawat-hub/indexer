// src/main.rs
mod events;
mod indexers;
pub mod solidity_structs;

use futures_util::future;
use std::future::Future;
use std::sync::Arc;

use actix_web::{web, App, HttpResponse, HttpServer, Responder};
use indexers::{BlockchainIndexer, EvmIndexer, SolanaIndexer};
use log::{debug, error, info, trace};
use tokio::signal;
use tokio_postgres::{connect, NoTls};

fn create_evm_indexer(url: &str, address: &str) -> Box<dyn BlockchainIndexer + Send + Sync> {
    Box::new(EvmIndexer::new(url.to_string(), address.to_string()))
}

#[tokio::main]
async fn main() {
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
    let (client, connection) = tokio_postgres::connect(
        "host=localhost user=postgres password=postgres dbname=mydb",
        NoTls,
    )
    .await
    .unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            error!("DB Connection error {:?}", e);
        }
    });

    let db_client = Arc::new(client);
    let db_server_client = Arc::clone(&db_client);

    let indexers: Vec<Box<dyn BlockchainIndexer + Send + Sync>> = vec![
        create_evm_indexer(
            "ws://192.241.245.190:18749",
            "0xD50EFb5eA641E7BcaCB1600b464cC1f45ea91588",
        ),
        create_evm_indexer(
            "wss://arb-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID",
            "0x8e3658985a9fE3d038925952101382792c57B70F",
        ),
        create_evm_indexer(
            "wss://arb-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID",
            "0x09D3b27BC29ada4D9F17d797BC7010672C51f37D",
        ),
        create_evm_indexer(
            "wss://opt-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID",
            "0xd6A3eB183eDCe33c44426Ab8cF94F1612b4C2102",
        ),
        create_evm_indexer(
            "wss://opt-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID",
            "0x5D4337dE92AFE95Bf2218c86255ea87855561d6E",
        ),
    ];

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
            .app_data(web::Data::new(Arc::clone(&db_server_client))) // Pass the database client
            .route("/intents/{intent_id}", web::get().to(fetch_intents)) // Define the route
    })
    .bind("127.0.0.1:8080")
    .unwrap() // Run on localhost:8080
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
    amount_out: String
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
                                amount_out: order.get("amount_out")
                            };
                            order_struct_data
                            // (id, intent_id, transaction_hash)
                        })
                        .collect();
                    Some(data)
                }
                Err(e) => None,
            };

            let data = IntentResponse {
                intent_id: row.get("intent_id"),
                transaction_hash: row.get("transaction_hash"),
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
