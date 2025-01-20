// src/main.rs
mod events;
mod indexers;
pub mod solidity_structs;

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
    // let api_server = HttpServer::new(move || {
    //     App::new()
    //         .app_data(web::Data::new(Arc::clone(&db_server_client))) // Pass the database client
    //         .route("/data", web::get().to(fetch_data)) // Define the route
    // })
    // .bind("127.0.0.1:8080")
    // .unwrap() // Run on localhost:8080
    // .run();
    // .await;

    // Wait for Ctrl+C signal to exit
    signal::ctrl_c().await.unwrap();
    info!("Shutting down...");
}

// Handler to fetch data from the database
async fn fetch_data(client: web::Data<Arc<tokio_postgres::Client>>) -> impl Responder {
    match client.query("SELECT * FROM your_table", &[]).await {
        Ok(rows) => {
            let data: Vec<_> = rows
                .iter()
                .map(|row| {
                    // Assuming your_table has columns id and name
                    let id: i32 = row.get(0);
                    let name: String = row.get(1);
                    (id, name)
                })
                .collect();

            HttpResponse::Ok().json(data) // Return JSON response
        }
        Err(_) => {
            // Handle the error and return a 500 Internal Server Error response
            HttpResponse::InternalServerError().finish()
        }
    }
}
