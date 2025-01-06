// src/main.rs
mod indexers;
mod events;
pub mod solidity_structs;

use tokio::signal;
use indexers::{BlockchainIndexer, EvmIndexer, SolanaIndexer};
use tokio_postgres::{NoTls, connect, };

#[tokio::main]
async fn main() {

    env_logger::builder().format(|buf, record| {
        use std::io::Write;
        writeln!(
            buf,
            "[{} - Thread: {:?}] {}",
            chrono::Local::now().format("%Y-%m-%d %H:%M:%S"),
            std::thread::current().id(),
            record.args()
        )
    }).init();

    // make a db connection
    let (client, connection) = tokio_postgres::connect("host=localhost user=postgres password=postgres dbname=mydb", NoTls).await.unwrap();
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("DB Connection error {:?}", e);
        }
    });

    let indexers: Vec<Box<dyn BlockchainIndexer + Send +Sync>> = vec![
        Box::new(EvmIndexer::new(
            "ws://192.241.245.190:18749".to_string(),
            "0xFAB814c2A68F54971A12Cf6990Ea3Df2EF14c3FB".to_string(),
        )),
        Box::new(EvmIndexer::new(
            "wss://arb-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID".to_string(),
            "0x22c423540918032B206Df38d86AFCB9B22eF1c0f".to_string(),
        )),
        Box::new(EvmIndexer::new(
            "wss://opt-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID".to_string(),
            "0x42Ad426D1C9dA42648535DEE83D9fc73bAd9f274".to_string(),
        )),
        Box::new(EvmIndexer::new(
            "wss://arb-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID".to_string(),
            "0x49E8FcC52698e78786ea1d929e1b3f1A7945Bccb".to_string(),
        )),
        Box::new(EvmIndexer::new(
            "wss://opt-sepolia.g.alchemy.com/v2/IiJTnNrz1Bp1PTE2vZf8T-ZWAXZ39pID".to_string(),
            "0xB5F67202064848c1528AbdC9e9e49a776E08ecC3".to_string(),
        ))
    ];

    let mut handles = vec![];
    for indexer in indexers {
        let handle = tokio::spawn(async move {
            if let Err(err) = indexer.listen_for_events().await {
                eprintln!("Error listening to events: {}", err);
            }
        });

        handles.push(handle);
    }

    println!("All tasks started. Press Ctrl+C to exit.");
    println!("total threads {:?}", handles.len());

    // Wait for Ctrl+C signal to exit
    signal::ctrl_c().await.unwrap();
    println!("Shutting down...");
}