pub mod evm_indexer;
pub(crate) mod raydium_events;
pub mod solana_indexer;

use async_trait::async_trait;
use std::sync::Arc;
use tokio_postgres::Client;

#[async_trait]
pub trait BlockchainIndexer {
    async fn listen_for_events(
        &self,
        client: Arc<Client>,
    ) -> Result<(), Box<dyn std::error::Error>>;
}

pub use evm_indexer::EvmIndexer;
pub use solana_indexer::SolanaIndexer;
