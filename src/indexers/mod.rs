pub mod evm_indexer;
pub mod solana_indexer;

use async_trait::async_trait;

#[async_trait]
pub trait BlockchainIndexer {
    async fn listen_for_events(&self) -> Result<(), Box<dyn std::error::Error>>;
}

pub use evm_indexer::EvmIndexer;
pub use solana_indexer::SolanaIndexer;