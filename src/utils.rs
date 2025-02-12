/// Returns the block explorer link for a given chain ID and transaction hash.
pub fn get_block_explorer_link(chain_id: String, tx_hash: Option<String>) -> Option<String> {
    let chain_id_num: u32 = chain_id.parse().unwrap();
    match tx_hash {
        Some(hash) => {
            match chain_id_num {
                11155111 => Some(format!("https://sepolia.etherscan.io/tx/{}", hash)), // Ethereum sepolia
                421614 => Some(format!("https://sepolia.arbiscan.io/tx/{}", hash)), // Arbitrum sepolia Testnet
                11155420 => Some(format!("https://sepolia-optimism.etherscan.io/tx/{}", hash)), // Optimism sepolia Testnet
                80002 => Some(format!("https://amoy.polygonscan.com/tx/{}", hash)), // Polygon amoy Testnet
                84532 => Some(format!("https://sepolia.basescan.org/tx/{}", hash)), // Base sepolia testnet
                17000 => Some(format!("https://holesky.etherscan.io/tx/{}", hash)), // Ethereum holesky testnet
                4294967295 => Some(format!("https://solscan.io/tx/{}?cluster=devnet", hash)), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        }
        None => None,
    }
}

/// Returns the block explorer link for a given chain ID and transaction hash.
pub fn get_api_url(chain_id: String, action: String, contract_address: String) -> Option<String> {
    let chain_id_num: u32 = chain_id.parse().unwrap();
    match chain_id_num {
        11155111 => Some(format!("https://eth-sepolia.blockscout.com/api?module=account&action={}&address={}", action, contract_address)), // Ethereum sepolia
        421614 => Some(format!("https://arbitrum-sepolia.blockscout.com/api?module=account&action={}&address={}", action, contract_address)), // Arbitrum sepolia Testnet
        11155420 => Some(format!("https://optimism-sepolia.blockscout.com/api?module=account&action={}&address={}", action, contract_address)), // Optimism sepolia Testnet
        // 80002 => Some(format!("https://amoy.polygonscan.com/tx/")), // Polygon amoy Testnet
        84532 => Some(format!("https://base-sepolia.blockscout.com/api?module=account&action={}&address={}", action, contract_address)), // Base sepolia testnet
        17000 => Some(format!("https://eth-holesky.blockscout.com/api?module=account&action={}&address={}", action, contract_address)), // Ethereum holesky testnet
        // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
        _ => None, // Return None for unsupported chain IDs
    }
}
