/// Returns the block explorer link for a given chain ID and transaction hash.
pub fn get_block_explorer_link(chain_id: String, tx_hash: Option<String>) -> Option<String> {
    let chain_id_num: u32 = chain_id.parse().unwrap();
    match tx_hash {
        Some(hash) => {
            match chain_id_num {
                11155111 => Some(format!("https://sepolia.etherscan.io/tx/{}", hash)), // Ethereum sepolia
                421614 => Some(format!("https://sepolia.arbiscan.io/tx/{}", hash)),
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
pub fn get_api_url(network: String, chain_id: String, alchemy_api_key: String) -> Option<String> {
    let chain_id_num: u32 = chain_id.parse().unwrap();

    match network.as_ref() {
        "testnet" => {
            match chain_id_num {
                11155111 => Some(format!("https://eth-sepolia.g.alchemy.com/v2/{}", alchemy_api_key)), // Ethereum sepolia
                421614 => Some(format!("https://arb-sepolia.g.alchemy.com/v2/{}", alchemy_api_key)),
                11155420 => Some(format!("https://opt-sepolia.g.alchemy.com/v2/{}", alchemy_api_key)), // Optimism sepolia Testnet
                80002 => Some(format!("https://polygon-amoy.g.alchemy.com/v2/{}", alchemy_api_key)), // Polygon amoy Testnet
                84532 => Some(format!("https://base-sepolia.g.alchemy.com/v2/{}", alchemy_api_key)), // Base sepolia testnet
                17000 => Some(format!("https://eth-holesky.g.alchemy.com/v2/{}", alchemy_api_key)), // Ethereum holesky testnet
                // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        },
        "mainnet" => {
            match chain_id_num {
                1 => Some(format!("https://eth-mainnet.g.alchemy.com/v2/{}", alchemy_api_key)), // Ethereum sepolia
                42161 => Some(format!("https://arb-mainnet.g.alchemy.com/v2/{}", alchemy_api_key)),
                10 => Some(format!("https://opt-mainnet.g.alchemy.com/v2/{}", alchemy_api_key)),
                137 => Some(format!("https://polygon-mainnet.g.alchemy.com/v2/{}", alchemy_api_key)),
                8453 => Some(format!("https://base-mainnet.g.alchemy.com/v2/{}", alchemy_api_key)),
                // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        },
        &_ => None
    }

}
