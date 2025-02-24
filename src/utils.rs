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
pub fn get_api_url(network: String, chain_id: String) -> Option<String> {
    let chain_id_num: u32 = chain_id.parse().unwrap();
    match network.as_ref() {
        "testnet" => {
            match chain_id_num {
                11155111 => Some(String::from("https://eth-sepolia.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")), // Ethereum sepolia
                421614 => Some(String::from("https://arb-sepolia.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")),
                11155420 => Some(String::from("https://opt-sepolia.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")), // Optimism sepolia Testnet
                80002 => Some(String::from("https://polygon-amoy.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")), // Polygon amoy Testnet
                84532 => Some(String::from("https://base-sepolia.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")), // Base sepolia testnet
                17000 => Some(String::from("https://eth-holesky.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")), // Ethereum holesky testnet
                // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        },
        "mainnet" => {
            match chain_id_num {
                1 => Some(String::from("https://eth-mainnet.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")), // Ethereum sepolia
                42161 => Some(String::from("https://arb-mainnet.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")),
                10 => Some(String::from("https://opt-mainnet.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")),
                137 => Some(String::from("https://polygon-mainnet.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")),
                8453 => Some(String::from("https://base-mainnet.g.alchemy.com/v2/7b4v9HWZgi_BlLtoxK25IdxuD5jeQkX5")),
                // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        },
        &_ => None
    }

}
