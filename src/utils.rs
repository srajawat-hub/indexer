use actix_web::web;
use log::error;
use std::sync::Arc;
use tokio_postgres::Row;

use crate::{InitialData, OrderTransactionData};

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
                1 => Some(format!("https://etherscan.io/tx/{}", hash)),
                42161 => Some(format!("https://arbiscan.io/tx/{}", hash)),
                10 => Some(format!("https://optimistic.etherscan.io/tx/{}", hash)),
                137 => Some(format!("https://polygonscan.com/tx/{}", hash)),
                8453 => Some(format!("https://basescan.org/tx/{}", hash)),
                1399811149 => Some(format!("https://solscan.io/tx/{}", hash)), // Solana devnet
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
                11155111 => Some(format!(
                    "https://eth-sepolia.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )), // Ethereum sepolia
                421614 => Some(format!(
                    "https://arb-sepolia.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )),
                11155420 => Some(format!(
                    "https://opt-sepolia.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )), // Optimism sepolia Testnet
                80002 => Some(format!(
                    "https://polygon-amoy.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )), // Polygon amoy Testnet
                84532 => Some(format!(
                    "https://base-sepolia.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )), // Base sepolia testnet
                17000 => Some(format!(
                    "https://eth-holesky.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )), // Ethereum holesky testnet
                // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        }
        "mainnet" => {
            match chain_id_num {
                1 => Some(format!(
                    "https://eth-mainnet.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )), // Ethereum sepolia
                42161 => Some(format!(
                    "https://arb-mainnet.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )),
                10 => Some(format!(
                    "https://opt-mainnet.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )),
                137 => Some(format!(
                    "https://polygon-mainnet.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )),
                8453 => Some(format!(
                    "https://base-mainnet.g.alchemy.com/v2/{}",
                    alchemy_api_key
                )),
                // 4294967295 => Some(format!("https://solscan.io/tx/")), // Solana devnet
                _ => None, // Return None for unsupported chain IDs
            }
        }
        &_ => None,
    }
}

fn check_order_type(solution_type: Option<i32>, receiver_type: Option<i32>) -> Option<String> {
    match solution_type {
        Some(3) => Some(String::from("Stake")),
        Some(2) => match receiver_type {
            Some(0) => Some(String::from("Cross Chain Transfer")), // receiver is not vault
            Some(1) => Some(String::from("Cross Chain Swap")),     // receiver is vault
            _ => None,
        },
        Some(1) => match receiver_type {
            Some(0) => Some(String::from("Local Transfer")), // receiver is not vault
            Some(1) => Some(String::from("Local Swap")),     // receiver is vault
            _ => None,
        },
        Some(0) => match receiver_type {
            Some(0) => Some(String::from("Local Transfer")), // receiver is not vault
            Some(1) => Some(String::from("Local Swap")),     // receiver is vault
            _ => None,
        },
        _ => Some(String::new()),
    }
}

pub async fn structure_intent_orders(
    client: &web::Data<Arc<tokio_postgres::Client>>,
    intent_row: &Row,
    intent_orders: Option<Vec<Row>>,
    ack_row: Option<Row>,
    solver_address: Option<String>,
    solver_transaction_hash: Option<String>,
    intent_version: i32,
    sender_address: &String,
) -> (
    Option<OrderTransactionData>,
    Option<OrderTransactionData>,
    Option<InitialData>,
    Option<String>,
) {
    let query_message_on_vaults =
        "SELECT transaction_hash FROM received_message_on_vault WHERE order_id = $1"; // we will get 2 rows for 1 intent id, 1 for src_chain, next for dest_chain
    let intent_id: i64 = intent_row.get("intent_id");
    let feeAmount: Option<String> = intent_row.get("feeAmount");

    let mut source_transaction_data: Option<OrderTransactionData>;
    let destination_transaction_data: Option<OrderTransactionData>;
    let initial_data: Option<InitialData>;
    let mut intent_type: Option<String> = Some(String::new());

    let bytes32_zero_address: String =
        String::from("0x0000000000000000000000000000000000000000000000000000000000000000");

    let fulfill_transaction_state_query =
        "SELECT transaction_hash FROM intent_state WHERE intent_id = $1 AND version = 4";
    let fulfill_transaction_hash: Option<String> = match client
        .query_one(fulfill_transaction_state_query, &[&intent_id])
        .await
    {
        Ok(row) => {
            let hash: String = row.get("transaction_hash");
            Some(hash)
        }
        Err(_) => None,
    };

    match intent_orders {
        Some(orders) => match orders.len() {
            1 => {
                let token_out: String = orders[0].get("token_out");
                let chain_id: String = orders[0].get("source_chain_id");
                let order_id: i64 = orders[0].get("order_id");
                let solution_type: Option<i32> = orders[0].get("solution_type");
                let multi_leg: bool = orders[0].get("multi_leg");
                let receiver_type: Option<i32> = orders[0].get("receiver_type");
                let receiver_address: Option<String> = orders[0].get("receiver_address");
                intent_type = check_order_type(solution_type, receiver_type);

                let vault_txn_hash: Option<String> = match client
                    .query_one(query_message_on_vaults, &[&order_id])
                    .await
                {
                    Ok(vault_order) => {
                        let txn_hash: String = vault_order.get("transaction_hash");
                        Some(txn_hash)
                    }
                    Err(e) => {
                        error!("error {:?}", e);
                        None
                    }
                };

                match multi_leg {
                    true => match token_out == bytes32_zero_address {
                        true => {
                            destination_transaction_data = Some(OrderTransactionData {
                                amountIn: orders[0].get("amount_in"),
                                amountOut: orders[0].get("amount_out"),
                                chainId: chain_id.clone(),
                                txHash: vault_txn_hash.clone(),
                                tokenIn: orders[0].get("token_in"),
                                tokenOut: orders[0].get("token_out"),
                                explorerLink: get_block_explorer_link(chain_id, vault_txn_hash),
                                order_payload: Some(orders[0].get("order_payload")),
                                orderId: Some(orders[0].get("order_id")),
                            });
                            source_transaction_data = None;

                            initial_data = Some(InitialData {
                                id: intent_row.get("id"),
                                intent_id: intent_id,
                                origin_chain: None,
                                target_chain: orders[0].get("source_chain_id"),
                                token_in: orders[0].get("token_in"),
                                token_out: orders[0].get("token_out"),
                                amount_in: orders[0].get("amount_in"),
                                amount_out: orders[0].get("amount_out"),
                                initiator_address: sender_address.clone(),
                                solver_address: solver_address.clone(),
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
                                solver_tx_hash: solver_transaction_hash,
                                ack_tx_hash: match ack_row {
                                    Some(ack) => ack.get("transaction_hash"),
                                    None => None,
                                },
                                fulfill_tx_hash: fulfill_transaction_hash,
                                intent_version: intent_version,
                                receiver_address,
                                feeAmount
                            });
                        }
                        false => {
                            destination_transaction_data = None;
                            source_transaction_data = Some(OrderTransactionData {
                                amountIn: orders[0].get("amount_in"),
                                amountOut: orders[0].get("amount_out"),
                                chainId: chain_id.clone(),
                                txHash: vault_txn_hash.clone(),
                                tokenIn: orders[0].get("token_in"),
                                tokenOut: orders[0].get("token_out"),
                                explorerLink: get_block_explorer_link(chain_id, vault_txn_hash),
                                order_payload: Some(orders[0].get("order_payload")),
                                orderId: Some(orders[0].get("order_id")),
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
                                initiator_address: sender_address.clone(),
                                solver_address: solver_address.clone(),
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
                                solver_tx_hash: solver_transaction_hash,
                                ack_tx_hash: match ack_row {
                                    Some(ack) => ack.get("transaction_hash"),
                                    None => None,
                                },
                                fulfill_tx_hash: fulfill_transaction_hash,
                                intent_version: intent_version,
                                receiver_address,
                                feeAmount
                            });
                        }
                    },
                    false => {
                        source_transaction_data = None;
                        if solution_type.unwrap() > 1 {
                            source_transaction_data = Some(OrderTransactionData {
                                amountIn: orders[0].get("amount_in"),
                                amountOut: bytes32_zero_address,
                                chainId: orders[0].get("source_chain_id"),
                                txHash: vault_txn_hash.clone(), // check
                                tokenIn: orders[0].get("token_in"),
                                tokenOut: orders[0].get("token_out"),
                                explorerLink: get_block_explorer_link(
                                    chain_id.clone(),
                                    vault_txn_hash.clone(),
                                ),
                                order_payload: Some(orders[0].get("order_payload")),
                                orderId: Some(orders[0].get("order_id")),
                            });
                        }
                        destination_transaction_data = Some(OrderTransactionData {
                            amountIn: orders[0].get("amount_in"),
                            amountOut: orders[0].get("amount_out"),
                            chainId: orders[0].get("destination_chain_id"),
                            txHash: match solution_type.unwrap() > 1 {
                                true => fulfill_transaction_hash.clone(),
                                false => vault_txn_hash.clone(),
                            },
                            tokenIn: orders[0].get("token_in"),
                            tokenOut: orders[0].get("token_out"),
                            explorerLink: match solution_type.unwrap() > 1 {
                                true => get_block_explorer_link(
                                    orders[0].get("destination_chain_id"),
                                    fulfill_transaction_hash.clone(),
                                ),
                                false => get_block_explorer_link(
                                    orders[0].get("destination_chain_id"),
                                    vault_txn_hash.clone(),
                                ),
                            },
                            order_payload: Some(orders[0].get("order_payload")),
                            orderId: Some(orders[0].get("order_id")),
                        });
                        initial_data = Some(InitialData {
                            id: intent_row.get("id"),
                            intent_id: intent_id,
                            origin_chain: orders[0].get("source_chain_id"),
                            target_chain: orders[0].get("destination_chain_id"),
                            token_in: orders[0].get("token_in"),
                            token_out: orders[0].get("token_out"),
                            amount_in: orders[0].get("amount_in"),
                            amount_out: orders[0].get("amount_out"),
                            initiator_address: sender_address.clone(),
                            solver_address: solver_address.clone(),
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
                            solver_tx_hash: solver_transaction_hash,
                            ack_tx_hash: match ack_row {
                                Some(ack) => ack.get("transaction_hash"),
                                None => None,
                            },
                            fulfill_tx_hash: fulfill_transaction_hash,
                            intent_version: intent_version,
                            receiver_address,
                            feeAmount
                        });
                    }
                }
            }
            2 => {
                let source_order;
                let destination_order;

                let order_id1: i64 = orders[0].get("order_id");
                let order_id2: i64 = orders[1].get("order_id");

                if order_id1 < order_id2 {
                    source_order = orders[0].clone();
                    destination_order = orders[1].clone();
                } else {
                    source_order = orders[1].clone();
                    destination_order = orders[0].clone();
                }

                let src_chain_id: String = source_order.get("source_chain_id");
                let src_order_id: i64 = source_order.get("order_id");
                let solution_type: Option<i32> = source_order.get("solution_type");
                let src_vault_txn_hash: Option<String> = match client
                    .query_one(query_message_on_vaults, &[&src_order_id])
                    .await
                {
                    Ok(vault_order) => {
                        let txn_hash: String = vault_order.get("transaction_hash");
                        Some(txn_hash)
                    }
                    Err(_) => None,
                };

                let dst_chain_id: String = destination_order.get("source_chain_id");
                let dst_order_id: i64 = destination_order.get("order_id");
                let dst_vault_txn_hash: Option<String> = match client
                    .query_one(query_message_on_vaults, &[&dst_order_id])
                    .await
                {
                    Ok(vault_order) => {
                        let txn_hash: String = vault_order.get("transaction_hash");
                        Some(txn_hash)
                    }
                    Err(_) => None,
                };

                let receiver_type: Option<i32> = source_order.get("receiver_type");
                let receiver_address: Option<String> = source_order.get("receiver_address");
                intent_type = check_order_type(solution_type, receiver_type);

                source_transaction_data = Some(OrderTransactionData {
                    amountIn: source_order.get("amount_in"),
                    amountOut: source_order.get("amount_out"),
                    chainId: src_chain_id.clone(),
                    txHash: src_vault_txn_hash.clone(),
                    tokenIn: source_order.get("token_in"),
                    tokenOut: source_order.get("token_out"),
                    explorerLink: get_block_explorer_link(src_chain_id, src_vault_txn_hash),
                    order_payload: Some(source_order.get("order_payload")),
                    orderId: Some(source_order.get("order_id")),
                });

                destination_transaction_data = Some(OrderTransactionData {
                    amountIn: destination_order.get("amount_in"),
                    amountOut: destination_order.get("amount_out"),
                    chainId: dst_chain_id.clone(),
                    txHash: dst_vault_txn_hash.clone(),
                    tokenIn: destination_order.get("token_in"),
                    tokenOut: destination_order.get("token_out"),
                    explorerLink: get_block_explorer_link(dst_chain_id, dst_vault_txn_hash),
                    order_payload: Some(destination_order.get("order_payload")),
                    orderId: Some(destination_order.get("order_id")),
                });

                initial_data = Some(InitialData {
                    id: intent_row.get("id"),
                    intent_id: intent_id,
                    origin_chain: Some(source_order.get("destination_chain_id")),
                    target_chain: Some(destination_order.get("source_chain_id")),
                    token_in: Some(source_order.get("token_in")),
                    token_out: Some(destination_order.get("token_out")),
                    amount_in: Some(source_order.get("amount_in")),
                    amount_out: Some(destination_order.get("amount_out")),
                    initiator_address: sender_address.clone(),
                    solver_address: solver_address.clone(),
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
                    solver_tx_hash: solver_transaction_hash,
                    ack_tx_hash: match ack_row {
                        Some(ack) => ack.get("transaction_hash"),
                        None => None,
                    },
                    fulfill_tx_hash: fulfill_transaction_hash,
                    intent_version: intent_version,
                    receiver_address,
                    feeAmount
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
                    initiator_address: sender_address.clone(),
                    solver_address: solver_address.clone(),
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
                    solver_tx_hash: solver_transaction_hash,
                    ack_tx_hash: match ack_row {
                        Some(ack) => ack.get("transaction_hash"),
                        None => None,
                    },
                    fulfill_tx_hash: fulfill_transaction_hash,
                    intent_version: intent_version,
                    receiver_address: None,
                    feeAmount: None
                });
            }
        },
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
                initiator_address: sender_address.clone(),
                solver_address: solver_address.clone(),
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
                solver_tx_hash: solver_transaction_hash,
                ack_tx_hash: match ack_row {
                    Some(ack) => ack.get("transaction_hash"),
                    None => None,
                },
                fulfill_tx_hash: fulfill_transaction_hash,
                intent_version: intent_version,
                receiver_address: None,
                feeAmount: None
            });
        }
    }

    (
        source_transaction_data,
        destination_transaction_data,
        initial_data,
        intent_type,
    )
}
