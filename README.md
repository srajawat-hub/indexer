```
intent (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    owner_address VARCHAR(66) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    feeamount TEXT
);
solution (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    solver_address VARCHAR(44) NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL
);
acknowledgement (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    sender_address VARCHAR(44) NOT NULL,
    result BOOLEAN NOT NULL,
    error_message TEXT,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_id BIGINT NOT NULL
    metadata TEXT
);
received_message_on_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    origin_domain_id INTEGER NOT NULL,
    sender_address VARCHAR(44) NOT NULL,
    message TEXT NOT NULL,
    provider INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    chain_id BIGINT NOT NULL,
    order_id BIGINT NOT NULL,
    dln_order_id VARCHAR(255),
    timeout_unix_timestamp_in_sec BIGINT
);
order_created (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    creator_address VARCHAR(66) NOT NULL,
    token_in VARCHAR(66) NOT NULL,
    token_out VARCHAR(66) NOT NULL,
    amount_in TEXT NOT NULL,
    amount_out TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_id BIGINT NOT NULL,
    source_chain_id TEXT NOT NULL,
    destination_chain_id TEXT NOT NULL,
    multi_leg BOOL NOT NULL DEFAULT false,
    order_payload TEXT NOT NULL,
    solution_type INTEGER,
    receiver_type INTEGER,
    receiver_address TEXT,
    
);
message_dispatched_from_vault (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    sender_address VARCHAR(66) NOT NULL,
    destination_domain_id INTEGER NOT NULL,
    provider INTEGER NOT NULL,
    message TEXT NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    block_number BIGINT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    order_id BIGINT NOT NULL,
);
intent_state (
    id BIGSERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL,
    version INTEGER NOT NULL,
    transaction_hash VARCHAR(88) NOT NULL,
    stage TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    gas_fees BIGINT,
    gas_token TEXT,
    order_id BIGINT,
    chain_id BIGINT NOT NULL,
    initiator_address VARCHAR(66) NOT NULL,
    transaction_cost TEXT
    transaction_cost_usd TEXT
);
deposit_received (
    id BIGSERIAL PRIMARY KEY,
    user_address TEXT NOT NULL,
    token_address TEXT NOT NULL,
    chain_id TEXT NOT NULL,
    amount TEXT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    source_transaction_hash TEXT NOT NULL,
    message_id TEXT UNIQUE DEFAULT NULL,
    status INTEGER
)
sanction_address_list (
    id BIGSERIAL PRIMARY KEY,
    address TEXT NOT NULL
)
intent_fees (
    id SERIAL PRIMARY KEY,
    intent_id BIGINT NOT NULL UNIQUE,
    fees JSONB NOT NULL
)
```

# APIs

**Base url Devnet - http://192.241.245.190:18891**
**Base url Mainnet - http://143.244.173.82:18891**

## All transactions

`/transactions`

#### Query params
- id - id for the last reponse received
- per_page - amount of transactions to receive on a single page

#### Response
```
[
    {
        "id": 20,
        "intent_id": 73,
        "status": "Done",
        "version": 5,
        "timestamp": "2025-01-20T06:57:48.364874Z"
    },
    {
        "id": 21,
        "intent_id": 74,
        "status": "Done",
        "version": 5,
        "timestamp": "2025-01-20T06:57:48.364874Z"
    },
    {
        "id": 22,
        "intent_id": 88,
        "status": "Done",
        "version": 5,
        "timestamp": "2025-01-20T06:57:48.364874Z"
    } ...
]
```

## History transactions for initiator

`/intents/history/{initiator_address}`

#### Query params
- id - id for the last reponse received
- per_page - amount of transactions to receive on a single page, Default 10
- tokens - a comma separated list of tokens for the history to filter from. The intents will be filters by the `token_in` is present in the comma separated tokens list.

#### Response
```
[
    {
        "id": 272,
        "intentId": 317,
        "createdAt": "2025-02-19T17:27:17.555571Z",
        "status": "Done",
        "isDeposit": null,
        "senderAddress": "0xaed223306A006975c00A939dBEB6d7eBd9C04d80",
        "solverAddress": "0xa3108fDb46992dC24eA2c3CbD19D38de59B17851",
        "source": {
            "chainId": "421614",
            "tokenIn": "0x000000000000000000000000b0268ffb7bc90eef1f3e0aa2133a1d5a5a73ee89",
            "tokenOut": "0x000000000000000000000000750b8c791080d89e2e9d0620c4cb4982caef9217",
            "txHash": "0x2afe9030da06ffa394cc0e22582f04f0ffc878d161ea6c988c879c4d2b2ba3fa",
            "explorerLink": "https://sepolia.arbiscan.io/tx/0x2afe9030da06ffa394cc0e22582f04f0ffc878d161ea6c988c879c4d2b2ba3fa",
            "amountIn": "4950000000000000000",
            "amountOut": "4950000000000000000",
            "order_payload": "0x000000000000000000..."
        },
        "destination": {
            "chainId": "84532",
            "tokenIn": "0x0000000000000000000000000000000000000000000000000000000000000000",
            "tokenOut": "0x000000000000000000000000c8893cbc1ac0b977d5092a5d85dafbcb7c960514",
            "txHash": "0x6fc402219aaab7692030d69ba7f81561c8f6a9336e37d2df311e94dc6791bd09",
            "explorerLink": "https://sepolia.basescan.org/tx/0x6fc402219aaab7692030d69ba7f81561c8f6a9336e37d2df311e94dc6791bd09",
            "amountIn": "4950000000000000000",
            "amountOut": "10000000000000000000",
            "order_payload": "0x00000000000000000..."
        },
        "initial_data": {
            "id": 272,
            "intent_id": 317,
            "origin_chain": "421614",
            "target_chain": "84532",
            "token_in": "0x000000000000000000000000b0268ffb7bc90eef1f3e0aa2133a1d5a5a73ee89",
            "amount_in": "4950000000000000000",
            "token_out": "0x000000000000000000000000c8893cbc1ac0b977d5092a5d85dafbcb7c960514",
            "amount_out": "10000000000000000000",
            "initiator_address": "0xaed223306A006975c00A939dBEB6d7eBd9C04d80",
            "solver_address": "0xa3108fDb46992dC24eA2c3CbD19D38de59B17851",
            "ack_result": true,
            "ack_tx_status": "Success",
            "ack_error_message": "",
            "solver_tx_hash": "0x8b44cfd30acacabfcc8320b4816a83eb75ab821c8c8cee678ed690069e0dcaf8",
            "ack_tx_hash": "0xbae0b2e153d7098e7c5e76e87a093e265f86ff06842633fbf11214384a77d048",
            "intent_version": 5
        },
        "intent_type": "Cross Chain Swap"
    },
    {...},
    {...}
]
```
## Intent with intent_id

`/intents/{intent_id}`

#### Response
```
{
    "id": 242,
    "intentId": 327,
    "createdAt": "2025-02-03T12:27:09.829708Z",
    "status": "Processing",
    "isDeposit": null,
    "senderAddress": "0x33F9C0d337e576533039d02f9CE10179311E67D0",
    "solverAddress": "0x99faB332Fe42b8783360d825795F699c686FC12c",
    "source": {
        "chainId": "421614",
        "tokenIn": "0x000000000000000000000000750b8c791080d89e2e9d0620c4cb4982caef9217",
        "tokenOut": "0x0000000000000000000000007f0b855163035cdbc04a128cbfbb426a07f9183f",
        "txHash": "0xe058a0d9dbe4e109211812fcd103f423a6c83c80967db7561e64fd8ded6e78a0",
        "explorerLink": "https://sepolia.arbiscan.io/tx/0xe058a0d9dbe4e109211812fcd103f423a6c83c80967db7561e64fd8ded6e78a0",
        "amountIn": "19800000000000000000",
        "amountOut": "39600000000000000000",
        "order_payload": "0x00000000000000000..."
    },
    "destination": {
        "chainId": "4294967295",
        "tokenIn": "0x0000000000000000000000000000000000000000000000000000000000000000",
        "tokenOut": "0x841b27e917d53b1733a74a12586783164af5ba5c0a1b59586f7e7dc2d95ba4c7",
        "txHash": "5J3DjDujko8xYLjENiuCennUxik8Pg76dtehFSm5XJNYM4rsUFPtAcZjmqGBvvVjJcooAYxLh1xpKuqDYYsRD6oA",
        "explorerLink": "https://solscan.io/tx/5J3DjDujko8xYLjENiuCennUxik8Pg76dtehFSm5XJNYM4rsUFPtAcZjmqGBvvVjJcooAYxLh1xpKuqDYYsRD6oA?cluster=devnet",
        "amountIn": "0",
        "amountOut": "6095735210",
        "order_payload": "0x00000000000000..."
    },
    "initial_data": {
        "id": 242,
        "intent_id": 327,
        "origin_chain": "421614",
        "target_chain": "4294967295",
        "token_in": "0x000000000000000000000000750b8c791080d89e2e9d0620c4cb4982caef9217",
        "amount_in": "19800000000000000000",
        "token_out": "0x841b27e917d53b1733a74a12586783164af5ba5c0a1b59586f7e7dc2d95ba4c7",
        "amount_out": "6095735210",
        "initiator_address": "0x33F9C0d337e576533039d02f9CE10179311E67D0",
        "solver_address": "0x99faB332Fe42b8783360d825795F699c686FC12c",
        "ack_result": false,
        "ack_tx_status": "Not started",
        "ack_error_message": null,
        "solver_tx_hash": "0xe639c3996a844e9760bfcde1e83ae9152f7a7dce9c28f3746008ce0e96092278",
        "ack_tx_hash": null,
        "intent_version": 3
    },
    "intent_type": "Cross Chain Swap"
}
```

### Intent Type field
In case of a `Cross Chain Transact`, the intent reponse should contain both Source and destination transactions as it involves 2 orders

In case of a `Local Transact` or `Stake` only the Destination transaction will be returned as it involves a single order

Possible values of `intent_type`
- Cross Chain Swap
- Cross Chain Transfer
- Local Transfer
- Local Swap
- Stake
- null // could not be determined

## Fetch token balance
Request Type - POST

endpoint - /contract_balance/{network}

**Query Params**
network - testnet or mainnet

**Input Payload**
- For EVM
```
{
    "type": "EVM",
    "token_address": [
        {
            "chain_id": "421614",
            "contract_address": "0x123141"
        },
        {
            "chain_id": "11155420",
            "contract_address": "0x12142"
        }
    ]
}
```

- For Solana
```
{
    "type": "SVM",
    "user_address": "0xaed223306A006975c00A939dBEB6d7eBd9C04d80",
    "token_address": [
        "2Y9NGj5JGhuMFcSYQM71eK1eFNVx6j3GpnjvHEGXopzU",
        "2Y9NGj5JGhuMFcSYQM71eK1eFNVx6j3GpnjvHEGXopzU",
        "5M8FC3ViURaWPLT8xEEW2EbW7jUfBJULEmoemqfcdKpw"
    ]
}
```

**Response**
```
{
    "data": {
        "11155420": [
            {
                "balance": "989995000000000000000000000",
                "contractAddress": "0x61d1350f74be0bcd1bdee84499c71ac47666a031",
                "decimals": "18",
                "name": "USDT",
                "symbol": "USDT"
            },
        ],
        "421614": [
            {
                "balance": "1000000000000000000000000000",
                "contractAddress": "0x24580a645e1b88d2261498d965562cfb7b68d646",
                "decimals": "18",
                "name": "USDC",
                "symbol": "USD Coin"
            },
        ]
    }
}
```