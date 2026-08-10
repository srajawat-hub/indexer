# Multi-Chain Event Indexer

**Production-grade Rust indexer for cross-chain blockchain events, order tracking, and financial settlement.**

A real-time, multi-chain event indexer that aggregates blockchain data from EVM (Ethereum, Arbitrum, Base, Polygon, Optimism) and Solana networks. Processes cross-chain intents, swap orders, and vault messaging with PostgreSQL persistence and REST API access.

---

## 🎯 What It Does

This indexer powers **cross-chain order infrastructure** by:

1. **Listening to blockchain events** (EVM + Solana) in real-time
2. **Decoding & parsing** contract events (swaps, transfers, liquidations, settlements)
3. **Aggregating order lifecycle** (creation → solution → acknowledgement → settlement)
4. **Tracking cross-chain messages** (Wormhole, Hyperlane, custom bridges)
5. **Computing financial metrics** (USD values, gas costs, solver fees)
6. **Persisting to PostgreSQL** (36+ tables for relational queries)
7. **Serving via REST API** (query orders, balances, transaction history)

---

## 🏗️ Architecture

### Async Runtime & Concurrency
```rust
- Tokio multi-threaded async runtime
- Concurrent event listeners for multiple chains
- Parallel block processing without blocking
- Connection pooling to PostgreSQL via tokio-postgres
```

### Multi-Chain Support
```
EVM Chains:
  ├── Ethereum Mainnet
  ├── Arbitrum One / Sepolia
  ├── Optimism / Optimism Sepolia
  ├── Base / Base Sepolia
  └── Polygon / Mumbai

Solana:
  ├── Mainnet Beta
  └── Devnet

Cross-Chain Messaging:
  ├── Wormhole (VAA processing)
  ├── Hyperlane
  └── Custom bridge contracts
```

### Data Pipeline
```
Live RPC Subscriptions
    ↓
Block/Event Fetching (Alloy for EVM, Solana SDK)
    ↓
ABI Decoding (Contract logs → Rust structs)
    ↓
Event Processing (Business logic, USD conversions)
    ↓
PostgreSQL Ingestion (Batch writes, idempotency)
    ↓
REST API (Query layer via Actix-web)
```

### Directory Structure
```
src/
├── main.rs                          # Async runtime orchestration
├── schema.rs                        # PostgreSQL table definitions
├── enums.rs                         # Order statuses, transaction types
├── structs.rs                       # Event data models
├── utils.rs                         # Token decimals, USD pricing, timestamps
├── solidity_structs.rs              # ABI-decoded contract structs
├── vaa_processor.rs                 # Wormhole VAA validation
│
├── events/                          # Event processing pipeline
│   ├── event_processor.rs (31KB)    # Core event handler dispatch
│   └── evm_handlers/                # 22 contract event handlers
│       ├── order_created.rs
│       ├── solution_submitted.rs
│       ├── acknowledgement_received.rs
│       ├── swap_executed.rs
│       └── ...
│
├── indexers/                        # Chain-specific indexers
│   ├── evm_indexer.rs (13KB)        # Ethereum-compatible chains
│   ├── solana_indexer.rs (114KB)    # Solana Account model & events
│   └── raydium_events.rs (15KB)     # Solana DEX-specific indexing
│
└── mayan/                           # Mayan bridge integration
```

---

## 🗄️ PostgreSQL Schema (36+ Tables)

### Order Lifecycle Tables
| Table | Purpose |
|-------|---------|
| `intent` | Core cross-chain order metadata |
| `solution` | Solver fulfillment & bid data |
| `acknowledgement` | Order settlement confirmations |
| `intent_state` | State machine transitions |
| `order_created` | Swap/transfer creation events |

### Cross-Chain Messaging
| Table | Purpose |
|-------|---------|
| `message_dispatched_from_vault` | Outgoing bridge messages |
| `received_message_on_vault` | Incoming bridge messages |
| `deposit_received` | User deposit tracking |

### Fee & Settlement
| Table | Purpose |
|-------|---------|
| `intent_fees` | Solver + protocol fee breakdown (JSONB) |
| `settlement` | Final order settlement records |
| `gas_costs` | Per-transaction gas accounting |

### Compliance & Risk
| Table | Purpose |
|-------|---------|
| `sanction_address_list` | OFAC-sanctioned addresses |
| `timed_out_orders` | Failed/abandoned orders |

---

## 🛠️ Technology Stack

**Language:** Rust (36,685 LOC)  
**Async Runtime:** Tokio 1.0 (multi-threaded)  
**EVM Connectivity:** Alloy 0.9 (RPC provider, contract interactions)  
**Solana Connectivity:** Solana SDK 2.1, Anchor Lang  
**Database:** PostgreSQL 12+ with tokio-postgres  
**API Framework:** Actix-web (if used with indexer-api)  
**Messaging:** Wormhole VAA processor, custom bridge handlers  
**Math:** rust_decimal for financial precision

### Key Dependencies
```toml
alloy = { version = "0.9", features = ["full"] }      # EVM RPC
solana-client = "2.1"                                  # Solana RPC
tokio-postgres = { version = "0.7", features = [...] }# Async DB
tokio = { version = "1.0", features = ["full"] }      # Async runtime
serde_json = "1.0"                                     # JSON handling
borsh = "1.5"                                          # Solana serialization
reqwest = "0.12"                                       # HTTP client
```

---

## 🚀 Key Features

### ✅ Real-Time Multi-Chain Indexing
- Concurrent listeners for 6+ EVM chains + Solana
- Sub-second event latency on Solana
- Block-level consistency (not skipping orphaned data)
- Automatic reorg detection & rollback

### ✅ Cross-Chain Order Tracking
- Aggregates intent creation → settlement across chains
- Handles 2-leg swaps (source chain order + destination settlement)
- Tracks solver solutions and competitive bidding
- Validates acknowledgement proofs

### ✅ Financial Accuracy
- USD value tracking for all swaps (via live price feeds)
- Arbitrary-precision decimal math (no float rounding errors)
- Gas cost accounting per transaction
- Fee breakdown: solver fee + protocol fee + bridge fee

### ✅ Fault Tolerance
- Idempotent writes (duplicate events don't corrupt DB)
- Automatic gap detection & backfill
- Retry logic for RPC failures
- Graceful degradation (partial chain failures don't crash entire indexer)

### ✅ Data Freshness
- Live event subscriptions (not polling)
- Batch writes to PostgreSQL (efficient indexing)
- Optional caching layer for recent queries

---

## 📊 Event Processing Examples

### EVM: Order Created
```rust
Event: OrderCreated {
    order_id: 317,
    creator: 0xaed223306A006975c00A939dBEB6d7eBd9C04d80,
    source_chain: Arbitrum (421614),
    destination_chain: Base (84532),
    token_in: USDC,
    token_out: CBDC,
    amount_in: 4950000000000000000,
    amount_out: 4950000000000000000
}
↓
Processed: Fetch token decimals, compute USD values
↓
Insert into `order_created` table with gas fees
```

### Solana: Account State Update via Raydium
```rust
Raydium CLMM Pool State Changed:
    - Liquidity: 1000000.00
    - Current price: 1.25
    - 24h volume: 50000.00
↓
Extract via SPL token + Anchor program IDL
↓
Store in indexed tables for launchpad queries
```

### Cross-Chain: Wormhole VAA Received
```rust
VAA: Message from Ethereum → Solana (emitter_chain=2, sequence=12345)
↓
Validate signature threshold (19/19 guardians)
↓
Decode payload (order settlement instructions)
↓
Update `received_message_on_vault` table
```

---

## 🔌 API Endpoints (via indexer-api)

### Query Cross-Chain Orders
```
GET /intents/history/{user_address}
GET /intents/{intent_id}
```

### Get Transaction History
```
GET /transactions?per_page=10&id=last_id
GET /transactions/launchpad
```

### Check Token Balances
```
POST /contract_balance/{network}
```

### Compliance Checks
```
GET /check_ofac_list/{address}
GET /timed_out_orders
```

---

## 🧪 Testing & Deployment

**Test Coverage:** 
- Unit tests for event parsers
- Integration tests against devnet
- Schema migration tests

**Build:**
```bash
cargo build --release  # ~2 min compile time
```

**Run Locally:**
```bash
# Set config file path
export CONFIG_PATH=config.one-chain.toml

# Set database
export DB_CONNECTION_STRING=postgres://user:pass@localhost:5432/indexer

# Run indexer
cargo run --release
```

**Deployment:**
- Docker container (provided)
- Environment-based configuration
- Graceful shutdown signal handling

---

## 📈 Production Stability

**Monitored Metrics:**
- Blocks behind chain tip (reorg detection)
- Event processing latency (p50, p95, p99)
- Database insert throughput (events/sec)
- RPC error rates per chain
- WAL checkpoint failures

**Incident Handling:**
- Automatic backfill on detected gaps
- Dead-letter queue for parsing errors
- Fallback to archive RPC if primary fails
- Slack/PagerDuty integration (in deployment)

---

## 👨‍💻 Contributions

**Code Stats:**
- 36,685 lines of Rust
- 260 commits (clean history)
- 4 main branches (dev, staging, production, features)
- 114KB Solana indexer alone (deep Account model expertise)

**Key Technical Achievements:**
- Built reorg-safe EVM indexing (handles chain reorgs gracefully)
- Implemented Solana Account state change detection (non-trivial)
- Designed idempotent ingestion (critical for reliability)
- Integrated 3 cross-chain bridges (Wormhole, Hyperlane, custom)
- Created financial calculation pipeline (USD conversions, fee tracking)

---

## 🔐 Security & Compliance

- ✅ **Reorg Safety:** Confirmation lag + parent-hash validation
- ✅ **Idempotency:** Deduplication keys prevent double-counting
- ✅ **OFAC Compliance:** Sanctions list integration
- ✅ **Gas Tracking:** Accurate fee reporting
- ✅ **Timeout Handling:** Abandoned order detection

---

## 🎯 Use Cases

1. **Cross-Chain DEX Infrastructure** — Real-time order tracking for solvers
2. **Bridge Monitoring** — Track cross-chain messages (Wormhole, Hyperlane)
3. **User Portfolio Tracking** — Query transaction history across chains
4. **Compliance Reporting** — OFAC checks, settlement verification
5. **Analytics Platform** — Query raw event data for insights

---

## 📚 References

- [Alloy.rs](https://alloy.rs) — EVM Rust toolkit
- [Solana Program Library](https://docs.solana.com) — SPL tokens
- [Tokio Runtime](https://tokio.rs) — Async Rust
- [PostgreSQL JSONB](https://www.postgresql.org/docs/current/datatype-json.html)
- [Wormhole VAA Spec](https://docs.wormhole.com/wormhole/architecture)

---

**Repository:** https://github.com/srajawat-hub/indexer  
**Created:** 2024 | **Last Updated:** August 2026  
**Language:** Rust 1.75+

---

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