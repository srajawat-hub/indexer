use crate::utils::deserialize_number;
use alloy::sol;
use serde::{Deserialize, Serialize};

pub mod intent_processor {
    use alloy::sol;
    sol!(
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        IntentProcessorV2,
        "abi/IntentProcessor.json"
    );
}

pub mod mocked_ln {
    use alloy::sol;
    sol!(
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        MockLN,
        "abi/MockLN.json"
    );
}

pub mod vault {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        Vault,
        "abi/Vault.json"
    );
}

pub mod token {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        Token,
        "abi/Token.json"
    );
}

pub mod intent_lib_v2 {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        IntentLibV2,
        "abi/IntentLibV2.json"
    );
}

pub mod intent_lib {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        IntentLib,
        "abi/IntentLib.json"
    );
}

// Define our intent payload enum using Alloy's sol! macro
sol! {
    #[derive(Debug)]
    struct UserBalance {
        uint256 chainIdentifier;
        bytes32 tokenAddress;
        uint256 amount;
    }

    #[derive(Debug)]
    struct SolidityIntentPayload {
        IntentPayloadEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct SolidityRawIntent {
        uint64 id;
        address intentOwner;
        uint64 creationUnixTimestampInSeconds;
        SolidityIntentPayload intentPayload;
    }

    #[derive(Debug)]
    struct SoliditySolution {
        SoliditySolutionEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct IntentPayloadSwapData {
        bytes32 tokenIn;
        uint256 amountIn;
        bytes32 tokenOut;
        uint256 worstExecutionOutTokenAmount;
        uint64 timeoutTimestampInSeconds;
        uint256 sourceChainId;
        uint256 destinationChainId;
    }

    #[derive(Debug)]
    struct IntentPayloadTransferData {
        bytes32 tokenIn;
        uint256 amountIn;
        bytes32 tokenOut;
        uint256 amountOut;
        SolidityTransferDestination destination;
        uint256 destinationChainId;
        uint256 sourceChainId;
        uint64 timeoutTimestampInSeconds;
    }

    #[derive(Debug)]
    struct IntentPayloadStakeData {
        StakeActionEnum stakeAction;
        StakeProtocolEnum stakeProtocol;
        uint256 amount;
        bytes32 tokenAddress;
        uint256 chainId;
        uint64 timeoutTimestampInSeconds;
        bytes32 stakingPoolAddress;
    }

    #[derive(Debug)]
    enum StakeActionEnum {
        Deposit,
        Withdraw
    }

    #[derive(Debug)]
    enum StakeProtocolEnum {
        Lido,
        Aave,
        SolanaLST,
        Marginfi
    }

    #[derive(Debug)]
    enum TransferDestinationEnum {
        DestinationAddress,
        DestinationVault
    }

    #[derive(Debug)]
    struct SolidityTransferDestination {
        TransferDestinationEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct SoliditySolutionSwapSolutionData {
        address intentOwner;
        bytes32 solver;
        uint64 intentId;
        uint256 amountIn;
        uint256 destinationChainId;
        SolverSolution[] solutions;
    }

    #[derive(Debug)]
    struct SoliditySolutionTransferSolutionData {
        SolverSolution[] solutions;
    }

    #[derive(Debug)]
    struct SoliditySolutionStakeSolutionData {
        SolverSolution solution;
    }

    #[derive(Debug)]
    struct SolverSolution {
        SoliditySolutionType solution;
        uint256 sourceChainId;
        bytes32 tokenIn;
        uint256 amountIn;
        uint256 amountOut;
    }

    #[derive(Debug)]
    enum SoliditySolutionEnum {
        SwapSolution,
        TransferSolution,
        StakeSolution
    }

    #[derive(Debug)]
    #[derive(PartialEq)]
    enum IntentPayloadEnum {
        Transact,
        Stake
    }

    #[derive(Debug)]
    struct TransferDestinationVaultData {
        address receiverAddress;
    }

    #[derive(Debug)]
    struct TransferDestinationAddressData {
        bytes32 receiverAddress;
    }

    #[derive(Debug)]
    #[derive(PartialEq)]
    enum IntentProcessorBoundMessageEnum {
        RegisterVault,
        UpdateVaultStatus,
        Acknowledgement,
        Deposit
    }

    #[derive(Debug)]
    struct SolidityIntentProcessorBoundMessage {
        IntentProcessorBoundMessageEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct IntentProcessorBoundMessageRegisterVaultData {
        Chain chain;
    }

    #[derive(Debug)]
    struct IntentProcessorBoundMessageUpdateVaultStatusData {
        uint256 chainIdentifier;
        bool isActive;
    }

    #[derive(Debug)]
    struct IntentProcessorBoundMessageAcknowledgementData {
        uint64 orderId;
        bool result;
        string errorMessage;
        SolidityAcknowledgementMetadata metadata;
    }

    #[derive(Debug)]
    struct SolidityAcknowledgementMetadata {
        IntentPayloadEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct AcknowledgementMetadataStake {
        bytes32 receiptTokenAddress;
        bool isTokenCredited;
        uint256 amountCredited;
        uint256 request_id;
    }

    /// This metadata is used for transact intents that are processed using deBridge DLN.
    #[derive(Debug)]
    struct AcknowledgementMetadataTransact {
        // This is the token mint that the solver has given to the vault.
        bytes32 receiveTokenAddress;
        // Amount of tokens that were credited during the transact.
        uint256 amount;
        // This is the address that received the above token.
        bytes32 receiver;
    }

    #[derive(Debug)]
    struct IntentProcessorBoundMessageDepositData {
        address userAddress;
        uint256 amount;
        bytes32 tokenAddress;
        uint256 chainIdentifier;
    }

    #[derive(Debug)]
    enum VaultBoundMessageEnum {
        PlaceOrder,
        CancelOrder
    }

    #[derive(Debug)]
    struct SolidityVaultBoundMessage {
        VaultBoundMessageEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct VaultBoundMessagePlaceOrderData {
        SolidityOrder order;
    }

    #[derive(Debug)]
    enum ReceiverEnum {
        UserAddress,
        Vault
    }

    #[derive(Debug)]
    struct SolidityReceiver {
        ReceiverEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct ReceiverUserAddressData {
        bytes32 userAddress;
    }

    #[derive(Debug)]
    struct ReceiverVaultData {
        bytes32 poolAddress;
        address vaultUser;
    }

    #[derive(Debug)]
    enum SolutionTypeEnum {
        LocalTransfer,
        LocalSwap,
        CrossChain,
        Stake
    }

    #[derive(Debug)]
    struct SoliditySolutionType {
        SolutionTypeEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct SolutionTypeStakeData {
        StakeProtocolEnum stakeProtocol;
        StakeActionEnum stakeAction;
        bytes32 stakingPoolAddress;
    }

    #[derive(Debug)]
    struct SolutionTypeLocalSwapData {
        SolidityAggregator aggregator;
    }

    #[derive(Debug)]
    struct SolutionTypeCrossChainData {
        SolidityLiquidityNetwork liquidityNetwork;
    }

    #[derive(Debug)]
    enum AggregatorEnum {
        Jupiter,
        OneInch
    }

    #[derive(Debug)]
    struct SolidityAggregator {
        AggregatorEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct AggregatorJupiterData {
        SoliditySolanaAccount[] accounts;
    }

    #[derive(Debug)]
    enum LiquidityNetworkEnum {
        DLN,
        MockedLN
    }

    #[derive(Debug)]
    struct SolidityLiquidityNetwork {
        LiquidityNetworkEnum enumVariant;
        bytes data;
    }

    #[derive(Debug)]
    struct LiquidityNetworkDLNData {
        uint256 sourceChainId;
        uint256 destinationChainId;
        bytes32 destinationVaultAuthority;
        bytes32 destinationVaultAddress;
    }

    #[derive(Debug)]
    struct LiquidityNetworkMockedLNData {
        uint256 sourceChainId;
        uint256 destinationChainId;
        bytes32 destinationVaultAuthority;
        bytes32 destinationVaultAddress;
    }

    #[derive(Debug)]
    struct SoliditySolanaAccount {
        bytes32 pubkey;
        bool isSigner;
        bool isWritable;
    }

    #[derive(Debug)]
    struct Chain {
        uint256 identifier;
        bytes32 vaultAddress;
        bytes32 vaultAuthority;
        bytes32 poolAddress;
        bool isActive;
        DomainId[] domainIds;
        ExecutionEnvironment executionEnvironment;
    }

    #[derive(Debug)]
    enum ExecutionEnvironment {
        EVM,
        SVM
    }

    #[derive(Debug)]
    struct DomainId {
        uint256 domainId;
        InteropProvider interopProvider;
    }

    #[derive(Debug)]
    enum InteropProvider {
        LayerZero,
        Hyperlane
    }

    #[derive(Debug)]
    struct VaultState {
        bytes32 vaultAddress;
        bool isVaultActive;
        uint256 chainId;
    }

    #[derive(Debug)]
    struct SolidityOrder {
        uint64 intentId;
        uint64 orderId;
        uint256 amountIn;
        bytes32 tokenIn;
        uint256 amountOut;
        bytes32 tokenOut;
        SolidityReceiver receiver;
        /// The source chain id is the identifier of the chain
        /// in our contract. This is used to send the message
        /// to the correct chain.
        ///
        /// This field is not used by the vaults.
        uint256 sourceChainId;
        /// Used to update the user balance on successful acknowledgement
        uint256 destinationChainId;
        uint64 timeoutUnixTimestampInSec;
        SoliditySolutionType solution;
        address initiatorAddress;
        bool multiLeg;
    }


    // @dev  Struct representing an order.
    #[derive(Debug)]
    struct DLNOrder {
        /// Nonce for each maker.
        uint64 makerOrderNonce;
        /// Order maker address (EOA signer for EVM) in the source chain.
        bytes makerSrc;
        /// Chain ID where the order's was created.
        uint256 giveChainId;
        /// Address of the ERC-20 token that the maker is offering as part of this order.
        /// Use the zero address to indicate that the maker is offering a native blockchain token (such as Ether, Matic, etc.).
        bytes giveTokenAddress;
        /// Amount of tokens the maker is offering.
        uint256 giveAmount;
        // the ID of the chain where an order should be fulfilled.
        uint256 takeChainId;
        /// Address of the ERC-20 token that the maker is willing to accept on the destination chain.
        bytes takeTokenAddress;
        /// Amount of tokens the maker is willing to accept on the destination chain.
        uint256 takeAmount;
        /// Address on the destination chain where funds should be sent upon order fulfillment.
        bytes receiverDst;
        /// Address on the source (current) chain authorized to patch the order by adding more input tokens, making it more attractive to takers.
        bytes givePatchAuthoritySrc;
        /// Address on the destination chain authorized to patch the order by reducing the take amount, making it more attractive to takers,
        /// and can also cancel the order in the take chain.
        bytes orderAuthorityAddressDst;
        // An optional address restricting anyone in the open market from fulfilling
        // this order but the given address. This can be useful if you are creating a order
        // for a specific taker. By default, set to empty bytes array (0x)
        bytes allowedTakerDst;
        // An optional address on the source (current) chain where the given input tokens
        // would be transferred to in case order cancellation is initiated by the orderAuthorityAddressDst
        // on the destination chain. This property can be safely set to an empty bytes array (0x):
        // in this case, tokens would be transferred to the arbitrary address specified
        // by the orderAuthorityAddressDst upon order cancellation
        bytes allowedCancelBeneficiarySrc;
        /// An optional external call data payload.
        bytes externalCall;
    }

    #[derive(Debug)]
    event CreatedOrder(
        DLNOrder order,
        bytes32 orderId,
        bytes affiliateFee,
        uint256 nativeFixFee,
        uint256 percentFee,
        uint32 referralCode,
        bytes metadata
    );

    #[derive(Debug)]
    event DebridgeOrderCreated(uint64 orderId, bytes32 debridgeOrderId);

    #[derive(Debug)]
    event Dispatch(
        address indexed sender,
        uint32 indexed destination,
        bytes32 indexed recipient,
        bytes message
    );

    #[derive(Debug)]
    event DispatchId(bytes32 indexed messageId);

    #[derive(Debug)]
    event ProcessId(bytes32 indexed messageId);
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ResultCosts {
    pub destination_cost: AmountTypes,
    pub inclusive_layer_fee: AmountTypes,
    pub provider_fee: ThirdPartyFeeResult,
    pub source_cost: AmountTypes,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct AmountTypes {
    pub value: Option<String>,
    pub value_type: Option<String>,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ThirdPartyFeeResult {
    pub flat_fee: AmountTypes,
    pub provider: Option<String>,
    pub solver_fee: AmountTypes,
    pub variable_fee: AmountTypes,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct QuoteApiResponse {
    pub fee_data: ResultCosts,
}

// Add these structs near other struct definitions
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderId {
    pub bytesValue: Option<String>,
    pub bytesArrayValue: Option<String>,
    pub stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct ChainId {
    bytesValue: Option<String>,
    bytesArrayValue: Option<String>,
    #[serde(deserialize_with = "deserialize_number")]
    bigIntegerValue: Option<f64>,
    stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TokenAddress {
    Base64Value: Option<String>,
    bytesArrayValue: Option<String>,
    stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct Amount {
    bytesValue: Option<String>,
    bytesArrayValue: Option<String>,
    #[serde(deserialize_with = "deserialize_number")]
    bigIntegerValue: Option<f64>,
    stringValue: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct TokenMetadata {
    decimals: Option<i32>,
    name: Option<String>,
    symbol: Option<String>,
    logoURI: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct OfferWithMetadata {
    chainId: ChainId,
    tokenAddress: TokenAddress,
    amount: Amount,
    finalAmount: Amount,
    metadata: TokenMetadata,
    decimals: Option<i32>,
    name: Option<String>,
    symbol: Option<String>,
    logoURI: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
struct AffiliateFee {
    beneficiarySrc: Option<TokenAddress>,
    amount: Option<Amount>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Order {
    pub orderId: OrderId,
    pub creationTimestamp: i64,
    pub giveOfferWithMetadata: OfferWithMetadata,
    pub takeOfferWithMetadata: OfferWithMetadata,
    pub state: String,
    pub externalCallState: String,
    pub finalPercentFee: Amount,
    pub fixFee: Amount,
    pub affiliateFee: AffiliateFee,
    pub unlockAuthorityDst: Option<TokenAddress>,
    pub createEventTransactionHash: Option<TokenAddress>,
    pub preswapData: Option<serde_json::Value>,
    pub orderMetadata: Option<serde_json::Value>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrdersResponse {
    pub orders: Vec<Order>,
}
