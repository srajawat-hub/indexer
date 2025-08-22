use alloy::sol;

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

pub mod uniswap_v3_factory_lib {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        UniswapV3FactoryLib,
        "abi/UniswapV3Factory.json"
    );
}

pub mod non_fungible_position_manager {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        NonFungiblePositionManager,
        "abi/NonfungiblePositionManager.json"
    );
}

pub mod uniswap_v3_pool_lib {
    use alloy::sol;
    use UniswapV3PoolLib::TokenLaunchType;

    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        UniswapV3PoolLib,
        "abi/UniswapV3Pool.json"
    );

    impl ToString for TokenLaunchType {
        fn to_string(&self) -> String {
            match self.clone().into() {
                0 => "FairLaunch".to_string(),
                1 => "CuratedLaunch".to_string(),
                n => panic!("Unsupported token launch type: {}", n),
            }
        }
    }
}

pub mod hook_executor {
    use alloy::sol;
    sol!(
        #[allow(clippy::too_many_arguments)]
        #[allow(missing_docs)]
        #[sol(rpc)]
        #[derive(Debug)]
        HookExecutor,
        "abi/HookExecutor.json"
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
        Stake,
        LaunchpadSwap,
        LaunchpadAddLiquidity,
        LaunchpadRemoveLiquidity
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
        Deposit,
        LaunchpadPoolCreation
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
    struct AcknowledgementMetadataTransactFailed {
        bytes32 tokenAddress;
        // amount of tokens that were credited during the swap
        uint256 amountCredited;
    }

    // create one for launchpad swap
    #[derive(Debug)]
    struct AcknowledgementMetadataLaunchpadSwap {
        // The output token amount that was received
        uint256 receivedAmount;
    }

    #[derive(Debug)]
    struct AcknowledgementMetadataLaunchpadAddLiquidity {
        // The receipt NFT that was minted to the user
        bytes32 receiptNFTAddress;
        // This is only used for NFTs on EVM. Can be set to 0 for Solana NFTs.
        uint256 tokenId;
        // exact amount of token0 that was added
        uint256 amount0;
        // exact amount of token1 that was added
        uint256 amount1;
    }

    #[derive(Debug)]
    struct AcknowledgementMetadataLaunchpadRemoveLiquidity {
        // exact amount of token0 that was removed
        uint256 amount0;
        // exact amount of token1 that was removed
        uint256 amount1;
    }

    #[derive(Debug)]
    struct IntentProcessorBoundMessageDepositData {
        address userAddress;
        uint256 amount;
        bytes32 tokenAddress;
        uint256 chainIdentifier;
    }

    #[derive(Debug)]
    struct IntentProcessorBoundMessageLaunchpadPoolCreationData {
        bytes32 poolAddress;
        bytes32 token0;
        bytes32 token1;
        uint256 chainId;
        uint256 launchType;
        bytes32 projectLiquidityProvider;
        uint256 feeTier;
        uint8 reserveTokenIndex;
        uint256 exclusiveTradingPeriodStartTimestamp;
        uint256 exclusiveTradingPeriodEndTimestamp;
    }

    #[derive(Debug)]
    struct LaunchpadPool {
        bytes32 token0;
        bytes32 token1;
        bytes32 poolAddress;
        uint256 chainId;
        // whether its fair/curated
        uint256 launchType;
        // The address that is responsible for seeding the pool with initial liquidity.
        bytes32 projectLiquidityProvider;
        // Would be set to false when pool is created. Will be flipped
        // when the project liquidity is provided.
        //
        // The trading is not allowed until the project liquidity is provided.
        bool isProjectLiquidityProvided;
        // Whether token0 or token1 is the reserve token i.e. the token with which
        // the launchpad token is bonded
        uint8 reserveTokenIndex;
        // The time at which the exclusive trading period starts
        uint256 exclusiveTradingPeriodStartTimestamp;
        // The time at which the exclusive trading period ends
        uint256 exclusiveTradingPeriodEndTimestamp;
        // The fee tier of the pool
        uint256 feeTier;
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
        Stake,
        LaunchpadSwap,
        LaunchpadAddLiquidity,
        LaunchpadRemoveLiquidity
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
    struct SolutionTypeLaunchpadSwapData {
        bytes32 poolAddress;
    }

    #[derive(Debug)]
    struct SolutionTypeLaunchpadAddLiquidityData {
        int256 tickLower;
        int256 tickUpper;
        uint256 liquidity;
        uint256 amount0Max;
        uint256 amount1Max;
        bytes32 token0;
        bytes32 token1;
        bytes32 poolAddress;
    }

    #[derive(Debug)]
    struct SolutionTypeLaunchpadRemoveLiquidityData {
        uint256 liquidity;
        uint256 amount0Min;
        uint256 amount1Min;
        bytes32 receiptTokenAddress;
        uint256 tokenId;
        bytes32 token0;
        bytes32 token1;
        bytes32 poolAddress;
    }

    #[derive(Debug)]
    enum AggregatorEnum {
        Jupiter,
        OneInch,
        Hashflow
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
        MockedLN,
        Mayan,
        OneInch,
        Hashflow,
        Stargate
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
