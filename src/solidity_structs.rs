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
    enum IntentPayloadEnum {
        Swap,
        Transfer,
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
        uint64 intentId;
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
    struct SolidityOrder {
        uint64 intentId;
        uint256 amountIn;
        bytes32 tokenIn;
        uint256 amountOut;
        bytes32 tokenOut;
        SolidityReceiver receiver;
        uint256 sourceChainId;
        uint256 destinationChainId;
        uint64 timeoutUnixTimestampInSec;
        SoliditySolutionType solution;
        bytes hook;
        address initiatorAddress;
        bytes additionalData;
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
}
