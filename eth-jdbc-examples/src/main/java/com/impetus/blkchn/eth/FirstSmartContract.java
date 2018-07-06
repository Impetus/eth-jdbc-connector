package com.impetus.blkchn.eth;

import org.web3j.abi.TypeReference;
import org.web3j.abi.datatypes.Bool;
import org.web3j.abi.datatypes.Function;
import org.web3j.abi.datatypes.Type;
import org.web3j.abi.datatypes.Utf8String;
import org.web3j.abi.datatypes.generated.Uint256;
import org.web3j.crypto.Credentials;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.RemoteCall;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.tx.Contract;
import org.web3j.tx.TransactionManager;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * <p>Auto generated code.
 * <p><strong>Do not modify!</strong>
 * <p>Please use the <a href="https://docs.web3j.io/command_line.html">web3j command line tools</a>,
 * or the org.web3j.codegen.SolidityFunctionWrapperGenerator in the 
 * <a href="https://github.com/web3j/web3j/tree/master/codegen">codegen module</a> to update.
 *
 * <p>Generated with web3j version 3.4.0.
 */

public class FirstSmartContract extends Contract {
    private static final String BINARY = "6080604052600a60005560018054600160a060020a031916331790556104498061002a6000396000f3006080604052600436106100985763ffffffff7c010000000000000000000000000000000000000000000000000000000060003504166312065fe0811461009d57806317d7de7c146100c457806318db62fb1461014e5780632e1a7d4d1461017a5780633104562b1461019457806360fe47b1146101ac5780636d4ce63c146101c4578063c47f0027146101d9578063d285b7b414610232575b600080fd5b3480156100a957600080fd5b506100b2610247565b60408051918252519081900360200190f35b3480156100d057600080fd5b506100d961024e565b6040805160208082528351818301528351919283929083019185019080838360005b838110156101135781810151838201526020016100fb565b50505050905090810190601f1680156101405780820380516001836020036101000a031916815260200191505b509250505060405180910390f35b34801561015a57600080fd5b506101666004356102e4565b604080519115158252519081900360200190f35b34801561018657600080fd5b506101926004356102ec565b005b3480156101a057600080fd5b5061019260043561032b565b3480156101b857600080fd5b5061019260043561035a565b3480156101d057600080fd5b506100b261035f565b3480156101e557600080fd5b506040805160206004803580820135601f81018490048402850184019095528484526101929436949293602493928401919081908401838280828437509497506103659650505050505050565b34801561023e57600080fd5b5061016661037c565b6000545b90565b60038054604080516020601f60026000196101006001881615020190951694909404938401819004810282018101909252828152606093909290918301828280156102da5780601f106102af576101008083540402835291602001916102da565b820191906000526020600020905b8154815290600101906020018083116102bd57829003601f168201915b5050505050905090565b600054101590565b60015473ffffffffffffffffffffffffffffffffffffffff16331461031057600080fd5b610319816102e4565b15610328576000805482900390555b50565b60015473ffffffffffffffffffffffffffffffffffffffff16331461034f57600080fd5b600080549091019055565b600255565b60025490565b8051610378906003906020840190610385565b5050565b600054600a1090565b828054600181600116156101000203166002900490600052602060002090601f016020900481019282601f106103c657805160ff19168380011785556103f3565b828001600101855582156103f3579182015b828111156103f35782518255916020019190600101906103d8565b506103ff929150610403565b5090565b61024b91905b808211156103ff57600081556001016104095600a165627a7a72305820a30727f98c6fa1e5eb65bf61e69d7aac29d4d4901a08246d432e3aad40722a4d0029";

    public static final String FUNC_GETBALANCE = "getBalance";

    public static final String FUNC_GETNAME = "getName";

    public static final String FUNC_CHECKVALUE = "checkValue";

    public static final String FUNC_WITHDRAW = "withdraw";

    public static final String FUNC_DEPOSITE = "deposite";

    public static final String FUNC_SET = "set";

    public static final String FUNC_GET = "get";

    public static final String FUNC_SETNAME = "setName";

    public static final String FUNC_LOAN = "loan";

    @SuppressWarnings("deprecation")
	protected FirstSmartContract(String contractAddress, Web3j web3j, Credentials credentials, BigInteger gasPrice, BigInteger gasLimit) {
        super(BINARY, contractAddress, web3j, credentials, gasPrice, gasLimit);
    }

    @SuppressWarnings("deprecation")
	protected FirstSmartContract(String contractAddress, Web3j web3j, TransactionManager transactionManager, BigInteger gasPrice, BigInteger gasLimit) {
        super(BINARY, contractAddress, web3j, transactionManager, gasPrice, gasLimit);
    }

    public RemoteCall<BigInteger> getBalance() {
        final Function function = new Function(FUNC_GETBALANCE, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
        return executeRemoteCallSingleValueReturn(function, BigInteger.class);
    }

    public RemoteCall<String> getName() {
        final Function function = new Function(FUNC_GETNAME, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Utf8String>() {}));
        return executeRemoteCallSingleValueReturn(function, String.class);
    }

    public RemoteCall<Boolean> checkValue(BigInteger amount) {
        final Function function = new Function(FUNC_CHECKVALUE, 
                Arrays.<Type>asList(new Uint256(amount)),
                Arrays.<TypeReference<?>>asList(new TypeReference<Bool>() {}));
        return executeRemoteCallSingleValueReturn(function, Boolean.class);
    }

    public RemoteCall<TransactionReceipt> withdraw(BigInteger ammount) {
        final Function function = new Function(
                FUNC_WITHDRAW, 
                Arrays.<Type>asList(new Uint256(ammount)),
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    public RemoteCall<TransactionReceipt> deposite(BigInteger ammount) {
        final Function function = new Function(
                FUNC_DEPOSITE, 
                Arrays.<Type>asList(new Uint256(ammount)),
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    public RemoteCall<TransactionReceipt> set(BigInteger x) {
        final Function function = new Function(
                FUNC_SET, 
                Arrays.<Type>asList(new Uint256(x)),
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    public RemoteCall<BigInteger> get() {
        final Function function = new Function(FUNC_GET, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Uint256>() {}));
        return executeRemoteCallSingleValueReturn(function, BigInteger.class);
    }

    public RemoteCall<TransactionReceipt> setName(String newName) {
        final Function function = new Function(
                FUNC_SETNAME, 
                Arrays.<Type>asList(new Utf8String(newName)),
                Collections.<TypeReference<?>>emptyList());
        return executeRemoteCallTransaction(function);
    }

    public RemoteCall<String> getName(String newName,byte[] bytesarr,List<String> lststr,List<byte[]> bytlist) {
        final Function function = new Function(FUNC_GETNAME,
                Arrays.<Type>asList(),
                Arrays.<TypeReference<?>>asList(new TypeReference<Utf8String>() {}));
        return executeRemoteCallSingleValueReturn(function, String.class);
    }

    public RemoteCall<String> getName(byte[] bytesarr) {
        final Function function = new Function(FUNC_GETNAME,
                Arrays.<Type>asList(),
                Arrays.<TypeReference<?>>asList(new TypeReference<Utf8String>() {}));
        return executeRemoteCallSingleValueReturn(function, String.class);
    }



    public RemoteCall<Boolean> loan() {
        final Function function = new Function(FUNC_LOAN, 
                Arrays.<Type>asList(), 
                Arrays.<TypeReference<?>>asList(new TypeReference<Bool>() {}));
        return executeRemoteCallSingleValueReturn(function, Boolean.class);
    }

    public static RemoteCall<FirstSmartContract> deploy(Web3j web3j, Credentials credentials, BigInteger gasPrice, BigInteger gasLimit) {
        return deployRemoteCall(FirstSmartContract.class, web3j, credentials, gasPrice, gasLimit, BINARY, "");
    }

    public static RemoteCall<FirstSmartContract> deploy(Web3j web3j, TransactionManager transactionManager, BigInteger gasPrice, BigInteger gasLimit) {
        return deployRemoteCall(FirstSmartContract.class, web3j, transactionManager, gasPrice, gasLimit, BINARY, "");
    }

    public static FirstSmartContract load(String contractAddress, Web3j web3j, Credentials credentials, BigInteger gasPrice, BigInteger gasLimit) {
        return new FirstSmartContract(contractAddress, web3j, credentials, gasPrice, gasLimit);
    }

    public static FirstSmartContract load(String contractAddress, Web3j web3j, TransactionManager transactionManager, BigInteger gasPrice, BigInteger gasLimit) {
        return new FirstSmartContract(contractAddress, web3j, transactionManager, gasPrice, gasLimit);
    }
}
