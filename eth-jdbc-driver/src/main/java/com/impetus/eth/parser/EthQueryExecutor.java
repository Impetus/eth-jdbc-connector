/******************************************************************************* 
* * Copyright 2018 Impetus Infotech.
* *
* * Licensed under the Apache License, Version 2.0 (the "License");
* * you may not use this file except in compliance with the License.
* * You may obtain a copy of the License at
* *
* * http://www.apache.org/licenses/LICENSE-2.0
* *
* * Unless required by applicable law or agreed to in writing, software
* * distributed under the License is distributed on an "AS IS" BASIS,
* * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* * See the License for the specific language governing permissions and
* * limitations under the License.
******************************************************************************/
package com.impetus.eth.parser;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import com.impetus.blkch.sql.smartcontract.*;
import java.util.*;
import com.impetus.blkch.sql.query.*;
import com.impetus.blkch.sql.query.Comparator;
import com.impetus.blkch.util.Utilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.crypto.CipherException;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.WalletUtils;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.RemoteCall;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.core.methods.response.EthBlockNumber;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.exceptions.TransactionException;
import org.web3j.tx.Transfer;
import org.web3j.tx.gas.DefaultGasProvider;
import org.web3j.utils.Convert;
import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.GroupedDataFrame;
import com.impetus.blkch.sql.function.ClassName;
import com.impetus.blkch.sql.function.Parameters;
import com.impetus.blkch.sql.insert.ColumnName;
import com.impetus.blkch.sql.insert.ColumnValue;
import com.impetus.blkch.sql.insert.Insert;
import com.impetus.blkch.sql.parser.AbstractQueryExecutor;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.BytesArgs;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.DataNode;
import com.impetus.blkch.sql.query.DirectAPINode;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.GroupByClause;
import com.impetus.blkch.sql.query.HavingClause;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.ListAgrs;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.LogicalOperation.Operator;
import com.impetus.blkch.util.Range;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.query.EthColumns;
import com.impetus.eth.query.EthTables;

public class EthQueryExecutor extends AbstractQueryExecutor {

    private static final String TO_ADDRESS = "toAddress";

    private static final String UNIT = "unit";

    private static final String ASYNC = "async";

    private BigInteger GAS = DefaultGasProvider.GAS_LIMIT;

    private BigInteger GAS_PRICE = DefaultGasProvider.GAS_PRICE;

    private static final Logger LOGGER = LoggerFactory.getLogger(EthQueryExecutor.class);

    Object classObjectSmartCrt = null;

    static Map<Class, Object> smartCrtClassObjectMap = new HashMap();

    private Web3j web3jClient;

    private Properties properties;

    protected Map<String, List<String>> blkTxnHashMap = new HashMap<>();

    public EthQueryExecutor(LogicalPlan logicalPlan, Web3j web3jClient, Properties properties) {
        this.logicalPlan = logicalPlan;
        this.web3jClient = web3jClient;
        this.properties = properties;
        this.originalPhysicalPlan = new EthPhysicalPlan(logicalPlan);
        this.physicalPlan = originalPhysicalPlan;
    }

    public DataFrame executeQuery() {
        physicalPlan.getWhereClause().traverse();
        if (!physicalPlan.validateLogicalPlan()) {
            throw new BlkchnException("This query can't be executed");
        }
        DataFrame dataframe = getFromTable();
        if (dataframe.isEmpty()) {
            return dataframe;
        }
        List<OrderItem> orderItems = null;
        if (logicalPlan.getQuery().hasChildType(OrderByClause.class)) {
            OrderByClause orderByClause = logicalPlan.getQuery().getChildType(OrderByClause.class, 0);
            orderItems = orderByClause.getChildType(OrderItem.class);
        }
        LimitClause limitClause = null;
        if (logicalPlan.getQuery().hasChildType(LimitClause.class)) {
            limitClause = logicalPlan.getQuery().getChildType(LimitClause.class, 0);
        }
        if (logicalPlan.getQuery().hasChildType(GroupByClause.class)) {
            GroupByClause groupByClause = logicalPlan.getQuery().getChildType(GroupByClause.class, 0);
            List<Column> groupColumns = groupByClause.getChildType(Column.class);
            List<String> groupByCols = groupColumns.stream()
                .map(col -> col.getChildType(IdentifierNode.class, 0).getValue()).collect(Collectors.toList());
            GroupedDataFrame groupedDF = dataframe.group(groupByCols);
            DataFrame afterSelect;
            if (logicalPlan.getQuery().hasChildType(HavingClause.class)) {
                afterSelect = groupedDF.having(logicalPlan.getQuery().getChildType(HavingClause.class, 0))
                    .select(physicalPlan.getSelectItems());
            } else {
                afterSelect = groupedDF.select(physicalPlan.getSelectItems());
            }
            DataFrame afterOrder;
            if (orderItems != null) {
                afterOrder = afterSelect.order(orderItems);
            } else {
                afterOrder = afterSelect;
            }
            if (limitClause == null) {
                return afterOrder;
            } else {
                return afterOrder.limit(limitClause);
            }
        }
        DataFrame preSelect;
        if (orderItems != null) {
            preSelect = dataframe.order(orderItems);
        } else {
            preSelect = dataframe;
        }
        DataFrame afterOrder;
        if (limitClause == null) {
            afterOrder = preSelect;
        } else {
            afterOrder = preSelect.limit(limitClause);
        }
        return afterOrder.select(physicalPlan.getSelectItems());
    }

    public Map<String, Integer> computeDataTypeColumnMap() {
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        return physicalPlan.getColumnTypeMap(tableName);
    }

    private DataFrame getFromTable() {
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        if (physicalPlan.getWhereClause() != null) {
            DataNode<?> finalData;
            if (physicalPlan.getWhereClause().hasChildType(LogicalOperation.class)) {
                TreeNode directAPIOptimizedTree =
                    executeDirectAPIs(tableName, physicalPlan.getWhereClause().getChildType(LogicalOperation.class, 0));
                TreeNode optimizedTree = optimize(directAPIOptimizedTree);
                finalData = execute(optimizedTree);
            } else if (physicalPlan.getWhereClause().hasChildType(DirectAPINode.class)) {
                DirectAPINode node = physicalPlan.getWhereClause().getChildType(DirectAPINode.class, 0);
                finalData = getDataNode(node.getTable(), node.getColumn(), node.getValue());
            } else {
                RangeNode<?> rangeNode = physicalPlan.getWhereClause().getChildType(RangeNode.class, 0);
                finalData = executeRangeNode(rangeNode);
                finalData.traverse();
            }
            return createDataFrame(finalData,tableName);
        } else {
            throw new BlkchnException("Can't query without where clause. Data will be huge");
        }

    }

    @Override
    public RangeNode getFullRange(){
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        RangeNode rangeNode = new RangeNode(tableName,EthColumns.BLOCKNUMBER);
        BigInteger blockHeight = null;
        try{
            blockHeight = getBlockHeight();
        }catch(IOException e){
        }
        rangeNode.getRangeList().addRange(new Range(new BigInteger("1"),blockHeight));
        return rangeNode;
    }

    @Override
    public  RangeNode getRangeNodeFromDataNode(DataNode dataNode) {
        String tableName = dataNode.getTable();
        RangeOperations rangeOps = physicalPlan.getRangeOperations(tableName, EthColumns.BLOCKNUMBER);
        if(tableName.equalsIgnoreCase(EthTables.BLOCK) && !dataNode.getKeys().isEmpty()){
            BigInteger directBlock = new BigInteger(dataNode.getKeys().get(0).toString());
            RangeNode rangeNode = new RangeNode(tableName,EthColumns.BLOCKNUMBER);
            rangeNode.getRangeList().addRange(new Range(directBlock,directBlock));
            return rangeNode;
        }else if(dataNode.getTable().equalsIgnoreCase(EthTables.TRANSACTION) && !dataNode.getKeys().isEmpty()){
            if(dataNode.getKeys().get(0) instanceof List && !((List) dataNode.getKeys().get(0)).isEmpty()){
                RangeNode rangeNode = new RangeNode(tableName,EthColumns.BLOCKNUMBER);
                try{
                    BigInteger directBlock = ((Transaction)dataMap.get(((List) dataNode.getKeys().get(0)).get(0).toString())).getBlockNumber();
                    rangeNode.getRangeList().addRange(new Range(directBlock,directBlock));
                }catch(Exception e){
                    rangeNode.getRangeList().addRange(new Range(rangeOps.getMinValue(), rangeOps.getMinValue()));
                }
                return rangeNode;
            }else{
                RangeNode rangeNode = new RangeNode(tableName,EthColumns.BLOCKNUMBER);
                try{
                    BigInteger directBlock = ((Transaction)dataMap.get(dataNode.getKeys().get(0).toString())).getBlockNumber();
                    rangeNode.getRangeList().addRange(new Range(directBlock,directBlock));
                }catch(Exception e){
                    rangeNode.getRangeList().addRange(new Range(rangeOps.getMinValue(), rangeOps.getMinValue()));
                }
                return rangeNode;
            }
        } else {
            RangeNode rangeNode = new RangeNode<>(tableName,EthColumns.BLOCKNUMBER);
            rangeNode.getRangeList().addRange(new Range(rangeOps.getMinValue(), rangeOps.getMinValue()));
            return rangeNode;
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected DataNode<?> getDataNode(String table, String column, String value) {
        if (dataMap.containsKey(value)) {
            return new DataNode<>(table, Arrays.asList(value));
        }

        if (table.equals(EthTables.BLOCK)) {
            Block block = null;
            if (column.equals(EthColumns.BLOCKNUMBER)) {
                try {
                    block = getBlockByNumber(value);
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                    return new DataNode<>(table, Arrays.asList());
                }
            } else if (column.equals(EthColumns.HASH)) {
                try {
                    block = getBlockByHash(value.replace("'", ""));
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                    return new DataNode<>(table, Arrays.asList());
                }
            }
            dataMap.put(block.getNumber().toString(), block);
            return new DataNode<>(table, Arrays.asList(block.getNumber().toString()));

        } else if (table.equals(EthTables.TRANSACTION)) {

            if (column.equals(EthColumns.HASH)) {
                Transaction transaction = null;
                try {
                    transaction = getTransactionByHash(value.replace("'", ""));
                    dataMap.put(transaction.getHash(), transaction);

                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                    return new DataNode<>(table, Arrays.asList());
                }
                return new DataNode<>(table, Arrays.asList(transaction.getHash()));
            } else if (column.equals(EthColumns.BLOCKNUMBER)) {
                List keys = new ArrayList();
                try {

                    List<?> txnList = getTransactions(value.replace("'", ""));
                    for (Transaction txnInfo : (List<Transaction>) txnList) {
                        dataMap.put(txnInfo.getHash(), txnInfo);
                        keys.add(txnInfo.getHash());
                    }

                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                    return new DataNode<>(table, Arrays.asList());
                }
                return new DataNode<>(table, keys);
            }else if (column.equals(EthColumns.BLOCKHASH)) {
                List keys = new ArrayList();
                try {
                    Block block = getBlockByHash(value.replace("'", ""));
                    List<?> txnList = block.getTransactions().stream().map(transactionResult -> transactionResult.get()).collect(Collectors.toList());
                    for (Transaction txnInfo : (List<Transaction>) txnList) {
                        dataMap.put(txnInfo.getHash(), txnInfo);
                        keys.add(txnInfo.getHash());
                    }

                } catch (Exception e) {
                    LOGGER.warn(e.getMessage());
                    return new DataNode<>(table, Arrays.asList());
                }
                return new DataNode<>(table, keys);
            } else
                throw new BlkchnException(
                        String.format("There is no direct API for table %s and column %s combination", table, column));

        } else
            throw new BlkchnException(
                    String.format("There is no direct API for table %s and column %s combination", table, column));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected <T extends Number & Comparable<T>> DataNode<?> executeRangeNode(RangeNode<T> rangeNode) {
        if (rangeNode.getRangeList().getRanges().isEmpty()) {
            return new DataNode<T>(rangeNode.getTable(), new ArrayList<>());
        }
        RangeOperations<T> rangeOps =
            (RangeOperations<T>) physicalPlan.getRangeOperations(rangeNode.getTable(), rangeNode.getColumn());
        String rangeCol = rangeNode.getColumn();
        String rangeTable = rangeNode.getTable();
        BigInteger height;
        try {
            height = getBlockHeight();
        } catch (Exception e) {
            throw new BlkchnException("Error getting height of ledger", e);
        }
        List<DataNode<String>> dataNodes = rangeNode.getRangeList().getRanges().stream().map(range -> {

            List<String> keys = new ArrayList<>();
            T current = range.getMin().equals(rangeOps.getMinValue()) ? (T) new BigInteger("0") : range.getMin();
            T max =
                range.getMax().equals(rangeOps.getMaxValue()) ? (T) rangeOps.subtract((T) height, 1) : range.getMax();
            do {
                if (EthTables.BLOCK.equals(rangeTable) && EthColumns.BLOCKNUMBER.equals(rangeCol)) {
                    try {
                        if (dataMap.get(current.toString()) != null) {
                            keys.add(current.toString());
                        } else {
                            Block block = getBlockByNumber(current.toString());
                            dataMap.put(block.getNumber().toString(), block);
                            keys.add(current.toString());
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                } else if (EthTables.TRANSACTION.equals(rangeTable) && EthColumns.BLOCKNUMBER.equals(rangeCol)) {
                    try {

                        if (blkTxnHashMap.containsKey(String.valueOf(current))) {
                            for (String txnHash : blkTxnHashMap.get(String.valueOf(current)))
                                keys.add(txnHash);

                        } else {
                            List<?> txnList = getTransactions(current.toString());
                            for (Transaction txnInfo : (List<Transaction>) txnList) {
                                dataMap.put(txnInfo.getHash(), txnInfo);
                                keys.add(txnInfo.getHash());
                            }
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query transaction by number " + current, e);
                    }

                }
                current = rangeOps.add(current, 1);
            } while (max.compareTo(current) >= 0);

            return new DataNode<String>(rangeTable, keys);
        }).collect(Collectors.toList());
        DataNode<String> finalDataNode = (DataNode<String>) dataNodes.get(0);
        if (dataNodes.size() > 1) {
            for (int i = 1; i < dataNodes.size(); i++) {
                finalDataNode = mergeDataNodes(finalDataNode, (DataNode<String>) dataNodes.get(i), Operator.OR);
            }
        }
        return (DataNode<String>) finalDataNode;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> TreeNode combineRangeAndDataNodes(RangeNode<T> rangeNode,
        DataNode<?> dataNode, LogicalOperation oper) {
        String tableName = dataNode.getTable();
        List<String> keys = dataNode.getKeys().stream().map(x -> x.toString()).collect(Collectors.toList());
        String rangeCol = rangeNode.getColumn();
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(tableName, rangeCol);
        if (EthTables.BLOCK.equals(tableName)) {
            if (EthColumns.BLOCKNUMBER.equals(rangeCol)) {
                List<RangeNode<T>> dataRanges = keys.stream().map(key -> {
                    Block blockInfo = (Block) dataMap.get(key);
                    if (auxillaryDataMap.containsKey(EthColumns.BLOCKNUMBER)) {
                        auxillaryDataMap.get(EthColumns.BLOCKNUMBER).put(key, blockInfo);
                    } else {
                        auxillaryDataMap.put(EthColumns.BLOCKNUMBER, new HashMap<>());
                        auxillaryDataMap.get(EthColumns.BLOCKNUMBER).put(key, blockInfo);
                    }
                    T blockNo = (T) blockInfo.getNumber();
                    RangeNode<T> node = new RangeNode<>(rangeNode.getTable(), rangeCol);
                    node.getRangeList().addRange(new Range<T>(blockNo, blockNo));
                    return node;
                }).collect(Collectors.toList());
                if (dataRanges.isEmpty() && oper.isAnd()) {
                    return filterRangeNodeWithValue(rangeNode, dataNode);
                }else if (dataRanges.isEmpty() && oper.isOr()) {
                    return rangeNode;
                }
                RangeNode<T> dataRangeNodes = dataRanges.get(0);
                if (dataRanges.size() > 1) {
                    for (int i = 1; i < dataRanges.size(); i++) {
                        dataRangeNodes = rangeOps.rangeNodeOr(dataRangeNodes, dataRanges.get(i));
                    }
                }
                if (oper.isAnd()) {
                    return rangeOps.rangeNodeAnd(dataRangeNodes, rangeNode);
                } else {
                    return rangeOps.rangeNodeOr(dataRangeNodes, rangeNode);
                }
            }
        } else if (EthColumns.BLOCKNUMBER.equals(rangeCol)) {
            if (oper.isOr()) {
                LogicalOperation newOper = new LogicalOperation(Operator.OR);
                newOper.addChildNode(dataNode);
                newOper.addChildNode(rangeNode);
                return newOper;
            } else {
                return filterRangeNodeWithValue(rangeNode, dataNode);
            }
        }
        RangeNode<T> emptyRangeNode = new RangeNode<>(rangeNode.getTable(), rangeNode.getColumn());
        emptyRangeNode.getRangeList().addRange(new Range<T>(rangeOps.getMinValue(), rangeOps.getMinValue()));
        return emptyRangeNode;
    }

    @Override
    protected boolean filterField(String fieldName, Object obj, String value, Comparator comparator) {
        boolean retValue = false;
        if (!comparator.isEQ() && !comparator.isNEQ()) {
            throw new BlkchnException(String.format(
                "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
        }
        if (obj instanceof Block) {
            Block blockInfo = (Block) obj;
            switch (fieldName) {
                case EthColumns.HASH:
                    if (comparator.isEQ()) {
                        retValue = blockInfo.getHash().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !blockInfo.getHash().equals(value.replaceAll("'", ""));
                    }
                    break;
                case EthColumns.PARENTHASH:
                    if (comparator.isEQ()) {
                        retValue = blockInfo.getParentHash().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !blockInfo.getParentHash().equals(value.replaceAll("'", ""));
                    }
                    break;
                case EthColumns.GASLIMIT:
                    if (comparator.isEQ()) {
                        retValue = blockInfo.getGasLimit().toString().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !blockInfo.getGasLimit().toString().equals(value.replaceAll("'", ""));
                    }
                    break;
                case EthColumns.GASUSED:
                    if (comparator.isEQ()) {
                        retValue = blockInfo.getGasUsed().toString().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !blockInfo.getGasUsed().toString().equals(value.replaceAll("'", ""));
                    }
                    break;
            }
        } else if (obj instanceof Transaction) {

            Transaction txnInfo = (Transaction) obj;
            switch (fieldName) {
                case EthColumns.FROM:
                    if (comparator.isEQ()) {
                        retValue = txnInfo.getFrom().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !txnInfo.getFrom().equals(value.replaceAll("'", ""));
                    }
                    break;
                case EthColumns.BLOCKHASH:
                    if (comparator.isEQ()) {
                        retValue = txnInfo.getBlockHash().equals(value.replaceAll("'", ""));
                    } else {
                        retValue = !txnInfo.getBlockHash().equals(value.replaceAll("'", ""));
                    }
                    break;
                case EthColumns.GAS:
                    if (comparator.isEQ()) {
                        retValue = String.valueOf(txnInfo.getGas()).equals(value);
                    } else {
                        retValue = !String.valueOf(txnInfo.getGas()).equals(value);
                    }
                    break;
            }

        }
        return retValue;
    }

    @Override
    protected <T> DataNode<T> filterRangeNodeWithValue(RangeNode<?> rangeNode, DataNode<T> dataNode) {
        List<T> filteredKeys = dataNode.getKeys().stream().filter(key -> {
            if (EthTables.BLOCK.equals(dataNode.getTable()) && EthColumns.BLOCKNUMBER.equals(rangeNode.getColumn())) {
                boolean include = false;
                BigInteger bigIntKey = (BigInteger) key;
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if ((((BigInteger) range.getMin()).compareTo(bigIntKey) == -1
                        && ((BigInteger) range.getMax()).compareTo(bigIntKey) == 1) ||
                            (((BigInteger) range.getMax()).compareTo(bigIntKey) == 0 ||
                                    ((BigInteger) range.getMin()).compareTo(bigIntKey) == 0)) {
                        include = true;
                        break;
                    }
                }
                return include;
            } else if (EthTables.TRANSACTION.equals(dataNode.getTable())
                && EthColumns.BLOCKNUMBER.equals(rangeNode.getColumn())) {
                boolean include = false;
                Transaction transaction = (Transaction) dataMap.get(key);
                BigInteger blockNo = transaction.getBlockNumber();
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if ((((BigInteger) range.getMin()).compareTo(blockNo) == -1
                        && ((BigInteger) range.getMax()).compareTo(blockNo) == 1) ||
                            (((BigInteger) range.getMax()).compareTo(blockNo) == 0 ||
                                    ((BigInteger) range.getMin()).compareTo(blockNo) == 0)) {
                        include = true;
                        break;
                    }
                }
                return include;
            }
            return false;
        }).collect(Collectors.toList());
        return new DataNode<>(dataNode.getTable(), filteredKeys);
    }

    private List<TransactionResult> getTransactions(String blockNumber) throws IOException,Exception {
        LOGGER.info("Getting details of transactions stored in block - " + blockNumber);
        EthBlock block =
            web3jClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)), true).send();

        if(block == null || block.hasError())
            throw new Exception("blockNumber not found : "+blockNumber);

        return block.getBlock().getTransactions();
    }

    private Block getBlockByNumber(String blockNumber) throws IOException,Exception  {
        LOGGER.info("Getting block - " + blockNumber + " Information ");
        EthBlock block =
            web3jClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)), true).send();

        if(block == null || block.hasError())
            throw new Exception("blockNumber not found : "+blockNumber);

        return block.getBlock();
    }

    private Block getBlockByHash(String blockHash) throws IOException,Exception  {
        LOGGER.info("Getting  information of block with hash - " + blockHash);
        EthBlock block = web3jClient.ethGetBlockByHash(blockHash, true).send();

        if(block == null || block.hasError())
            throw new Exception("blockHash not found : "+blockHash);

        return block.getBlock();
    }

    private Transaction getTransactionByHash(String transactionHash) throws IOException,Exception  {
        LOGGER.info("Getting information of Transaction by hash - " + transactionHash);
        Transaction transaction = web3jClient.ethGetTransactionByHash(transactionHash).send().getResult();

        if(transaction == null)
            throw new Exception("blockHash not found : "+transactionHash);

        return transaction;
    }

    private Transaction getTransactionByBlockHashAndIndex(String blockHash, BigInteger transactionIndex)
        throws IOException {
        LOGGER.info("Getting information of Transaction by blockhash - " + blockHash + " and transactionIndex"
            + transactionIndex);

        Transaction transaction =
            web3jClient.ethGetTransactionByBlockHashAndIndex(blockHash, transactionIndex).send().getResult();
        return transaction;
    }

    private BigInteger getBlockHeight() throws IOException {
        LOGGER.info("Getting block height ");
        EthBlockNumber block = web3jClient.ethBlockNumber().send();
        return block.getBlockNumber();
    }

    private Object insertTransaction(String toAddress, String value, String unit, boolean syncRequest)
        throws IOException, CipherException, InterruptedException, TransactionException, ExecutionException {
        if (toAddress == null || value == null || unit == null) {
            LOGGER.error("Check if [toAddress, value, unit] are correctly used in insert query columns");
            throw new BlkchnException("Check if [toAddress, value, unit] are correctly used in insert query columns");
        }
        toAddress = toAddress.replaceAll("'", "");
        value = value.replaceAll("'", "");
        unit = unit.replaceAll("'", "").toUpperCase();
        Object val;
        try {
            if (value.indexOf('.') > 0) {
                val = Double.parseDouble(value);
            } else {
                val = Long.parseLong(value);
            }
        } catch (NumberFormatException e) {
            LOGGER.error("Exception while parsing value", e);
            throw new RuntimeException("Exception while parsing value", e);
        }

        if (properties == null || !properties.containsKey(DriverConstants.KEYSTORE_PASSWORD)
            || !properties.containsKey(DriverConstants.KEYSTORE_PATH)) {
            throw new BlkchnException(
                "Insert query needs keystore path and password, passed as Properties while creating connection");
        }

        Credentials credentials = WalletUtils.loadCredentials(properties.getProperty(DriverConstants.KEYSTORE_PASSWORD),
            properties.getProperty(DriverConstants.KEYSTORE_PATH));
        Object transactionReceipt;
        if (syncRequest) {
            try {
                transactionReceipt = Transfer.sendFunds(web3jClient, credentials, toAddress,
                    BigDecimal.valueOf((val instanceof Long) ? (Long) val : (Double) val), Convert.Unit.valueOf(unit))
                    .send();
            } catch (Exception e) {
                throw new BlkchnException("Exception while making the remote send call", e);
            }
        } else {
            transactionReceipt = Transfer
                .sendFunds(web3jClient, credentials, toAddress,
                    BigDecimal.valueOf((val instanceof Long) ? (Long) val : (Double) val), Convert.Unit.valueOf(unit))
                .sendAsync();
        }

        return transactionReceipt;
    }

    protected DataFrame createDataFrame(DataNode<?> dataNode, String tableName) {
        if (dataNode.getKeys().isEmpty() || dataNode.getKeys().isEmpty()) {
            if (tableName.equals("block")) {
                String[] columns = { EthColumns.BLOCKNUMBER, EthColumns.HASH, EthColumns.PARENTHASH, EthColumns.NONCE,
                        EthColumns.SHA3UNCLES, EthColumns.LOGSBLOOM, EthColumns.TRANSACTIONSROOT, EthColumns.STATEROOT,
                        EthColumns.RECEIPTSROOT, EthColumns.AUTHOR, EthColumns.MINER, EthColumns.MIXHASH,
                        EthColumns.TOTALDIFFICULTY, EthColumns.EXTRADATA, EthColumns.SIZE, EthColumns.GASLIMIT,
                        EthColumns.GASUSED, EthColumns.TIMESTAMP, EthColumns.TRANSACTIONS, EthColumns.UNCLES,
                        EthColumns.SEALFIELDS };
                return new DataFrame(new ArrayList<>(), columns, physicalPlan.getColumnAliasMapping());
            } else if (tableName.equals("transaction")) {
                String columns[] = { EthColumns.BLOCKHASH, EthColumns.BLOCKNUMBER, EthColumns.CREATES, EthColumns.FROM,
                        EthColumns.GAS, EthColumns.GASPRICE, EthColumns.HASH, EthColumns.INPUT, EthColumns.NONCE,
                        EthColumns.PUBLICKEY, EthColumns.R, EthColumns.RAW, EthColumns.S, EthColumns.TO,
                        EthColumns.TRANSACTIONINDEX, EthColumns.V, EthColumns.VALUE };
                return new DataFrame(new ArrayList<>(), columns, physicalPlan.getColumnAliasMapping());
            } else
                return new DataFrame(new ArrayList<>(), new ArrayList<>(), physicalPlan.getColumnAliasMapping());
        }
        DataFrame df = null;
        List<List<Object>> data = new ArrayList<>();
        if (dataMap.get(dataNode.getKeys().get(0).toString()) instanceof Block) {
            String[] columns = { EthColumns.BLOCKNUMBER, EthColumns.HASH, EthColumns.PARENTHASH, EthColumns.NONCE,
                EthColumns.SHA3UNCLES, EthColumns.LOGSBLOOM, EthColumns.TRANSACTIONSROOT, EthColumns.STATEROOT,
                EthColumns.RECEIPTSROOT, EthColumns.AUTHOR, EthColumns.MINER, EthColumns.MIXHASH,
                EthColumns.TOTALDIFFICULTY, EthColumns.EXTRADATA, EthColumns.SIZE, EthColumns.GASLIMIT,
                EthColumns.GASUSED, EthColumns.TIMESTAMP, EthColumns.TRANSACTIONS, EthColumns.UNCLES,
                EthColumns.SEALFIELDS };

            for (Object key : dataNode.getKeys()) {
                Block blockInfo = (Block) dataMap.get(key.toString());
                BigInteger blocknumber = blockInfo.getNumber();
                String hash = blockInfo.getHash();
                String parenthash = blockInfo.getParentHash();
                BigInteger nonce = blockInfo.getNonce();
                String sha3uncles = blockInfo.getSha3Uncles();
                String logsbloom = blockInfo.getLogsBloom();
                String transactionsroot = blockInfo.getTransactionsRoot();
                String stateroot = blockInfo.getStateRoot();
                String receiptsroot = blockInfo.getReceiptsRoot();
                String author = blockInfo.getAuthor();
                String miner = blockInfo.getMiner();
                String mixhash = blockInfo.getMixHash();
                BigInteger totaldifficulty = blockInfo.getTotalDifficulty();
                String extradata = blockInfo.getExtraData();
                BigInteger size = blockInfo.getSize();
                BigInteger gaslimit = blockInfo.getGasLimit();
                BigInteger gasused = blockInfo.getGasUsed();
                BigInteger timestamp = blockInfo.getTimestamp();
                List<TransactionResult> transactions = blockInfo.getTransactions();
                List<String> uncles = blockInfo.getUncles();
                List<String> sealfields = blockInfo.getSealFields();
                data.add(Arrays.asList(blocknumber, hash, parenthash, nonce, sha3uncles, logsbloom, transactionsroot,
                    stateroot, receiptsroot, author, miner, mixhash, totaldifficulty, extradata, size, gaslimit,
                    gasused, timestamp, transactions, uncles, sealfields));
            }
            df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        } else if (dataMap.get(dataNode.getKeys().get(0).toString()) instanceof Transaction) {
            String columns[] = { EthColumns.BLOCKHASH, EthColumns.BLOCKNUMBER, EthColumns.CREATES, EthColumns.FROM,
                EthColumns.GAS, EthColumns.GASPRICE, EthColumns.HASH, EthColumns.INPUT, EthColumns.NONCE,
                EthColumns.PUBLICKEY, EthColumns.R, EthColumns.RAW, EthColumns.S, EthColumns.TO,
                EthColumns.TRANSACTIONINDEX, EthColumns.V, EthColumns.VALUE };
            for (Object key : dataNode.getKeys()) {
                Transaction txnInfo = (Transaction) dataMap.get(key.toString());
                String blockhash = txnInfo.getBlockHash();
                BigInteger blocknumber = txnInfo.getBlockNumber();
                String creates = txnInfo.getCreates();
                String from = txnInfo.getFrom();
                String gas = txnInfo.getGas().toString();
                BigInteger gasprice = txnInfo.getGasPrice();
                String hash = txnInfo.getHash();
                String input = txnInfo.getInput();
                BigInteger nonce = txnInfo.getNonce();
                String publickey = txnInfo.getPublicKey();
                String r = txnInfo.getR();
                String raw = txnInfo.getRaw();
                String s = txnInfo.getS();
                String to = txnInfo.getTo();
                BigInteger transactionindex = txnInfo.getTransactionIndex();
                String v = String.valueOf(txnInfo.getV());
                BigInteger value = txnInfo.getValue();
                data.add(Arrays.asList(blockhash, blocknumber, creates, from, gas, gasprice, hash, input, nonce,
                    publickey, r, raw, s, to, transactionindex, v, value));
            }
            df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        } else
            throw new BlkchnException("Cannot create dataframe from unknown object type");
    }

    public Boolean execute() {
        try {
            executeAndReturn();
        } catch (Exception e) {
            return false;
        }
        return true;
    }

    public Object executeAndReturn() {
        // get values from logical plan and pass it to insertTransaction method
        Insert insert = logicalPlan.getInsert();
        String tableName = insert.getChildType(Table.class).get(0).getChildType(IdentifierNode.class, 0).getValue();
        if (!EthTables.TRANSACTION.equalsIgnoreCase(tableName)) {
            throw new BlkchnException("Please give valid table name in insert query. Expected: transaction");
        }
        ColumnName names = insert.getChildType(ColumnName.class).get(0);
        ColumnValue values = insert.getChildType(ColumnValue.class).get(0);
        Map<String, String> namesMap = new HashMap<String, String>();
        namesMap.put(names.getChildType(IdentifierNode.class, 0).getValue(),
            values.getChildType(IdentifierNode.class, 0).getValue());
        namesMap.put(names.getChildType(IdentifierNode.class, 1).getValue(),
            values.getChildType(IdentifierNode.class, 1).getValue());
        namesMap.put(names.getChildType(IdentifierNode.class, 2).getValue(),
            values.getChildType(IdentifierNode.class, 2).getValue());
        if (names.getChildType(IdentifierNode.class, 3) != null
            && values.getChildType(IdentifierNode.class, 3) != null) {
            namesMap.put(names.getChildType(IdentifierNode.class, 3).getValue(),
                values.getChildType(IdentifierNode.class, 3).getValue());
        }
        boolean async = namesMap.get(ASYNC) == null ? true : Boolean.parseBoolean(namesMap.get(ASYNC));
        Object result = null;
        try {
            result =
                insertTransaction(namesMap.get(TO_ADDRESS), namesMap.get(EthColumns.VALUE), namesMap.get(UNIT), !async);
        } catch (IOException | CipherException | InterruptedException | TransactionException | ExecutionException e) {
            e.printStackTrace();
            throw new BlkchnException("Error while executing query", e);
        }
        return result;
    }

    public Object executeFunction() {
        TreeNode callFunc = logicalPlan.getCallFunction();
        List<Object> args = new ArrayList<>();
        List<Class> argsType = new ArrayList<>();
        String className;
        String smartContractAdd;
        boolean isValid = false;
        boolean async = false;
        Object result;
        String functionName = callFunc.getChildType(IdentifierNode.class, 0).getValue();
        if (callFunc.hasChildType(SmartContractFunction.class)) {
            Parameters params = callFunc.getChildType(Parameters.class, 0);
            if (params != null)
                getParametersTypeValue(params, args, argsType);
            SmartContractFunction smf = callFunc.getChildType(SmartContractFunction.class, 0);
            if (smf.hasChildType(SmartCnrtClassOption.class) && smf.hasChildType(SmartCnrtAddressOption.class)) {
                className = Utilities.unquote(
                    ((ClassName) smf.getChildType(SmartCnrtClassOption.class, 0).getChildType(ClassName.class, 0))
                        .getName());
                smartContractAdd =
                    Utilities.unquote(smf.getChildType(SmartCnrtAddressOption.class, 0).getAddressOption());
            } else {
                throw new BlkchnException("Query is incomplete needs CLASS and ADDRESS");
            }
            isValid = smf.hasChildType(SmartCnrtIsValidFlag.class);
            if (smf.hasChildType(SmartCnrtAsyncOption.class)) {
                async = smf.getChildType(SmartCnrtAsyncOption.class, 0).getAsyncOption().equalsIgnoreCase("TRUE");
            }
        } else {
            throw new BlkchnException("Query is incomplete needs CLASS and ADDRESS");
        }

        try {
            result = functionTransaction(functionName, args.toArray(), argsType.toArray(new Class[0]), className,
                smartContractAdd, async, isValid);
        } catch (Exception e) {
            throw new BlkchnException("Error while executing query", e);
        }
        return result;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Object functionTransaction(String functionName, Object[] argument, Class[] argClass, String className,
        String address, boolean async, boolean isValid) {
        try {
            Class smartContractClass = Class.forName(className);
            if (smartCrtClassObjectMap.containsKey(smartContractClass)
                && smartCrtClassObjectMap.get(smartContractClass) != null)
                classObjectSmartCrt = smartCrtClassObjectMap.get(smartContractClass);
            else
                classObjectSmartCrt = getLoadClass(smartContractClass, address, isValid);
            Method functionToInvoke = smartContractClass.getDeclaredMethod(functionName, argClass);
            Object value = functionToInvoke.invoke(classObjectSmartCrt, argument);

            if (functionToInvoke.getReturnType().equals(RemoteCall.class)) {
                RemoteCall<?> transactionR = (RemoteCall) value;
                if (async)
                    return transactionR.sendAsync();
                else
                    return transactionR.send();
            } else {
                return value;
            }
        } catch (Exception e) {
            throw new BlkchnException("Error while running function transaction", e);
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public Object executeDeploy() {
        TreeNode smartCnrt = logicalPlan.getSmartCnrtDeploy();
        boolean async = false;
        List<Object> args = new ArrayList<>();
        List<Class> argsType = new ArrayList<>();
        if (smartCnrt.hasChildType(ClassName.class)) {
            String className = Utilities.unquote(((ClassName) smartCnrt.getChildType(ClassName.class, 0)).getName());
            Credentials credentials = getCredential();
            try {
                Class smartContractClass = Class.forName(className);
                argsType.add(Web3j.class);
                args.add(web3jClient);
                argsType.add(Credentials.class);
                args.add(credentials);
                argsType.add(BigInteger.class);
                args.add(GAS_PRICE);
                argsType.add(BigInteger.class);
                args.add(GAS);
                Parameters params = smartCnrt.getChildType(Parameters.class, 0);
                if (params != null)
                    getParametersTypeValue(params, args, argsType);
                Method loadMethod = smartContractClass.getDeclaredMethod("deploy", argsType.toArray(new Class[0]));
                RemoteCall objSmartCntr = (RemoteCall) loadMethod.invoke(smartContractClass, args.toArray());
                Method getContractAddress = smartContractClass.getMethod("getContractAddress");
                if (smartCnrt.hasChildType(SmartCnrtAsyncOption.class)) {
                    async =
                        smartCnrt.getChildType(SmartCnrtAsyncOption.class, 0).getAsyncOption().equalsIgnoreCase("TRUE");
                }
                if (async) {
                    return CompletableFuture.supplyAsync(() -> {
                        try {
                            return getContractAddress.invoke(objSmartCntr.send());
                        } catch (Exception e) {
                            throw new BlkchnException(e);
                        }
                    });
                } else {
                    return getContractAddress.invoke(objSmartCntr.send());
                }
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
                throw new BlkchnException("Smart Contract Class not found ", e);
            } catch (Exception e) {
                e.printStackTrace();
                throw new BlkchnException("Error while deploying smart contract " + e.getMessage(), e);
            }
        } else {
            throw new BlkchnException("Should give classpath for deploying Smart Contract");
        }
    }

    private Credentials getCredential() {
        if (properties == null || !properties.containsKey(DriverConstants.KEYSTORE_PASSWORD)
            || !properties.containsKey(DriverConstants.KEYSTORE_PATH)) {
            throw new BlkchnException(
                "Query needs keystore path and password, passed as Properties while creating connection");
        }
        try {
            return WalletUtils.loadCredentials(properties.getProperty(DriverConstants.KEYSTORE_PASSWORD),
                properties.getProperty(DriverConstants.KEYSTORE_PATH));
        } catch (Exception e) {
            throw new BlkchnException("Check your KEYSTORE credentials ", e);
        }
    }

    private void getParametersTypeValue(Parameters params, List<Object> args, List<Class> argsType) {
        for (int i = 0; i < params.getChildNodes().size(); i++) {
            if (params.getChildNode(i) instanceof IdentifierNode) {
                IdentifierNode ident = (IdentifierNode) params.getChildNode(i);
                FunctionUtil.handleIdent(ident, args, argsType);
            } else if (params.getChildNode(i) instanceof ListAgrs) {
                ListAgrs lstArgs = (ListAgrs) params.getChildNode(i);
                FunctionUtil.handleList(lstArgs, args, argsType);
            } else if (params.getChildNode(i) instanceof BytesArgs) {
                String hexValue = ((BytesArgs) params.getChildNode(i)).getValue();
                FunctionUtil.handleHex(hexValue, args, argsType);
            } else {
                throw new BlkchnException("Cannot create method call from unknown Data type");
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private Object getLoadClass(Class smartContractClass, String address, boolean isValid) {
        try {
            Class params[] = { String.class, Web3j.class, Credentials.class, BigInteger.class, BigInteger.class };
            Credentials credentials = getCredential();
            Method loadMethod = smartContractClass.getDeclaredMethod("load", params);
            Object classObject =
                loadMethod.invoke(smartContractClass, address, web3jClient, credentials, GAS_PRICE, GAS);
            if(isValid) {
                Method checkValidMethod = smartContractClass.getMethod("isValid");
                Boolean valid = (Boolean) checkValidMethod.invoke(classObject);
                if (!valid)
                    throw new BlkchnException("SmartContract is not valid");
            }
            smartCrtClassObjectMap.put(smartContractClass, classObject);
            return classObject;
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
            throw new BlkchnException(e);
        }
    }

}
