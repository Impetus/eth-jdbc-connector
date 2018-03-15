package com.impetus.eth.parser;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.crypto.CipherException;
import org.web3j.crypto.Credentials;
import org.web3j.crypto.WalletUtils;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.Block;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.core.methods.response.EthBlockNumber;
import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.exceptions.TransactionTimeoutException;
import org.web3j.tx.Transfer;
import org.web3j.utils.Convert;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkch.sql.DataFrame;
import com.impetus.blkch.sql.GroupedDataFrame;
import com.impetus.blkch.sql.insert.ColumnName;
import com.impetus.blkch.sql.insert.ColumnValue;
import com.impetus.blkch.sql.insert.Insert;
import com.impetus.blkch.sql.parser.AbstractQueryExecutor;
import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.TreeNode;
import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.Comparator;
import com.impetus.blkch.sql.query.DataNode;
import com.impetus.blkch.sql.query.DirectAPINode;
import com.impetus.blkch.sql.query.FromItem;
import com.impetus.blkch.sql.query.GroupByClause;
import com.impetus.blkch.sql.query.HavingClause;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.LimitClause;
import com.impetus.blkch.sql.query.LogicalOperation;
import com.impetus.blkch.sql.query.LogicalOperation.Operator;
import com.impetus.blkch.sql.query.OrderByClause;
import com.impetus.blkch.sql.query.OrderItem;
import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.blkch.sql.query.Table;
import com.impetus.blkch.util.Range;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.eth.jdbc.DriverConstants;

public class EthQueryExecutor extends AbstractQueryExecutor
{
    
    private static final Logger LOGGER = LoggerFactory.getLogger(EthQueryExecutor.class);
    
    private Web3j web3jClient;
    
    private Properties properties;
    
    public EthQueryExecutor(LogicalPlan logicalPlan, Web3j web3jClient, Properties properties)
    {
       this.logicalPlan = logicalPlan;
       this.web3jClient = web3jClient;
       this.properties = properties;
       this.physicalPlan = new EthPhysicalPlan(logicalPlan);
    }
    
    public DataFrame executeQuery() {
        physicalPlan.getWhereClause().traverse();
        if (!physicalPlan.validateLogicalPlan()) {
            throw new BlkchnException("This query can't be executed");
        }
        DataFrame dataframe = getFromTable();
        if(dataframe.isEmpty()) {
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
            if(logicalPlan.getQuery().hasChildType(HavingClause.class)) {
                afterSelect = groupedDF.having(logicalPlan.getQuery().getChildType(HavingClause.class, 0)).select(physicalPlan.getSelectItems());
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

    private DataFrame getFromTable() {
        Table table = logicalPlan.getQuery().getChildType(FromItem.class, 0).getChildType(Table.class, 0);
        String tableName = table.getChildType(IdentifierNode.class, 0).getValue();
        if (physicalPlan.getWhereClause() != null) {
            DataNode<?> finalData;
            if (physicalPlan.getWhereClause().hasChildType(LogicalOperation.class)) {
                TreeNode directAPIOptimizedTree = executeDirectAPIs(tableName, physicalPlan.getWhereClause()
                        .getChildType(LogicalOperation.class, 0));
                TreeNode optimizedTree = optimize(directAPIOptimizedTree);
                 finalData = execute(optimizedTree);
            } else if (physicalPlan.getWhereClause().hasChildType(DirectAPINode.class)) {
                DirectAPINode node = physicalPlan.getWhereClause().getChildType(DirectAPINode.class, 0);
                finalData = getDataNode(node.getTable(), node.getColumn(), node.getValue());
            } else {
                RangeNode<?> rangeNode = physicalPlan.getWhereClause().getChildType(RangeNode.class, 0);
                System.out.println("-- Executing Range Node");
                finalData = executeRangeNode(rangeNode);
                System.out.println("DataNode: ");
                finalData.traverse();
            }
            return createDataFrame(finalData);
        } else {
            throw new BlkchnException("Can't query without where clause. Data will be huge");
        }

    }

    @Override
    protected DataNode<?> getDataNode(String table, String column, String value)
    {
        if (dataMap.containsKey(value)) {
            return new DataNode<>(table, Arrays.asList(value));
        }
        Block block = null;
        if (table.equals("block") && column.equals("blocknumber")) {
            try {
                block = getBlockByNumber(value);
            } catch (Exception e) {
                throw new BlkchnException("Error querying block by number " + value, e);
            }
        } else if (table.equals("block") && column.equals("blockhash")) {
            try {
                block = getBlockByHash(value);
            } catch (Exception e) {
                throw new BlkchnException("Error querying block by hash " + value.replace("'", ""), e);
            }
        } else if (table.equals("transaction") && column.equals("hash")) {
            try {
                //TODO: check for unique representation of transaction with blocknumber
                Transaction transaction = getTransactionByHash(value);
                dataMap.put(block.getNumber().toString(), block);
                return new DataNode<>(table, Arrays.asList(block.getNumber().toString()));
            } catch (Exception e) {
                throw new BlkchnException("Error querying block by hash " + value.replace("'", ""), e);
            }
        } 
        if(block == null){
            throw new BlkchnException(String.format("There is no direct API for table %s and column %s combination",
                    table, column));
        }
        dataMap.put(block.getNumber().toString(), block);
        return new DataNode<>(table, Arrays.asList(block.getNumber().toString()));
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> DataNode<T> executeRangeNode(RangeNode<T> rangeNode)
    {
        if(rangeNode.getRangeList().getRanges().isEmpty()) {
            return new DataNode<>(rangeNode.getTable(), new ArrayList<>());
        }
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(rangeNode.getTable(),
                rangeNode.getColumn());
        String rangeCol = rangeNode.getColumn();
        String rangeTable = rangeNode.getTable();
        BigInteger height;
        try {
            height = getBlockHeight();
        } catch (Exception e) {
            throw new BlkchnException("Error getting height of ledger", e);
        }
        List<DataNode<T>> dataNodes = rangeNode.getRangeList().getRanges().stream().map(range -> {
            List<T> keys = new ArrayList<>();
            T current = range.getMin().equals(rangeOps.getMinValue()) ? (T) new BigInteger("0") : range.getMin();
            T max = range.getMax().equals(rangeOps.getMaxValue()) ? (T) rangeOps.subtract((T) height, 1) : range.getMax();
            do {
                if ("block".equals(rangeTable) && "blocknumber".equals(rangeCol)) {
                    try {
                        if (dataMap.get(current.toString()) != null) {
                            keys.add(current);
                        } else {
                            Block block = getBlockByNumber(current.toString());
                            dataMap.put(block.getNumber().toString(), block);
                            keys.add(current);
                        }
                    } catch (Exception e) {
                        throw new BlkchnException("Error query block by number " + current, e);
                    }
                }
                current = rangeOps.add(current, 1);
            } while (max.compareTo(current) >= 0);
            return new DataNode<>(rangeTable, keys);
        }).collect(Collectors.toList());
        DataNode<T> finalDataNode = dataNodes.get(0);
        if (dataNodes.size() > 1) {
            for (int i = 1; i < dataNodes.size(); i++) {
                finalDataNode = mergeDataNodes(finalDataNode, dataNodes.get(i), Operator.OR);
            }
        }
        return finalDataNode;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected <T extends Number & Comparable<T>> RangeNode<T> combineRangeAndDataNodes(RangeNode<T> rangeNode,
            DataNode<?> dataNode, LogicalOperation oper)
    {
        String tableName = dataNode.getTable();
        List<String> keys = dataNode.getKeys().stream().map(x -> x.toString()).collect(Collectors.toList());
        String rangeCol = rangeNode.getColumn();
        RangeOperations<T> rangeOps = (RangeOperations<T>) physicalPlan.getRangeOperations(tableName, rangeCol);
        if ("block".equals(tableName)) {
            if ("blocknumber".equals(rangeCol)) {
                List<RangeNode<T>> dataRanges = keys.stream().map(key -> {
                    Block blockInfo = (Block) dataMap.get(key);
                    if (auxillaryDataMap.containsKey("blocknumber")) {
                        auxillaryDataMap.get("blocknumber").put(key, blockInfo);
                    } else {
                        auxillaryDataMap.put("blocknumber", new HashMap<>());
                        auxillaryDataMap.get("blocknumber").put(key, blockInfo);
                    }
                    T blockNo = (T) blockInfo.getNumber();
                    RangeNode<T> node = new RangeNode<>(rangeNode.getTable(), rangeCol);
                    node.getRangeList().addRange(new Range<T>(blockNo, blockNo));
                    return node;
                }).collect(Collectors.toList());
                if(dataRanges.isEmpty()){
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
        }
        RangeNode<T> emptyRangeNode = new RangeNode<>(rangeNode.getTable(), rangeNode.getColumn());
        emptyRangeNode.getRangeList().addRange(new Range<T>(rangeOps.getMinValue(), rangeOps.getMinValue()));
        return emptyRangeNode;
    }

    @Override
    protected boolean filterField(String fieldName, Object obj, String value, Comparator comparator)
    {
        boolean retValue = false;
        if (!comparator.isEQ() && !comparator.isNEQ()) {
            throw new BlkchnException(String.format(
                    "String values in %s field can only be compared for equivalence and non-equivalence", fieldName));
        }
        if (obj instanceof Block) {
            Block blockInfo = (Block) obj;
            switch(fieldName){
            case "hash":
                if(comparator.isEQ()) {
                    retValue = blockInfo.getHash().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !blockInfo.getHash().equals(value.replaceAll("'", ""));
                }
                break;
            case "parenthash":
                if(comparator.isEQ()) {
                    retValue = blockInfo.getParentHash().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !blockInfo.getParentHash().equals(value.replaceAll("'", ""));
                }
                break;
            case "gaslimit":
                if(comparator.isEQ()) {
                    retValue = blockInfo.getGasLimit().toString().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !blockInfo.getGasLimit().toString().equals(value.replaceAll("'", ""));
                }
                break;
            case "gasused":
                if(comparator.isEQ()) {
                    retValue = blockInfo.getGasUsed().toString().equals(value.replaceAll("'", ""));
                } else {
                    retValue = !blockInfo.getGasUsed().toString().equals(value.replaceAll("'", ""));
                }
                break;
            }
        }
        return retValue;
    }

    @Override
    protected <T> DataNode<T> filterRangeNodeWithValue(RangeNode<?> rangeNode, DataNode<T> dataNode)
    {
        List<T> filteredKeys = dataNode.getKeys().stream().filter(key -> {
            if ("block".equals(dataNode.getTable()) && "blocknumber".equals(rangeNode.getColumn())) {
                boolean include = false;
                Long longKey = (Long) key;
                for (Range<?> range : rangeNode.getRangeList().getRanges()) {
                    if (((Long) range.getMin()) <= longKey && ((Long) range.getMax()) >= longKey) {
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
    
    
    private List<TransactionResult> getTransactions(String blockNumber) throws IOException {
        LOGGER.info("Getting details of transactions stored in block - " + blockNumber);
        EthBlock block = web3jClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)),
                true).send();

        return block.getBlock().getTransactions();
    }

    private Block getBlockByNumber(String blockNumber) throws IOException {
        LOGGER.info("Getting block - " + blockNumber + " Information ");
        EthBlock block = web3jClient.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)),
                true).send();
        return block.getBlock();
    }

    private Block getBlockByHash(String blockHash) throws IOException {
        LOGGER.info("Getting  information of block with hash - " + blockHash);
        EthBlock block = web3jClient.ethGetBlockByHash(blockHash, true).send();
        return block.getBlock();
    }

    private Transaction getTransactionByHash(String transactionHash) throws IOException {
        LOGGER.info("Getting information of Transaction by hash - " + transactionHash);

        Transaction transaction = web3jClient.ethGetTransactionByHash(transactionHash).send().getResult();
        return transaction;
    }

    private Transaction getTransactionByBlockHashAndIndex(String blockHash, BigInteger transactionIndex)
            throws IOException {
        LOGGER.info("Getting information of Transaction by blockhash - " + blockHash + " and transactionIndex"
                + transactionIndex);

        Transaction transaction = web3jClient.ethGetTransactionByBlockHashAndIndex(blockHash, transactionIndex).send()
                .getResult();
        return transaction;
    }
    
    private BigInteger getBlockHeight() throws IOException {
        LOGGER.info("Getting block height ");
        EthBlockNumber block = web3jClient.ethBlockNumber().send();
        return block.getBlockNumber();
    }
    
    private Object insertTransaction(String toAddress, String value, String unit, boolean syncRequest)
            throws IOException, CipherException, InterruptedException, TransactionTimeoutException, ExecutionException
    {
        toAddress = toAddress.replaceAll("'", "");
        value = value.replaceAll("'", "");
        unit = unit.replaceAll("'", "").toUpperCase();
        Object val;
        try
        {
            if (value.indexOf('.') > 0)
            {
                val = Double.parseDouble(value);
            }
            else
            {
                val = Long.parseLong(value);
            }
        }
        catch (NumberFormatException e)
        {
            LOGGER.error("Exception while parsing value", e);
            throw new RuntimeException("Exception while parsing value", e);
        }
        Credentials credentials = WalletUtils.loadCredentials(
                properties.getProperty(DriverConstants.KEYSTORE_PASSWORD),
                properties.getProperty(DriverConstants.KEYSTORE_PATH));
        Object transactionReceipt;
        if (syncRequest)
        {
            transactionReceipt = Transfer.sendFunds(web3jClient, credentials, toAddress,
                    BigDecimal.valueOf((val instanceof Long) ? (Long) val : (Double) val), Convert.Unit.valueOf(unit));
        }
        else
        {
            transactionReceipt = Transfer.sendFundsAsync(web3jClient, credentials,
                    toAddress, BigDecimal.valueOf((val instanceof Long) ? (Long) val : (Double) val),
                    Convert.Unit.valueOf(unit));
        }

       
        return transactionReceipt;
    }
    
    protected DataFrame createDataFrame(DataNode<?> dataNode) {
        if(dataNode.getKeys().isEmpty()) {
            return new DataFrame(new ArrayList<>(), new ArrayList<>(), physicalPlan.getColumnAliasMapping());
        }
        if (dataMap.get(dataNode.getKeys().get(0).toString()) instanceof Block) {
            String[] columns = { "blocknumber", "hash", "parenthash", "nonce", "sha3uncles", "logsbloom",
                    "transactionsroot", "stateroot", "receiptsroot", "author", "miner", "mixhash", "totaldifficulty",
                    "extradata", "size", "gaslimit", "gasused", "timestamp", "transactions", "uncles", "sealfields" };
            List<List<Object>> data = new ArrayList<>();
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
            DataFrame df = new DataFrame(data, columns, physicalPlan.getColumnAliasMapping());
            df.setRawData(dataMap.values());
            return df;
        }
        throw new BlkchnException("Cannot create dataframe from unknown object type");
    }

    public Boolean execute()
    {
        try
        {
            executeAndReturn();
        }
        catch (Exception e)
        {
            return false;
        }
        return true;
    }

    public Object executeAndReturn()
    {
        // get values from logical plan and pass it to insertTransaction method
        Insert insert = logicalPlan.getInsert();
        String tableName = insert.getChildType(Table.class).get(0).getChildType(IdentifierNode.class, 0).getValue();
        if(!"transaction".equalsIgnoreCase(tableName)){
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
        if (names.getChildType(IdentifierNode.class, 3) != null && values.getChildType(IdentifierNode.class, 3) != null)
        {
            namesMap.put(names.getChildType(IdentifierNode.class, 3).getValue(),
                    values.getChildType(IdentifierNode.class, 3).getValue());
        }
        boolean async = namesMap.get("async") == null ? true : Boolean.parseBoolean(namesMap.get("async"));
        Object result = null;
        try
        {
            result = insertTransaction(namesMap.get("toAddress"), namesMap.get("value"), namesMap.get("unit"), !async);
        }
        catch (IOException | CipherException | InterruptedException | TransactionTimeoutException | ExecutionException e)
        {
            e.printStackTrace();
            throw new RuntimeException("Error while executing query", e);
        }
        return result;
    }

}
