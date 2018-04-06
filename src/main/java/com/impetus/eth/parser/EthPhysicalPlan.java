package com.impetus.eth.parser;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.PhysicalPlan;
import com.impetus.blkch.util.BigIntegerRangeOperations;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.blkch.util.Tuple2;
import com.impetus.eth.query.EthColumns;
import com.impetus.eth.query.EthTables;

public class EthPhysicalPlan extends PhysicalPlan {
    
    public static final String DESCRIPTION = "ETHEREUM_PHYSICAL_PLAN";
    
    private static Map<String, List<String>> rangeColMap = new HashMap<>();
    
    private static Map<String, List<String>> queryColMap = new HashMap<>();
    
    private static List<String> ethTables = Arrays.asList(EthTables.BLOCK, EthTables.TRANSACTION);
    
    private static Map<String, List<String>> ethTableColumnMap = new HashMap<>();
    
    private static Map<Tuple2<String, String>, RangeOperations<?>> rangeOpMap = new HashMap<>();
    
    static {
        rangeColMap.put(EthTables.BLOCK, Arrays.asList(EthColumns.BLOCKNUMBER));
        rangeColMap.put(EthTables.TRANSACTION, Arrays.asList(EthColumns.BLOCKNUMBER));
        
        queryColMap.put(EthTables.BLOCK, Arrays.asList(EthColumns.BLOCKHASH));
        queryColMap.put(EthTables.TRANSACTION, Arrays.asList(EthColumns.HASH));
        
        rangeOpMap.put(new Tuple2<>(EthTables.BLOCK, EthColumns.BLOCKNUMBER), new BigIntegerRangeOperations());
        rangeOpMap.put(new Tuple2<>(EthTables.TRANSACTION, EthColumns.BLOCKNUMBER), new BigIntegerRangeOperations());
        
        ethTableColumnMap.put(EthTables.BLOCK, Arrays.asList(EthColumns.BLOCKNUMBER, EthColumns.HASH, EthColumns.PARENTHASH, EthColumns.NONCE, EthColumns.SHA3UNCLES, EthColumns.LOGSBLOOM,
                    EthColumns.TRANSACTIONSROOT, EthColumns.STATEROOT, EthColumns.RECEIPTSROOT, EthColumns.AUTHOR, EthColumns.MINER, EthColumns.MIXHASH, EthColumns.TOTALDIFFICULTY,
                    EthColumns.EXTRADATA, EthColumns.SIZE, EthColumns.GASLIMIT, EthColumns.GASUSED, EthColumns.TIMESTAMP, EthColumns.TRANSACTIONS, EthColumns.UNCLES, EthColumns.SEALFIELDS));
        
        ethTableColumnMap.put(EthTables.TRANSACTION, Arrays.asList(EthColumns.BLOCKHASH, EthColumns.BLOCKNUMBER, EthColumns.CREATES, EthColumns.FROM, EthColumns.GAS, EthColumns.GASPRICE, EthColumns.HASH, EthColumns.INPUT,
                    EthColumns.NONCE, EthColumns.PUBLICKEY, EthColumns.R, EthColumns.RAW, EthColumns.S, EthColumns.TO, EthColumns.TRANSACTIONINDEX, EthColumns.V, EthColumns.VALUE));
    }

    public EthPhysicalPlan(LogicalPlan logicalPlan) {
        super(DESCRIPTION, logicalPlan);
    }

    @Override
    public List<String> getRangeCols(String table) {
        return rangeColMap.get(table);
    }

    @Override
    public List<String> getQueryCols(String table) {
        return queryColMap.get(table);
    }

    @Override
    public RangeOperations<?> getRangeOperations(String table, String column) {
        return rangeOpMap.get(new Tuple2<>(table, column));
    }

    @Override
    public boolean tableExists(String table) {
        return ethTables.contains(table);
    }

    @Override
    public boolean columnExists(String table, String column) {
        if (!ethTableColumnMap.containsKey(table)) {
            return false;
        }
        return ethTableColumnMap.get(table).contains(column);
    }

    static Map<String, List<String>> getEthTableColumnMap() {
        return ethTableColumnMap;
    }
}