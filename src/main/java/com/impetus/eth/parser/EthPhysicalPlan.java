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

public class EthPhysicalPlan extends PhysicalPlan {
    
    public static final String DESCRIPTION = "ETHEREUM_PHYSICAL_PLAN";
    
    private static Map<String, List<String>> rangeColMap = new HashMap<>();
    
    private static Map<String, List<String>> queryColMap = new HashMap<>();
    
    private static Map<Tuple2<String, String>, RangeOperations<?>> rangeOpMap = new HashMap<>();
    
    static {
        rangeColMap.put("block", Arrays.asList("blocknumber"));
        queryColMap.put("block", Arrays.asList("blockhash"));
        queryColMap.put("transaction", Arrays.asList("hash"));
        
        rangeOpMap.put(new Tuple2<>("block", "blocknumber"), new BigIntegerRangeOperations());
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
}