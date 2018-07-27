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

import java.math.BigInteger;
import java.sql.Types;
import java.util.*;

import com.impetus.blkch.sql.parser.LogicalPlan;
import com.impetus.blkch.sql.parser.PhysicalPlan;
import com.impetus.blkch.sql.query.*;
import com.impetus.blkch.util.BigIntegerRangeOperations;
import com.impetus.blkch.util.RangeOperations;
import com.impetus.blkch.util.Tuple2;
import com.impetus.blkch.util.Utilities;
import com.impetus.eth.query.EthColumns;
import com.impetus.eth.query.EthTables;

public class EthPhysicalPlan extends PhysicalPlan {

    public static final String DESCRIPTION = "ETHEREUM_PHYSICAL_PLAN";

    private static Map<String, List<String>> rangeColMap = new HashMap<>();

    private static Map<String, List<String>> queryColMap = new HashMap<>();

    private static List<String> ethTables = Arrays.asList(EthTables.BLOCK, EthTables.TRANSACTION);

    private static Map<String, List<String>> ethTableColumnMap = new HashMap<>();

    private static Map<Tuple2<String, String>, RangeOperations<?>> rangeOpMap = new HashMap<>();

    private static Map ethTableTypeMap = new HashMap<String, Map>();

    static {
        rangeColMap.put(EthTables.BLOCK, Arrays.asList(EthColumns.BLOCKNUMBER));
        rangeColMap.put(EthTables.TRANSACTION, Arrays.asList(EthColumns.BLOCKNUMBER));

        queryColMap.put(EthTables.BLOCK, Arrays.asList(EthColumns.HASH));
        queryColMap.put(EthTables.TRANSACTION, Arrays.asList(EthColumns.HASH));

        rangeOpMap.put(new Tuple2<>(EthTables.BLOCK, EthColumns.BLOCKNUMBER), new BigIntegerRangeOperations());
        rangeOpMap.put(new Tuple2<>(EthTables.TRANSACTION, EthColumns.BLOCKNUMBER), new BigIntegerRangeOperations());

        ethTableColumnMap.put(EthTables.BLOCK,
            Arrays.asList(EthColumns.BLOCKNUMBER, EthColumns.HASH, EthColumns.PARENTHASH, EthColumns.NONCE,
                EthColumns.SHA3UNCLES, EthColumns.LOGSBLOOM, EthColumns.TRANSACTIONSROOT, EthColumns.STATEROOT,
                EthColumns.RECEIPTSROOT, EthColumns.AUTHOR, EthColumns.MINER, EthColumns.MIXHASH,
                EthColumns.TOTALDIFFICULTY, EthColumns.EXTRADATA, EthColumns.SIZE, EthColumns.GASLIMIT,
                EthColumns.GASUSED, EthColumns.TIMESTAMP, EthColumns.TRANSACTIONS, EthColumns.UNCLES,
                EthColumns.SEALFIELDS));

        ethTableColumnMap.put(EthTables.TRANSACTION,
            Arrays.asList(EthColumns.BLOCKHASH, EthColumns.BLOCKNUMBER, EthColumns.CREATES, EthColumns.FROM,
                EthColumns.GAS, EthColumns.GASPRICE, EthColumns.HASH, EthColumns.INPUT, EthColumns.NONCE,
                EthColumns.PUBLICKEY, EthColumns.R, EthColumns.RAW, EthColumns.S, EthColumns.TO,
                EthColumns.TRANSACTIONINDEX, EthColumns.V, EthColumns.VALUE));

        Map ethColumnTypeBlckMap = new HashMap<String, Class>();

        Map ethColumnTypeTransactionMap = new HashMap<String, Class>();

        ethColumnTypeBlckMap.put(EthColumns.BLOCKNUMBER, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.EXTRADATA, String.class);
        ethColumnTypeBlckMap.put(EthColumns.SIZE, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.GASLIMIT, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.GASUSED, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.TIMESTAMP, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.TRANSACTIONS, Object.class);
        ethColumnTypeBlckMap.put(EthColumns.SEALFIELDS, Object.class);
        ethColumnTypeBlckMap.put(EthColumns.UNCLES, Object.class);
        ethColumnTypeBlckMap.put(EthColumns.TOTALDIFFICULTY, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.MIXHASH, String.class);
        ethColumnTypeBlckMap.put(EthColumns.MINER, String.class);
        ethColumnTypeBlckMap.put(EthColumns.AUTHOR, String.class);
        ethColumnTypeBlckMap.put(EthColumns.RECEIPTSROOT, String.class);
        ethColumnTypeBlckMap.put(EthColumns.STATEROOT, String.class);
        ethColumnTypeBlckMap.put(EthColumns.TRANSACTIONSROOT, String.class);
        ethColumnTypeBlckMap.put(EthColumns.LOGSBLOOM, String.class);
        ethColumnTypeBlckMap.put(EthColumns.SHA3UNCLES, String.class);
        ethColumnTypeBlckMap.put(EthColumns.NONCE, BigInteger.class);
        ethColumnTypeBlckMap.put(EthColumns.PARENTHASH, String.class);
        ethColumnTypeBlckMap.put(EthColumns.HASH, String.class);

        ethColumnTypeTransactionMap.put(EthColumns.BLOCKHASH, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.BLOCKNUMBER, BigInteger.class);
        ethColumnTypeTransactionMap.put(EthColumns.CREATES, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.FROM, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.GAS, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.GASPRICE, BigInteger.class);
        ethColumnTypeTransactionMap.put(EthColumns.HASH, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.INPUT, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.NONCE, BigInteger.class);
        ethColumnTypeTransactionMap.put(EthColumns.PUBLICKEY, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.R, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.RAW, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.S, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.TO, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.TRANSACTIONINDEX, BigInteger.class);
        ethColumnTypeTransactionMap.put(EthColumns.V, String.class);
        ethColumnTypeTransactionMap.put(EthColumns.VALUE, BigInteger.class);

        ethTableTypeMap.put(EthTables.BLOCK, ethColumnTypeBlckMap);
        ethTableTypeMap.put(EthTables.TRANSACTION, ethColumnTypeTransactionMap);
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

    @Override
    public Map<String, Integer> getColumnTypeMap(String s) {
        Map<String, Integer> mapType = new HashMap<>();
        List<SelectItem> cols = this.getSelectItems();
        Iterator colItterator = cols.iterator();
        Map<String,Class> lclcolumnTypeMap = (Map) ethTableTypeMap.get(s);
        while (colItterator.hasNext()) {
            SelectItem col = (SelectItem) colItterator.next();
            if (col.hasChildType(StarNode.class)) {

                for (Map.Entry<String, Class> entry : lclcolumnTypeMap.entrySet()) {
                    if(entry.getValue() instanceof Class){
                        mapType.put(entry.getKey(), getSQLType(entry.getValue()));
                    }
                }
                break;
            } else if (col.hasChildType(Column.class)) {
                String colName = ((IdentifierNode) ((Column) col.getChildType(Column.class, 0))
                    .getChildType(IdentifierNode.class, 0)).getValue();
                if (lclcolumnTypeMap.containsKey(colName))
                    mapType.put(colName, getSQLType((Class) lclcolumnTypeMap.get(colName)));
                else
                    mapType.put(colName, getSQLType(Object.class));
            } else if (col.hasChildType(FunctionNode.class)) {
                String func = ((IdentifierNode) ((FunctionNode) col.getChildType(FunctionNode.class, 0))
                        .getChildType(IdentifierNode.class, 0)).getValue();
                String functionString =
                        Utilities.createFunctionColName((FunctionNode) col.getChildType(FunctionNode.class, 0));
                switch (func) {
                    case "sum":
                        mapType.put(functionString, getSQLType(Long.class));
                        break;
                    case "count":
                        mapType.put(functionString, getSQLType(Long.class));
                }
            }
        }
        return mapType;
    }

    /* Map Class to SQL Types */
    public int getSQLType(Class className) {
        if (className.equals(String.class)) {
            return Types.VARCHAR;
        } else if (className.equals(int.class)) {
            return Types.INTEGER;
        } else if (className.equals(BigInteger.class) || className.equals(Long.class)) {
            return Types.BIGINT;
        } else if (className.equals(Float.class)) {
            return Types.FLOAT;
        } else if (className.equals(Double.class)) {
            return Types.DOUBLE;
        }
        // else take object type
        return Types.JAVA_OBJECT;
    }

    static Map<String, List<String>> getEthTableColumnMap() {
        return Collections.unmodifiableMap(ethTableColumnMap);
    }
}
