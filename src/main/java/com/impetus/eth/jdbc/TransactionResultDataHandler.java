/******************************************************************************* 
 * * Copyright 2017 Impetus Infotech.
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
package com.impetus.eth.jdbc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.core.methods.response.Transaction;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.StarNode;
import com.impetus.eth.parser.Function;
import com.impetus.eth.parser.Utils;

public class TransactionResultDataHandler implements DataHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionResultDataHandler.class);

    private static Map<String, Integer> columnNamesMap = new LinkedHashMap<String, Integer>();

    static {
        columnNamesMap.put("blockhash", 0);
        columnNamesMap.put("blocknumber", 1);
        columnNamesMap.put("creates", 2);
        columnNamesMap.put("from", 3);
        columnNamesMap.put("gas", 4);
        columnNamesMap.put("gasprice", 5);
        columnNamesMap.put("hash", 6);
        columnNamesMap.put("input", 7);
        columnNamesMap.put("nonce", 8);
        columnNamesMap.put("publickey", 9);
        columnNamesMap.put("r", 10);
        columnNamesMap.put("raw", 11);
        columnNamesMap.put("s", 12);
        columnNamesMap.put("to", 13);
        columnNamesMap.put("transactionindex", 14);
        columnNamesMap.put("v", 15);
        columnNamesMap.put("value", 16);
    }

    public Map<String, Integer> returnColumnNamesMap = new LinkedHashMap<>();

    public Map<String, Integer> getColumnNamesMap() {
        return returnColumnNamesMap;
    }

    private ArrayList<List<Object>> result = new ArrayList<>();

    private boolean columnsInitialized = false;

    private boolean isGroupbySelect = false;

    @Override
    public ArrayList<List<Object>> convertGroupedDataToObjArray(List rows, List<SelectItem> selItems,
            List<String> groupByCols) {
        isGroupbySelect = true;
        Map<Object, List<Object>> groupedData = ((List<Object>) rows).stream().collect(
                Collectors.groupingBy(
                        transInfo -> groupByCols.stream()
                                .map(column -> Utils.getTransactionColumnValue((Transaction) transInfo, column))
                                .collect(Collectors.toList()), Collectors.toList()));
        for (Entry<Object, List<Object>> entry : groupedData.entrySet()) {
            convertToObjArray((List<Object>) entry.getValue(), selItems, null);
        }
        return result;
    }

    @Override
    public ArrayList<List<Object>> convertToObjArray(List rows, List<SelectItem> selItems, List<String> extraSelectCols) {

        LOGGER.info("Conversion of transaction objects to Result set Objects started");

        for (Object record : rows) {
            Transaction transInfo = (Transaction) record;
            List<Object> returnRec = new ArrayList<>();
            for (SelectItem col : selItems) {
                if (col.hasChildType(StarNode.class)) {

                    returnRec.add(transInfo.getBlockHash());
                    returnRec.add(transInfo.getBlockNumber().longValueExact());
                    returnRec.add(transInfo.getCreates());
                    returnRec.add(transInfo.getFrom());
                    returnRec.add(transInfo.getGasRaw());
                    returnRec.add(transInfo.getGasPrice().longValueExact());
                    returnRec.add(transInfo.getHash());
                    returnRec.add(transInfo.getInput());
                    returnRec.add(transInfo.getNonce().longValueExact());
                    returnRec.add(transInfo.getPublicKey());
                    returnRec.add(transInfo.getR());
                    returnRec.add(transInfo.getRaw());
                    returnRec.add(transInfo.getS());
                    returnRec.add(transInfo.getTo());
                    returnRec.add(transInfo.getTransactionIndex().longValueExact());
                    returnRec.add(transInfo.getV());
                    returnRec.add(transInfo.getValue().toString());
                    if (!columnsInitialized) {
                        returnColumnNamesMap = columnNamesMap;
                    }
                } else if (col.hasChildType(Column.class)) {
                    String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                    if (!columnsInitialized) {
                        if (columnNamesMap.containsKey(colName.toLowerCase())) {
                            returnColumnNamesMap.put(colName, returnColumnNamesMap.size());
                        } else {
                            LOGGER.error("Column " + colName + " doesn't exist in table");
                            throw new RuntimeException("Column " + colName + " doesn't exist in table");
                        }

                    }

                    returnRec.add(Utils.getTransactionColumnValue(transInfo, colName));
                } else if (col.hasChildType(FunctionNode.class)) {
                    Function computFunc = new Function(rows, columnNamesMap, getTableName());
                    Object computeResult = computFunc.computeFunction(col.getChildType(FunctionNode.class, 0));
                    returnRec.add(computeResult);
                    if (!columnsInitialized) {
                        returnColumnNamesMap.put(
                                computFunc.createFunctionColName(col.getChildType(FunctionNode.class, 0)),
                                returnColumnNamesMap.size());

                    }
                }
            }
            if (!(null == extraSelectCols)) {

                for (String extraSelectColumn : extraSelectCols) {
                    if (!columnsInitialized) {
                        if (columnNamesMap.containsKey(extraSelectColumn.toLowerCase())) {
                            returnColumnNamesMap.put(extraSelectColumn, returnColumnNamesMap.size());
                        } else {
                            LOGGER.error("Column " + extraSelectColumn + " doesn't exist in table");
                            throw new RuntimeException("Column " + extraSelectColumn + " doesn't exist in table");
                        }
                    }
                    returnRec.add(Utils.getTransactionColumnValue(transInfo, extraSelectColumn));
                }
            }
            result.add(returnRec);
            columnsInitialized = true;

            if (isGroupbySelect)
                break;
        }

        LOGGER.info("Conversion completed. Returning ..");
        return result;
    }

    @Override
    public String getTableName() {

        return "transactions";
    }

}
