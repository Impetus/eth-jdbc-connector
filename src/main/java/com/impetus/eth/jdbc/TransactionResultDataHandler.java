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
import java.util.HashMap;
import java.util.List;

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

/**
 * The Class TransactionResultDataHandler.
 * 
 * @author karthikp.manchala
 * 
 */
public class TransactionResultDataHandler implements DataHandler
{

    /** The Constant LOGGER. */
    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionResultDataHandler.class);

    /** The column names map. */
    private static HashMap<String, Integer> columnNamesMap = new HashMap<String, Integer>();

    static
    {
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
        columnNamesMap.put("tranactionindex", 14);
        columnNamesMap.put("v", 15);
        columnNamesMap.put("value", 16);
    }

    /** The return column names map. */
    public static HashMap<String, Integer> returnColumnNamesMap = new HashMap<>();

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.DataHandler#getColumnNamesMap()
     */
    public HashMap<String, Integer> getColumnNamesMap()
    {
        return returnColumnNamesMap;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.DataHandler#convertToObjArray(java.util.List)
     */
    @Override
    public ArrayList<List<Object>> convertToObjArray(List rows, List<SelectItem> selItems)
    {

        LOGGER.info("Conversion of transaction objects to Result set Objects started");
        ArrayList<List<Object>> result = new ArrayList<>();
        boolean columnsInitialized = false;
        for (Object record : rows)
        {
            Transaction transInfo = (Transaction) record;
            List<Object> returnRec = new ArrayList<>();
            for (SelectItem col : selItems)
            {
                if (col.hasChildType(StarNode.class))
                {

                    returnRec.add(transInfo.getBlockHash());
                    returnRec.add(transInfo.getBlockNumberRaw());
                    returnRec.add(transInfo.getCreates());
                    returnRec.add(transInfo.getFrom());
                    returnRec.add(transInfo.getGasRaw());
                    returnRec.add(transInfo.getGasPriceRaw());
                    returnRec.add(transInfo.getHash());
                    returnRec.add(transInfo.getInput());
                    returnRec.add(transInfo.getNonceRaw());
                    returnRec.add(transInfo.getPublicKey());
                    returnRec.add(transInfo.getR());
                    returnRec.add(transInfo.getRaw());
                    returnRec.add(transInfo.getS());
                    returnRec.add(transInfo.getTo());
                    returnRec.add(transInfo.getTransactionIndexRaw());
                    returnRec.add(transInfo.getV());
                    returnRec.add(transInfo.getValueRaw());
                    if (!columnsInitialized)
                    {
                        returnColumnNamesMap = columnNamesMap;
                    }
                }
                else if (col.hasChildType(Column.class))
                {
                    String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0).getValue();
                    if (!columnsInitialized)
                    {
                        if (columnNamesMap.containsKey(colName.toLowerCase()))
                        {
                            returnColumnNamesMap.put(colName, returnColumnNamesMap.size());
                        }
                        else
                        {
                            LOGGER.error("Column " + colName + " doesn't exist in table");
                            throw new RuntimeException("Column " + colName + " doesn't exist in table");
                        }

                    }

                    returnRec.add(Utils.getTransactionColumnValue(transInfo, colName));
                }
                else if (col.hasChildType(FunctionNode.class))
                {
                    Function computFunc = new Function(rows, columnNamesMap, getTableName());
                    Object computeResult = computFunc.computeFunction(col.getChildType(FunctionNode.class, 0));
                    returnRec.add(computeResult);
                    if (col.hasChildType(IdentifierNode.class))
                    {
                        if (!columnsInitialized)
                        {
                            returnColumnNamesMap.put(col.getChildType(IdentifierNode.class, 0).getValue(),
                                    returnColumnNamesMap.size());
                        }
                    }
                    else if (!columnsInitialized)
                    {
                        returnColumnNamesMap.put(
                                computFunc.createFunctionColName(col.getChildType(FunctionNode.class, 0)),
                                returnColumnNamesMap.size());
                    }
                }
            }
            result.add(returnRec);
            columnsInitialized = true;

        }

        LOGGER.info("Conversion completed. Returning ..");
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.DataHandler#getTableName()
     */
    @Override
    public String getTableName()
    {

        return "transactions";
    }

}
