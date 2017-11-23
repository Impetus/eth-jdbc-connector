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
import org.web3j.protocol.core.methods.response.EthBlock.Block;

import com.impetus.blkch.sql.query.Column;
import com.impetus.blkch.sql.query.FunctionNode;
import com.impetus.blkch.sql.query.IdentifierNode;
import com.impetus.blkch.sql.query.SelectItem;
import com.impetus.blkch.sql.query.StarNode;
import com.impetus.eth.parser.AggregationFunctions;
import com.impetus.eth.parser.DataFrame;

/**
 * The Class BlockResultDataHandler.
 * 
 * @author ashishk.shukla
 * 
 */
public class BlockResultDataHandler implements DataHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger(BlockResultDataHandler.class);

    /** The column names map. */
    private static HashMap<String, Integer> columnNamesMap = new HashMap<String, Integer>();

    static
    {
        columnNamesMap.put("number", 0);
        columnNamesMap.put("hash", 1);
        columnNamesMap.put("parenthash", 2);
        columnNamesMap.put("nonce", 3);
        columnNamesMap.put("sha3uncles", 4);
        columnNamesMap.put("logsbloom", 5);
        columnNamesMap.put("transactionsroot", 6);
        columnNamesMap.put("stateroot", 7);
        columnNamesMap.put("receiptsroot", 8);
        columnNamesMap.put("author", 9);
        columnNamesMap.put("miner", 10);
        columnNamesMap.put("mixhash", 11);
        columnNamesMap.put("totaldifficulty", 12);
        columnNamesMap.put("extradata", 13);
        columnNamesMap.put("size", 14);
        columnNamesMap.put("gaslimit", 15);
        columnNamesMap.put("gasused", 16);
        columnNamesMap.put("timestamp", 17);
        columnNamesMap.put("transactions", 18);
        columnNamesMap.put("uncles", 19);
        columnNamesMap.put("sealfields", 20);
    }

    public static HashMap<String, Integer> returnColumnNamesMap = new HashMap<>();

    public HashMap<String, Integer> getColumnNamesMap()
    {
        return returnColumnNamesMap;
    }

    @Override
    public ArrayList<List<Object>> convertToObjArray(List rows, List<SelectItem> selItems)
    {
        LOGGER.info("Conversion of Block objects to Result set Objects started");
        ArrayList<List<Object>> result = new ArrayList<>();
        boolean columnsInitialized = false;
        for (Object record : rows)
        {
            Block blockInfo = (Block) record;
            List<Object> returnRec = new ArrayList<>();
            for (SelectItem col : selItems)
            {
                if (col.hasChildType(StarNode.class))
                {
                   
                    returnRec.add(blockInfo.getNumberRaw());
                    returnRec.add(blockInfo.getHash());
                    returnRec.add(blockInfo.getParentHash());
                    returnRec.add(blockInfo.getNonceRaw());
                    returnRec.add(blockInfo.getSha3Uncles());
                    returnRec.add(blockInfo.getLogsBloom());
                    returnRec.add(blockInfo.getTransactionsRoot());
                    returnRec.add(blockInfo.getStateRoot());
                    returnRec.add(blockInfo.getReceiptsRoot());
                    returnRec.add(blockInfo.getAuthor());
                    returnRec.add(blockInfo.getMiner());
                    returnRec.add(blockInfo.getMixHash());
                    returnRec.add(blockInfo.getTotalDifficultyRaw());
                    returnRec.add(blockInfo.getExtraData());
                    returnRec.add(blockInfo.getSizeRaw());
                    returnRec.add(blockInfo.getGasLimitRaw());
                    returnRec.add(blockInfo.getGasUsedRaw());
                    returnRec.add(blockInfo.getTimestampRaw());
                    returnRec.add(blockInfo.getTransactions());
                    returnRec.add(blockInfo.getUncles());
                    returnRec.add(blockInfo.getSealFields());
                    if (!columnsInitialized)
                    {
                        returnColumnNamesMap = columnNamesMap;
                    }
                }
                else if (col.hasChildType(Column.class))
                {
                    String colName = col.getChildType(Column.class, 0).getChildType(IdentifierNode.class, 0)
                            .getValue();
                    if (!columnsInitialized)
                    {
                        if (columnNamesMap.containsKey(colName.toLowerCase()))
                        {
                           returnColumnNamesMap.put(colName, returnColumnNamesMap.size());
                        }else {
                            throw new RuntimeException("Column " + colName + " doesn't exist in table");
                        }

                    }
               
                    returnRec.add(getBlockColumnValue(blockInfo, colName)); 
                }
                else if (col.hasChildType(FunctionNode.class))
                {
                    // todo implement
                }
            }
            result.add(returnRec);
            columnsInitialized = true;

        }

        LOGGER.info("Conversion completed. Returning to ResultSet");
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

        return "blocks";
    }
    
    private Object getBlockColumnValue(Block blockInfo, String colName){
        if("number".equalsIgnoreCase(colName)){
            return blockInfo.getNumberRaw();       
        }else if("hash".equalsIgnoreCase(colName)){
            return blockInfo.getHash();
        }else if("parenthash".equalsIgnoreCase(colName)){
            return blockInfo.getParentHash();
        }else if("nonce".equalsIgnoreCase(colName)){
            return blockInfo.getNonceRaw();
        }else if("sha3uncles".equalsIgnoreCase(colName)){
            return blockInfo.getSha3Uncles();
        }else if("logsbloom".equalsIgnoreCase(colName)){
            return blockInfo.getLogsBloom();
        }else if("transactionsroot".equalsIgnoreCase(colName)){
            return blockInfo.getTransactionsRoot();
        }else if("stateroot".equalsIgnoreCase(colName)){
            return blockInfo.getStateRoot();
        }else if("receiptsroot".equalsIgnoreCase(colName)){
            return blockInfo.getReceiptsRoot();
        }else if("author".equalsIgnoreCase(colName)){
            return blockInfo.getAuthor();
        }else if("miner".equalsIgnoreCase(colName)){
            return blockInfo.getMiner();
        }else if("mixhash".equalsIgnoreCase(colName)){
            return blockInfo.getMixHash();
        }else if("totaldifficulty".equalsIgnoreCase(colName)){
            return blockInfo.getTotalDifficultyRaw();
        }else if("extradata".equalsIgnoreCase(colName)){
            return blockInfo.getExtraData();
        }else if("size".equalsIgnoreCase(colName)){
            return blockInfo.getSizeRaw();
        }else if("gaslimit".equalsIgnoreCase(colName)){
            return blockInfo.getGasLimitRaw();
        }else if("gasused".equalsIgnoreCase(colName)){
            return blockInfo.getGasUsedRaw();
        }else if("timestamp".equalsIgnoreCase(colName)){
            return blockInfo.getTimestampRaw();
        }else if("transactions".equalsIgnoreCase(colName)){
            return blockInfo.getTransactions();
        }else if("uncles".equalsIgnoreCase(colName)){
            return blockInfo.getUncles();
        }else if("sealfields".equalsIgnoreCase(colName)){
            return blockInfo.getSealFields();
        }
        else{
            throw new RuntimeException("column "+colName+" does not exist in the table");
        }
    }
}
