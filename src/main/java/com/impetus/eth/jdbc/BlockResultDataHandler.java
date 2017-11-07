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
        columnNamesMap.put("parentHash", 2);
        columnNamesMap.put("nonce", 3);
        columnNamesMap.put("sha3Uncles", 4);
        columnNamesMap.put("logsBloom", 5);
        columnNamesMap.put("transactionsRoot", 6);
        columnNamesMap.put("stateRoot", 7);
        columnNamesMap.put("receiptsRoot", 8);
        columnNamesMap.put("author", 9);
        columnNamesMap.put("miner", 10);
        columnNamesMap.put("mixHash", 11);
        columnNamesMap.put("totalDifficulty", 12);
        columnNamesMap.put("extraData", 13);
        columnNamesMap.put("size", 14);
        columnNamesMap.put("gasLimit", 15);
        columnNamesMap.put("gasUsed", 16);
        columnNamesMap.put("timestamp", 17);
        columnNamesMap.put("transactions", 18);
        columnNamesMap.put("uncles", 19);
        columnNamesMap.put("sealFields", 20);
    }

    /**
     * Gets the column names map.
     *
     * @return the column names map
     */
    public static HashMap<String, Integer> getColumnNamesMap()
    {
        return columnNamesMap;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.impetus.eth.jdbc.DataHandler#convertToObjArray(java.util.List)
     */
    @Override
    public ArrayList<Object[]> convertToObjArray(List rows)
    {
        LOGGER.info("Conversion of Block objects to Result set Objects started");
        ArrayList<Object[]> result = new ArrayList<Object[]>();
        for (Object bl : rows)
        {

            Object[] arr = new Object[columnNamesMap.size()];
            Block blockInfo = (Block) bl;
            arr[0] = blockInfo.getNumberRaw();
            arr[1] = blockInfo.getHash();
            arr[2] = blockInfo.getParentHash();
            arr[3] = blockInfo.getNonceRaw();
            arr[4] = blockInfo.getSha3Uncles();
            arr[5] = blockInfo.getLogsBloom();
            arr[6] = blockInfo.getTransactionsRoot();
            arr[7] = blockInfo.getStateRoot();
            arr[8] = blockInfo.getReceiptsRoot();
            arr[9] = blockInfo.getAuthor();
            arr[10] = blockInfo.getMiner();
            arr[11] = blockInfo.getMixHash();
            arr[12] = blockInfo.getTotalDifficultyRaw();
            arr[13] = blockInfo.getExtraData();
            arr[14] = blockInfo.getSize();
            arr[15] = blockInfo.getGasLimitRaw();
            arr[16] = blockInfo.getGasUsed();
            arr[17] = blockInfo.getTimestampRaw();
            arr[18] = blockInfo.getTransactions();
            arr[19] = blockInfo.getUncles();
            arr[20] = blockInfo.getSealFields();
            result.add(arr);
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

}
