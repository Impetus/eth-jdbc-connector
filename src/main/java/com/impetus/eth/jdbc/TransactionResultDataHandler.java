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

import org.web3j.protocol.core.methods.response.Transaction;

/**
 * The Class TransactionResultDataHandler.
 * 
 * @author karthikp.manchala
 * 
 */
public class TransactionResultDataHandler implements DataHandler
{

    /** The column names map. */
    private static HashMap<String, Integer> columnNamesMap = new HashMap<String, Integer>();

    static
    {
        columnNamesMap.put("blockHash", 0);
        columnNamesMap.put("blockNumber", 1);
        columnNamesMap.put("creates", 2);
        columnNamesMap.put("from", 3);
        columnNamesMap.put("gas", 4);
        columnNamesMap.put("gasprice", 5);
        columnNamesMap.put("hash", 6);
        columnNamesMap.put("input", 7);
        columnNamesMap.put("nonce", 8);
        columnNamesMap.put("publicKey", 9);
        columnNamesMap.put("r", 10);
        columnNamesMap.put("raw", 11);
        columnNamesMap.put("s", 12);
        columnNamesMap.put("to", 13);
        columnNamesMap.put("tranactionIndex", 14);
        columnNamesMap.put("v", 15);
        columnNamesMap.put("value", 16);
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
    public ArrayList<Object[]> convertToObjArray(List<?> rows)
    {

        ArrayList<Object[]> result = new ArrayList<Object[]>();
        for (Object t : rows)
        {
            Object[] arr = new Object[columnNamesMap.size()];
            Transaction tr = (Transaction) t;

            arr[0] = tr.getBlockHash();
            arr[1] = tr.getBlockNumberRaw();
            arr[2] = tr.getCreates();
            arr[3] = tr.getFrom();
            arr[4] = tr.getGasRaw();
            arr[5] = tr.getGasPriceRaw();
            arr[6] = tr.getHash();
            arr[7] = tr.getInput();
            arr[8] = tr.getNonceRaw();
            arr[9] = tr.getPublicKey();
            arr[10] = tr.getR();
            arr[11] = tr.getRaw();
            arr[12] = tr.getS();
            arr[13] = tr.getTo();
            arr[14] = tr.getTransactionIndexRaw();
            arr[15] = tr.getV();
            arr[16] = tr.getValueRaw();
            result.add(arr);
        }
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
