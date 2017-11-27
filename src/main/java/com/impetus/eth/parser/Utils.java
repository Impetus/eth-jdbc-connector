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
package com.impetus.eth.parser;

import java.util.List;

import org.web3j.protocol.core.methods.response.Transaction;
import org.web3j.protocol.core.methods.response.EthBlock.Block;

/**
 * The Class Utils.
 */
public class Utils
{

    /**
     * Gets the transaction column value.
     *
     * @param transInfo
     *            the trans info
     * @param colName
     *            the col name
     * @return the transaction column value
     */
    public static Object getTransactionColumnValue(Transaction transInfo, String colName)
    {
        if ("blockhash".equalsIgnoreCase(colName))
        {
            return transInfo.getBlockHash();
        }
        else if ("blocknumber".equalsIgnoreCase(colName))
        {
            return transInfo.getBlockNumberRaw();
        }
        else if ("creates".equalsIgnoreCase(colName))
        {
            return transInfo.getCreates();
        }
        else if ("from".equalsIgnoreCase(colName))
        {
            return transInfo.getFrom();
        }
        else if ("gas".equalsIgnoreCase(colName))
        {
            return transInfo.getGasRaw();
        }
        else if ("gasprice".equalsIgnoreCase(colName))
        {
            return transInfo.getGasPriceRaw();
        }
        else if ("hash".equalsIgnoreCase(colName))
        {
            return transInfo.getHash();
        }
        else if ("input".equalsIgnoreCase(colName))
        {
            return transInfo.getInput();
        }
        else if ("nonce".equalsIgnoreCase(colName))
        {
            return transInfo.getNonceRaw();
        }
        else if ("publickey".equalsIgnoreCase(colName))
        {
            return transInfo.getPublicKey();
        }
        else if ("r".equalsIgnoreCase(colName))
        {
            return transInfo.getR();
        }
        else if ("raw".equalsIgnoreCase(colName))
        {
            return transInfo.getRaw();
        }
        else if ("s".equalsIgnoreCase(colName))
        {
            return transInfo.getS();
        }
        else if ("to".equalsIgnoreCase(colName))
        {
            return transInfo.getTo();
        }
        else if ("tranactionindex".equalsIgnoreCase(colName))
        {
            return transInfo.getTransactionIndex();
        }
        else if ("v".equalsIgnoreCase(colName))
        {
            return transInfo.getV();
        }
        else if ("value".equalsIgnoreCase(colName))
        {
            return transInfo.getValueRaw();
        }
        else
        {
            throw new RuntimeException("column " + colName + " does not exist in the table");
        }
    }

    /**
     * Gets the block column value.
     *
     * @param blockInfo
     *            the block info
     * @param colName
     *            the col name
     * @return the block column value
     */
    public static Object getBlockColumnValue(Block blockInfo, String colName)
    {
        if ("blocknumber".equalsIgnoreCase(colName))
        {
            return blockInfo.getNumberRaw();
        }
        else if ("hash".equalsIgnoreCase(colName))
        {
            return blockInfo.getHash();
        }
        else if ("parenthash".equalsIgnoreCase(colName))
        {
            return blockInfo.getParentHash();
        }
        else if ("nonce".equalsIgnoreCase(colName))
        {
            return blockInfo.getNonceRaw();
        }
        else if ("sha3uncles".equalsIgnoreCase(colName))
        {
            return blockInfo.getSha3Uncles();
        }
        else if ("logsbloom".equalsIgnoreCase(colName))
        {
            return blockInfo.getLogsBloom();
        }
        else if ("transactionsroot".equalsIgnoreCase(colName))
        {
            return blockInfo.getTransactionsRoot();
        }
        else if ("stateroot".equalsIgnoreCase(colName))
        {
            return blockInfo.getStateRoot();
        }
        else if ("receiptsroot".equalsIgnoreCase(colName))
        {
            return blockInfo.getReceiptsRoot();
        }
        else if ("author".equalsIgnoreCase(colName))
        {
            return blockInfo.getAuthor();
        }
        else if ("miner".equalsIgnoreCase(colName))
        {
            return blockInfo.getMiner();
        }
        else if ("mixhash".equalsIgnoreCase(colName))
        {
            return blockInfo.getMixHash();
        }
        else if ("totaldifficulty".equalsIgnoreCase(colName))
        {
            return blockInfo.getTotalDifficultyRaw();
        }
        else if ("extradata".equalsIgnoreCase(colName))
        {
            return blockInfo.getExtraData();
        }
        else if ("size".equalsIgnoreCase(colName))
        {
            return blockInfo.getSizeRaw();
        }
        else if ("gaslimit".equalsIgnoreCase(colName))
        {
            return blockInfo.getGasLimitRaw();
        }
        else if ("gasused".equalsIgnoreCase(colName))
        {
            return blockInfo.getGasUsedRaw();
        }
        else if ("timestamp".equalsIgnoreCase(colName))
        {
            return blockInfo.getTimestampRaw();
        }
        else if ("transactions".equalsIgnoreCase(colName))
        {
            return blockInfo.getTransactions();
        }
        else if ("uncles".equalsIgnoreCase(colName))
        {
            return blockInfo.getUncles();
        }
        else if ("sealfields".equalsIgnoreCase(colName))
        {
            return blockInfo.getSealFields();
        }
        else
        {
            throw new RuntimeException("column " + colName + " does not exist in the table");
        }
    }
    
    
 public static void verifyGroupedColumns(List<String> columns, List<String> groupedColumns){

     for(String selectColumn: columns){
         
         if (groupedColumns.contains(selectColumn))
             continue;
         else 
             throw new RuntimeException("Select column " + selectColumn + " should exist in group by clause");
     }
 }
}
