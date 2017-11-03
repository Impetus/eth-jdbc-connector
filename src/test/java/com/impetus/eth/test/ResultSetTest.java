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
package com.impetus.eth.test;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.http.HttpService;

import com.impetus.eth.jdbc.EthResultSet;
import com.impetus.eth.jdbc.TransactionResultDataHandler;

/**
 * The Class ResultSetTest.
 * 
 * @author ashishk.shukla
 * 
 */
public class ResultSetTest
{

    /** The web 3 j. */
    private static Web3j web3j = null;

    /**
     * Sets the up.
     */
    @Before
    public void setUp()
    {
        web3j = Web3j.build(new HttpService("http://172.25.41.52:8545"));
        web3j.web3ClientVersion().observable().subscribe(x -> {
            String clientVersion = x.getWeb3ClientVersion();
            System.out.println("Client Version: " + clientVersion);
        });
    }

    /**
     * Test transaction result.
     *
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    @Test
    public void testTransactionResult() throws IOException
    {
        // GET ALL TRANSACTIONS FROM BLOCK TABLE WITH BLOCK_NUMBER = <NUMBER>
        List<TransactionResult> trans = getTransactions("1876545");

        TransactionResultDataHandler dataHandler = new TransactionResultDataHandler();

        ResultSet r = new EthResultSet(dataHandler.convertToObjArray(trans), dataHandler.getColumnNamesMap(), 1, 1,
                dataHandler.getTableName());

        try
        {
            while (r.next())
            {
                System.out.println(r.getString("hash"));
            }
        }
        catch (SQLException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

    }

    /**
     * Gets the transactions.
     *
     * @param blockNumber
     *            the block number
     * @return the transactions
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    private List<TransactionResult> getTransactions(String blockNumber) throws IOException
    {
        EthBlock block = web3j.ethGetBlockByNumber(DefaultBlockParameter.valueOf(new BigInteger(blockNumber)), true)
                .send();

        return block.getBlock().getTransactions();
    }

}
