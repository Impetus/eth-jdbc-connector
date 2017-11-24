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
package com.impetus.eth.general;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;
import org.web3j.utils.Numeric;

/**
 * The Class EthDriverTestBlocks.
 * 
 * @author ashishk.shukla
 * 
 */
public class EthDriverTestBlocks
{

    /**
     * The main method.
     *
     * @param args
     *            the arguments
     * @throws ClassNotFoundException
     *             the class not found exception
     */
    public static void main(String[] args) throws ClassNotFoundException
    {

        String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try
        {
            Class.forName(driverClass);
            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select transactions as ts, count(transactions) from blocks where blocknumber=1652339 ");
            while (rs.next())
            {
                // For Blocks

                /*System.out.println("\n************");
                System.out.println("2nd column value : " + rs.getString(1));
                System.out.println("block number in hex : " + rs.getString("number"));
                System.out.println("block number decoded : " + Numeric.decodeQuantity(rs.getString("number")));
                */
                System.out.println(rs.getObject(0));
                List<TransactionObject> lt = (List<TransactionObject>) rs.getObject("transactions");
                System.out.println("transation "+lt);
                System.out.println("blockNumber : " + lt.get(0).getBlockNumber());
                System.out.println(rs.getInt(1));
            }

        }
        catch (SQLException e1)
        {
            e1.printStackTrace();
        }

    }
}
