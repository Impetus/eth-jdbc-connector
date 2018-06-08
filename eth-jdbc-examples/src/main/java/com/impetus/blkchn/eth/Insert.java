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
package com.impetus.blkchn.eth;

import java.io.IOException;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.web3j.crypto.CipherException;
import org.web3j.protocol.core.methods.response.TransactionReceipt;


import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthStatement;

public class Insert {
    public static void main(String[] args) throws ClassNotFoundException, IOException, CipherException,
            InterruptedException, ExecutionException, Exception {
        String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";

        String query = "insert into transaction (toAddress, value, unit, async) values ('0xa76cd046cf6089fe2adcf1680fcede500e44bacd', 1.11, 'ether', true)";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,
                    "/home/impadmin/keystore/UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1");
            prop.put(DriverConstants.KEYSTORE_PASSWORD, "impetus123");
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            ResultSet ret = ((EthStatement) stmt).executeAndReturn(query);
            while (ret.next()) {
               System.out.println(((TransactionReceipt) (((CompletableFuture) ret.getObject(1)).get())).getTransactionHash());
          }
        } catch (SQLException | ClassNotFoundException e1) {
            e1.printStackTrace();
        }

    }

}
