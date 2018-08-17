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

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthStatement;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.crypto.CipherException;
import org.web3j.protocol.core.methods.response.TransactionReceipt;

import java.io.IOException;
import java.sql.*;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class InsertInBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertInBatch.class);

    public static void main(String[] args){
        ReadConfig.loadConfig();
        String keystorePath = ReadConfig.keystorePath;
        String keystorePassword = ReadConfig.keystorePassword;
        String address = ReadConfig.address;
        String url = "jdbc:blkchn:ethereum://ropsten.infura.io/1234";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";

        String query1 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query2 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query3 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query4 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query5 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query6 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query7 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        String query8 =
            "insert into transaction (toAddress, value, unit, async) values ('"+address+"', 1.11, 'ether', false)";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,keystorePath);
            prop.put(DriverConstants.KEYSTORE_PASSWORD, keystorePassword);
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            stmt.addBatch(query1);
            stmt.addBatch(query2);
            stmt.addBatch(query3);
            stmt.addBatch(query4);
            stmt.addBatch(query5);
            stmt.addBatch(query6);
            stmt.addBatch(query7);
            stmt.addBatch(query8);
            int[] output = stmt.executeBatch();
            System.out.println(Arrays.toString(output));
        } catch (SQLException | ClassNotFoundException e1) {
            e1.printStackTrace();
        }

    }

}
