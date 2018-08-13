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
package com.impetus.blkchn.eth.smartContract;

import com.impetus.blkch.BlkchnException;
import com.impetus.blkchn.eth.ReadConfig;
import com.impetus.eth.jdbc.DriverConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SmartContractFunction {
    private static final Logger LOGGER = LoggerFactory.getLogger(SmartContractFunction.class);

    public static void main(String[] args) throws InterruptedException, ExecutionException {
        String url = "jdbc:blkchn:ethereum://ropsten.infura.io/1234";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        ReadConfig.loadConfig();
        String keystorePath = ReadConfig.keystorePath;
        String keystorePassword = ReadConfig.keystorePassword;
        String smartContractAddress = ReadConfig.smartContractAddress;
        Long timeOutInSec = 30L;
        String query = "CALL getName() USE SMARTCONTRACT "
                + "'com.impetus.blkchn.eth.smartContract.FirstSmartContract' WITH ADDRESS '"+smartContractAddress+"' AND WITHASYNC true is_Valid";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH, keystorePath);
            prop.put(DriverConstants.KEYSTORE_PASSWORD, keystorePassword);
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            boolean retBool = stmt.execute(query);
            if (retBool) {
                ResultSet ret = stmt.getResultSet();
                ret.next();
                CompletableFuture return_value = (CompletableFuture) ret.getObject(1);
                LOGGER.info("completed :: " + return_value.get(timeOutInSec,TimeUnit.SECONDS));
            }
        } catch (Exception e1) {
            throw new BlkchnException("Error while running function transaction", e1);
        }
    }

}
