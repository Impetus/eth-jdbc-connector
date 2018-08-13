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

import com.impetus.blkchn.eth.ReadConfig;
import com.impetus.eth.jdbc.DriverConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class SmartContractDeployWithAsync {
    private static final Logger LOGGER = LoggerFactory.getLogger(SmartContractDeployWithAsync.class);

    public static void main(String[] args) {
        String url = "jdbc:blkchn:ethereum://ropsten.infura.io/1234";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        ReadConfig.loadConfig();
        String keystorePath = ReadConfig.keystorePath;
        String keystorePassword = ReadConfig.keystorePassword;
        String query = "DEPLOY smartcontract 'com.impetus.blkchn.eth.smartContract.FirstSmartContract'() AND withasync true";
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
                LOGGER.info("Smart Contract Deployed at :: " + return_value.get(10,TimeUnit.SECONDS));
            }
            LOGGER.info("done");
        } catch (Exception e) {
            LOGGER.error(e.getMessage());
        }
    }
}
