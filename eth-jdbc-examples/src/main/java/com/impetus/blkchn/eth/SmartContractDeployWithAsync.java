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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

public class SmartContractDeployWithAsync {
	private static final Logger LOGGER = LoggerFactory.getLogger(SmartContractDeployWithAsync.class);

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		String url = "jdbc:blkchn:ethereum://127.0.0.1:8545";
		String driverClass = "com.impetus.eth.jdbc.EthDriver";
		String query = "DEPLOY smartcontract 'com.impetus.blkchn.eth.FirstSmartContract'() AND withasync true";
		try {
			Class.forName(driverClass);
			Properties prop = new Properties();
			prop.put(DriverConstants.KEYSTORE_PATH, "/home/<path>");
			prop.put(DriverConstants.KEYSTORE_PASSWORD, "<password>");
			Connection conn = DriverManager.getConnection(url, prop);
			Statement stmt = conn.createStatement();
			boolean retBool = stmt.execute(query);
			if (retBool) {
				ResultSet ret = stmt.getResultSet();
				ret.next();
				CompletableFuture return_value = (CompletableFuture) ret.getObject(1);
				while (true)
					if (return_value.isDone()) {
						LOGGER.info("completed :: " + return_value.get());
						break;
					} else {
						LOGGER.info("Waiting future to complete");
						Thread.sleep(1000);
					}
			}
			LOGGER.info("done");
		} catch (Exception e) {
			LOGGER.info(e.getMessage());
		}
	}
}
