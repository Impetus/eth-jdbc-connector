package com.impetus.eth.integration.test;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;

@Category(IntegrationTest.class)
public class TestSmartContract {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSmartContract.class);

    @Test
    public void testSmartContractFunction() {
        LOGGER.info("***************Testing SmartContract Function************************");
        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        String query = "CALL getName() USE SMARTCONTRACT "
            + "'com.impetus.eth.integration.test.FirstSmartContract' WITH ADDRESS '0xf221ab0b1dee8108af38b13c109c3bf8deea7806' AND WITHASYNC true";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH, ConnectionUtil.getKeyStorePath());
            prop.put(DriverConstants.KEYSTORE_PASSWORD, ConnectionUtil.getKeyStorePassword());
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            boolean retBool = stmt.execute(query);
            if (retBool) {
                ResultSet ret = stmt.getResultSet();
                ret.next();
                CompletableFuture return_value = (CompletableFuture) ret.getObject(1);
                while (true)
                    if (return_value.isDone()) {
                        LOGGER.info("Return Value :: " + return_value.get());
                        break;
                    } else {
                        LOGGER.info("Waiting future to complete");
                        Thread.sleep(1000);
                    }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}