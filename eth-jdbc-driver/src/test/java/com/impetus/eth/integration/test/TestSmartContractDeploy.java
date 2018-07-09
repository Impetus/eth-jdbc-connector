package com.impetus.eth.integration.test;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

@Category(IntegrationTest.class)
public class TestSmartContractDeploy {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestSmartContractDeploy.class);

    @Test
    public void testSmartContractDeploy() {
        LOGGER.info("***************Testing SmartContract Deploy************************");
        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        String query = "DEPLOY smartcontract 'com.impetus.eth.integration.test.FirstSmartContract'() AND withasync true";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,
                    ConnectionUtil.getKeyStorePath());
            prop.put(DriverConstants.KEYSTORE_PASSWORD, ConnectionUtil.getKeyStorePassword());
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            boolean retBool = stmt.execute(query);
            if(retBool) {
                ResultSet ret = stmt.getResultSet();
                ret.next();
                CompletableFuture return_value = (CompletableFuture) ret.getObject(1);
                while (true) if (return_value.isDone()) {
                    LOGGER.info("completed :: "+return_value.get());
                    break;
                } else {
                    LOGGER.info("Waiting future to complete");
                    Thread.sleep(1000);
                }
            }
            LOGGER.info("done");
        }catch (Exception e){
            LOGGER.info(e.getMessage());
        }
    }
}
