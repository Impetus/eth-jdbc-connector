package com.impetus.eth.integration.test;

import com.impetus.blkch.BlkchnException;
import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.tx.exceptions.ContractCallException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

@Category(IntegrationTest.class)
public class TestSmrtCntrctWrongAdd {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestSmartContract.class);

    @Test
    public void testSmartContractAddress() {
        LOGGER.info("***************Testing SmartContract Wrong Address Test************************");
        LOGGER.info("***************Should Finish with Exception************************");
        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        String query = "CALL getName() USE SMARTCONTRACT "
                + "'com.impetus.eth.integration.test.FirstSmartContract' WITH ADDRESS '0xf221ab0b1dee81' AND WITHASYNC true";
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
                LOGGER.info("completed :: " + return_value.get(ConnectionUtil.getTimeout(),TimeUnit.SECONDS));
            }
        } catch (BlkchnException e) {
            LOGGER.info("completed with exception "+ e.getMessage());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
