package com.impetus.eth.integration.test;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthPreparedStatement;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Properties;

@Category(IntegrationTest.class)
public class TestPreparedStatementBatch {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestPreparedStatementBatch.class);

    @Test
    public void testInsertQuery() {
        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,
                    "/home/impadmin/keystore/UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1");
            prop.put(DriverConstants.KEYSTORE_PASSWORD, "impetus123");
            Connection conn = DriverManager.getConnection(url, prop);
            String query = "insert into transaction (toAddress, value, unit, async) values (?, ?, 'ether', false)";

            PreparedStatement stmt = conn.prepareStatement(query);
            stmt.setObject(2, 0.0001);
            stmt.setObject(1, "0x8144c67b144A408ABC989728e32965EDf37Adaa1");
            stmt.addBatch();
            stmt.addBatch();

            stmt.setObject(2, 0.0003);
            stmt.addBatch();

            stmt.executeBatch();

            assert(((EthPreparedStatement) stmt).getBatchedArgs().size() == 0);


        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
