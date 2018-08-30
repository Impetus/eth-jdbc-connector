package com.impetus.eth.integration.test;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthPreparedStatement;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;
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
            prop.put(DriverConstants.KEYSTORE_PATH,ConnectionUtil.getKeyStorePath());
            prop.put(DriverConstants.KEYSTORE_PASSWORD,ConnectionUtil.getKeyStorePassword());
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

            Field f = stmt.getClass().getDeclaredField("batchList");
            f.setAccessible(true);

            assert(((List<Object[]>) f.get(stmt)).size() == 0);


        }catch(Exception e){
            e.printStackTrace();
        }
    }

}
