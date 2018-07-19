package com.impetus.eth.test;

import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.impetus.eth.jdbc.EthConnection;
import com.impetus.eth.jdbc.EthPreparedStatement;
import com.impetus.test.catagory.UnitTest;

@Category(UnitTest.class)
public class TestEthPreparedStatement {

    @Test
    public void testPreparedStatement() {
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "select count(*) as cnt, blocknumber from transaction where blocknumber = 1234 or blocknumber = ? or blocknumber =12323 or blocknumber = ? group by blocknumber";
        boolean status = true;
        try {
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);

        } catch (Exception e) {
            status = false;
        }
        assertEquals(true, status);
    }

}
