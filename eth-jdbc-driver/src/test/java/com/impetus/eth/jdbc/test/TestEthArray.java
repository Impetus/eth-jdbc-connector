package com.impetus.eth.jdbc.test;

import com.impetus.blkch.BlkchnException;
import com.impetus.eth.jdbc.EthArray;
import org.junit.Test;
import org.junit.experimental.categories.Category;


import com.impetus.test.catagory.UnitTest;

import junit.framework.TestCase;
import static org.junit.Assert.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

@Category(UnitTest.class)
public class TestEthArray extends TestCase  {

    @Test
    public void testEthArray() {
        List expected = Arrays.asList("Sachin", "Sourav", "Dravid");
        EthArray ethArray = new EthArray(expected, 12);
        try {
            Object[] actual = (Object[]) ethArray.getArray();
            assertArrayEquals(expected.toArray(),actual);
            assert(12 == ethArray.getBaseType());

            ethArray.free();
            assert(12 != ethArray.getBaseType());

        } catch (SQLException e) {
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            fail(stringWriter.toString());
        }
    }
    @Test
    public void testEthException() {
        List expected = Arrays.asList("Sachin", "Sourav", "Dravid");
        EthArray ethArray = new EthArray(expected, 0);
        boolean exception = false;
        try {
            ethArray.getArray();
        } catch (SQLException e) {

        } catch (BlkchnException e) {
            exception = true;
        }
        assertEquals(true, exception);
    }




}
