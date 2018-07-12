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
package com.impetus.eth.test;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthStatement;
import com.impetus.test.catagory.UnitTest;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Category(UnitTest.class)
public class TestEthStatement extends TestCase {
    EthStatement statement = null;

    @Override
    protected void setUp() {
        statement = new EthStatement(null, 0, 0);
    }

    @Test
    public void testCreateBatch() {
        try {
            statement.addBatch("select * from blocks where blocknumber = 123");
            statement.addBatch("select * from blocks where blocknumber = 124");
            statement.addBatch("select * from blocks where blocknumber = 125");
            statement.addBatch("select * from blocks where blocknumber = 126");
        } catch (SQLException e) {

        }
        List expected = new ArrayList<Object>();
        expected.add("select * from blocks where blocknumber = 123");
        expected.add("select * from blocks where blocknumber = 124");
        expected.add("select * from blocks where blocknumber = 125");
        expected.add("select * from blocks where blocknumber = 126");
        assertEquals(statement.getBatchedArgs(), expected);
    }

    @Test
    public void testClearBatch() {
        try {
            statement.clearBatch();
        } catch (SQLException e) {

        }
        assertEquals(statement.getBatchedArgs(), null);
    }

    @Test
    public void testContinueBatchOnError() {
        assertEquals(statement.isContinueBatchOnError(), false);
        statement.setContinueBatchOnError(true);
        assertEquals(statement.isContinueBatchOnError(), true);
    }

    @Test
    public void testTruncateAndConvertToInt() {
        long[] longArray = { 1L, 2L, 3L, 4L };
        int[] expectedArray = { 1, 2, 3, 4 };
        int[] gotArray = statement.truncateAndConvertToInt(longArray);
        Assert.assertArrayEquals(expectedArray, gotArray);
    }
}
