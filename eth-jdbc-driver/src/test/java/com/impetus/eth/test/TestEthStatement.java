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

import com.impetus.blkch.BlkchnException;
import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthStatement;
import com.impetus.test.catagory.UnitTest;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
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
            statement.addBatch("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
            statement.addBatch("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
            statement.addBatch("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
            statement.addBatch("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
        } catch (SQLException e) {

        }
        List expected = new ArrayList<Object>();
        expected.add("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
        expected.add("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
        expected.add("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
        expected.add("insert into transaction (toAddress, value, unit, async) values ('\"+ address +\"', 1.11, 'ether', true)");
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
    public void testgetSchema(){
        ResultSetMetaData resultSetMetaData = statement.getSchema("select blocknumber,extradata,size from block");
        int getColumnType1 = 0;
        int getColumnType2 = 0;
        int getColumnCount = 0;
        int getColumnType3 = 0;
        try {
            getColumnCount = resultSetMetaData.getColumnCount();
            getColumnType1 = resultSetMetaData.getColumnType(1);
            getColumnType2 = resultSetMetaData.getColumnType(2);
            getColumnType3 = resultSetMetaData.getColumnType(3);
        } catch (Exception e) {

        }
        assertEquals(getColumnCount, 3);
        assertEquals(getColumnType1,Types.BIGINT);
        assertEquals(getColumnType3,Types.BIGINT);
        assertEquals(getColumnType2,Types.VARCHAR);
    }

    @Test
    public void testCheckStatemetType(){
        Exception exceptionExpected = null;
        try{
            statement.addBatch("Select * from block");
        } catch (Exception e) {
            exceptionExpected = e;
        }
        assert(exceptionExpected instanceof BlkchnException);
    }

}
