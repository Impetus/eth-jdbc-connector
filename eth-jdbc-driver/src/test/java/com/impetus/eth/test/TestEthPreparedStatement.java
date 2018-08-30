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

import static org.junit.Assert.*;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

import com.impetus.eth.jdbc.EthConnection;
import com.impetus.eth.jdbc.EthPreparedStatement;
import com.impetus.test.catagory.UnitTest;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.util.List;

@Category(UnitTest.class)
public class TestEthPreparedStatement {

    @Test
    public void testPreparedStatement() {
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "select count(*) as cnt, blocknumber from transaction where blocknumber = 1234 or blocknumber = ? or blocknumber =12323 or blocknumber = ? group by blocknumber";
        boolean status = true;
        try {
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);
            stmt.close();
        } catch (Exception e) {
            status = false;
        }
        assertEquals(true, status);
    }

    @Test
    public void testSetParameters() {
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "select count(*) as cnt, blocknumber from transaction where blocknumber = 1234 or blocknumber = ? or blocknumber =12323 or blocknumber = ? group by blocknumber";
        boolean status = true;

        try {
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);
            stmt.setInt(1, 111);
            stmt.setInt(2, 111);
            stmt.close();

        } catch (Exception e) {
            status = false;
        }
        assertEquals(true, status);
    }

    @Test
    public void testSetParametersException() {
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "select count(*) as cnt, blocknumber from transaction where blocknumber = 1234 or blocknumber = ? or blocknumber =12323 or blocknumber = ? group by blocknumber";
        boolean status = true;

        try {
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);
            stmt.setInt(5, 111);
            stmt.close();
        } catch (Exception e) {
            status = false;
        }
        assertEquals(false, status);
    }

    @Test
    public void testPreparedStatementAddBatchSelect(){
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "select * from block where blocknumber = ?";
        boolean status = true;
        try{
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);
            stmt.setInt(1, 111);
            stmt.addBatch();
            stmt.close();
        }catch(Exception e){
            status = false;
        }
        assertEquals(false, status);
    }

    @Test
    public void testPreparedStatementBatchAfterClose(){
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "insert into block values(?)";
        int beforeClose = 0;
        int afterClose = 0;
        try{
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);
            Field f = stmt.getClass().getDeclaredField("batchList");
            f.setAccessible(true);

            stmt.setInt(1, 111);
            stmt.addBatch();

            stmt.setInt(1, 111);
            stmt.addBatch();

            stmt.setInt(1, 111);
            stmt.addBatch();

            beforeClose = ((List<Object[]>) f.get(stmt)).size();

            stmt.close();

            afterClose = ((List<Object[]>) f.get(stmt)).size();
        }catch(Exception e){

        }
        assertEquals(3, beforeClose);
        assertEquals(0, afterClose);
    }

    @Test
    public void testPreparedStatementAddBatch(){
        EthConnection connection = Mockito.mock(EthConnection.class);
        String sql = "insert into block values(?, ?, ?, ?)";
        try{
            EthPreparedStatement stmt = new EthPreparedStatement(connection, sql, 0, 0);

            Field f = stmt.getClass().getDeclaredField("batchList");
            f.setAccessible(true);

            stmt.setInt(1, 11);
            stmt.setInt(2, 12);
            stmt.setInt(3, 13);
            stmt.setInt(4, 14);
            stmt.addBatch();
            Object[] firstBatch = ((List<Object[]>) f.get(stmt)).get(0);
            assertEquals(firstBatch[0],11);
            assertEquals(firstBatch[1],12);
            assertEquals(firstBatch[2],13);
            assertEquals(firstBatch[3],14);

            stmt.setInt(1, 21);
            stmt.setInt(3, 23);
            stmt.setInt(4, 24);
            stmt.addBatch();
            Object[] secondBatch = ((List<Object[]>) f.get(stmt)).get(1);
            firstBatch = ((List<Object[]>) f.get(stmt)).get(0);
            assertEquals(firstBatch[0],11);
            assertEquals(firstBatch[1],12);
            assertEquals(firstBatch[2],13);
            assertEquals(firstBatch[3],14);
            assertEquals(secondBatch[0],21);
            assertEquals(secondBatch[1],12);
            assertEquals(secondBatch[2],23);
            assertEquals(secondBatch[3],24);

            stmt.setInt(1, 31);
            stmt.addBatch();
            Object[] thirdBatch = ((List<Object[]>) f.get(stmt)).get(2);
            firstBatch = ((List<Object[]>) f.get(stmt)).get(0);
            secondBatch = ((List<Object[]>) f.get(stmt)).get(1);
            assertEquals(firstBatch[0],11);
            assertEquals(firstBatch[1],12);
            assertEquals(firstBatch[2],13);
            assertEquals(firstBatch[3],14);
            assertEquals(secondBatch[0],21);
            assertEquals(secondBatch[1],12);
            assertEquals(secondBatch[2],23);
            assertEquals(secondBatch[3],24);
            assertEquals(thirdBatch[0],31);
            assertEquals(thirdBatch[1],12);
            assertEquals(thirdBatch[2],23);
            assertEquals(thirdBatch[3],24);

        }catch(Throwable e){
            StringWriter stringWriter = new StringWriter();
            e.printStackTrace(new PrintWriter(stringWriter));
            fail(stringWriter.toString());
        }

    }


}
