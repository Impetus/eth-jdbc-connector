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
package com.impetus.eth.jdbc.test;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.blkch.BlkchnException;
import com.impetus.eth.jdbc.EthStatement;
import com.impetus.test.catagory.UnitTest;

import junit.framework.TestCase;

@Category(UnitTest.class)
public class TestEthStatement extends TestCase {
    EthStatement statement = null;

    @Override
    protected void setUp() {
        statement = new EthStatement(null, 0, 0);
    }
    
    @Test
    public void testGetSchemaStar() {
        ResultSetMetaData rsmd=statement.getSchema("select * from block ");
        String colName="";
        try {
           colName=rsmd.getColumnName(1);
        } catch (SQLException e) {
           throw new BlkchnException("Error while running test case - testGetSchema");
        }
        
        assertEquals("blocknumber",colName );
    }
    
    @Test
    public void testGetSchemaColumnAlias() {
        ResultSetMetaData rsmd=statement.getSchema("select blocknumber,hash as blkhash,nonce from block ");
        String colName="";
        try {
           colName=rsmd.getColumnLabel(2);
        } catch (SQLException e) {
           throw new BlkchnException("Error while running test case - testGetSchema");
        }
        
        assertEquals("blkhash",colName );
    }
    
    
    @Test
    public void testAddbatch() {
        boolean status=false;
        try {
            statement.addBatch("insert into transaction (toAddress, value, unit, async) values ('address', 1, 'ether', true)");
            status=true;
        } catch (SQLException e) {
            status=false;
        }
        assertEquals(true, status);
    }
    
    @Test
    public void testAddbatchError() {
        boolean status=false;
        try {
            statement.addBatch("select * from block where blocknumber=22333444");
            status=true;
        } catch (Exception e) {
            status=false;
        }
        assertEquals(false, status);
    }
}
