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
}
