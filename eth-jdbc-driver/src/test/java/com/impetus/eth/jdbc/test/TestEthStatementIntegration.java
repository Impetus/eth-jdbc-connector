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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.sql.query.RangeNode;
import com.impetus.eth.integration.test.EthDriverTest;
import com.impetus.eth.jdbc.EthStatement;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;

import junit.framework.TestCase;

@Category(IntegrationTest.class)
public class TestEthStatementIntegration extends TestCase {
    private static final Logger LOGGER = LoggerFactory.getLogger(EthDriverTest.class);

    String url = ConnectionUtil.getEthUrl();

    String driverClass = "com.impetus.eth.jdbc.EthDriver";

    @Test
    public void testGetBlockHeight() {
        boolean status = false;
        try {

            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            EthStatement stmt = (EthStatement) conn.createStatement();
            stmt.getBlockHeight();
            status = true;
        } catch (Exception e1) {
            e1.printStackTrace();
        }
        assertEquals(true, status);
    }

    @Test
    public void testGetRange() {
        RangeNode rangeNode=null;
        try {

            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            EthStatement stmt = (EthStatement) conn.createStatement();
           rangeNode=  stmt.getProbableRange("select * from transaction where blocknumber>=1652339 and blocknumber < 1652346 or blocknumber =1652346");
        } catch (Exception e1) {
            e1.printStackTrace();
        }
      assertEquals("[1652339-1652345]", rangeNode.getRangeList().getRanges().get(0).toString());
    }
    
    @Test
    public void testGetRange2() {
        RangeNode rangeNode=null;
        try {

            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            EthStatement stmt = (EthStatement) conn.createStatement();
           rangeNode=  stmt.getProbableRange("select * from transaction where blocknumber>=1652339 and blocknumber < 1652346 and blocknumber > 1652341 ");
        } catch (Exception e1) {
            e1.printStackTrace();
        }
      assertEquals("[1652342-1652345]", rangeNode.getRangeList().getRanges().get(0).toString());
    }
}
