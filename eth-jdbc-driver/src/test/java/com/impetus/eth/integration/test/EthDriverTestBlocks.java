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
package com.impetus.eth.integration.test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;

import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;

/**
 * The Class EthDriverTestBlocks.
 * 
 * @author ashishk.shukla
 * 
 */
@Category(IntegrationTest.class)
public class EthDriverTestBlocks {
    private static final Logger LOGGER = LoggerFactory.getLogger(EthDriverTestBlocks.class);

    @Test
    public void testBlock() {

        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {
            Class.forName(driverClass);
            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery(
                    "select count(blocknumber) from block where blocknumber=1652339 or blocknumber=1652340 group by blocknumber");
            while (rs.next()) {
                LOGGER.info("count " + rs.getInt(1));
            }

            rs = stmt.executeQuery(
                    "select count(blocknumber), blocknumber from block where blocknumber=1652339 or blocknumber=1652340 or blocknumber=2120613 group by blocknumber ");
            while (rs.next()) {
                LOGGER.info("count : " + rs.getInt(1));
                LOGGER.info(" block number " + rs.getObject(2));

            }

            LOGGER.info("*****************SELECT * TEST***************");
            rs = stmt.executeQuery("select * from block where blocknumber=1652339 or blocknumber=1652340");
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");
            System.out.println();
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)

                    LOGGER.info(rs.getObject(i) + " | ");
            }

            LOGGER.info("*****************Test when given the wrong value***************");
            rs = stmt.executeQuery("select * from block where hash='wornghash'");
            assert(!rs.next());

            LOGGER.info("*****************Test when given the wrong value where range***************");
            rs = stmt.executeQuery("select * from block where blocknumber > 123 and blocknumber < 132 and hash='wornghash'");
            assert(!rs.next());

            conn.close();
            assert(true == conn.isClosed());
            assert(true == rs.isClosed());
            assert(true == stmt.isClosed());
            LOGGER.info("Connection should be closed "+conn.isClosed());
            LOGGER.info("ResultSet should be closed "+rs.isClosed());
            LOGGER.info("Statement should be closed "+stmt.isClosed());
        } catch (Exception e1) {
            e1.printStackTrace();
        }

    }
}