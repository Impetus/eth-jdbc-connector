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

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.eth.jdbc.EthConnection;
import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;

/**
 * The Class EthDriverTest.
 * 
 * @author ashishk.shukla
 * 
 */
@Category(IntegrationTest.class)
public class EthDriverTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(EthDriverTest.class);

    @Test
    public void testTransactions() {

        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {

            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();
            LOGGER.info("*****************SELECT * FROM A Transaction ***************");

            ResultSet rs = stmt.executeQuery("select * from transaction where blocknumber=1652339");
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    LOGGER.info(rs.getObject(i) + " | ");

            }

            LOGGER.info("************SELECT MULTIPLE COLUMNS WITH MULTIPLE BLOCKS *************   ");

            rs = stmt.executeQuery(
                    "select blocknumber, blockhash,to,value,gasprice from transaction where blocknumber=1652339 or blocknumber=1652340");
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    LOGGER.info(rs.getObject(i) + " | ");
            }

            LOGGER.info("************SELECT ORDER BY COLUMNS*************   ");
            rs = stmt.executeQuery(
                    "select blocknumber, blockhash,to,value,gasprice from transaction where blocknumber=1652339 or blocknumber=1652340 order by gasprice desc");
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    LOGGER.info(rs.getObject(i) + " | ");
            }

            LOGGER.info("********************SELECT GROUP BY COLUMNS ************");
            rs = stmt.executeQuery(
                    "select count(to) as count, to from transaction where blocknumber=1652339 or blocknumber=1652340 group by to");
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");
            while (rs.next()) {
                LOGGER.info(rs.getInt(1) + "");
                LOGGER.info(" | " + rs.getString(2));
                System.out.println();
            }

        } catch (Exception e1) {
            e1.printStackTrace();
        }

    }
}