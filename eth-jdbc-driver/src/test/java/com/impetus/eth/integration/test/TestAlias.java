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

import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;

@Category(IntegrationTest.class)
public class TestAlias {
    private static final Logger LOGGER = LoggerFactory.getLogger(TestAlias.class);

    @Test
    public void testAlias() {

        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {
            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();

            LOGGER.info("*****************SELECT * TEST***************");

            ResultSet rs = stmt
                    .executeQuery("select gas as gp from transaction where blocknumber=1652339 or blocknumber=1652340");
            ResultSetMetaData rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                LOGGER.info(rsmd.getColumnName(i) + " | ");
            }

            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    LOGGER.info(rs.getObject(i) + " | ");
            }

            LOGGER.info("*****************SELECT group By Order By Alias***************");
            rs = stmt.executeQuery(
                    "select gas as gp,count(gas) as count from transaction where blocknumber=1652339 or blocknumber=1652340 group by gas order by count desc limit 4");
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");
            }
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    LOGGER.info(rs.getObject(i) + " | ");
                }
            }

            LOGGER.info("*****************SELECT group By Having Alias***************");
            rs = stmt.executeQuery(
                    "select gas as gp,count(gas) as count from transaction where blocknumber=1652339 or blocknumber=1652340 group by gas order by count desc");
            rsmd = rs.getMetaData();
            for (int i = 1; i <= rsmd.getColumnCount(); i++)
                LOGGER.info(rsmd.getColumnLabel(i) + " | ");
            while (rs.next()) {
                for (int i = 1; i <= rsmd.getColumnCount(); i++)
                    if (i == 2)
                        LOGGER.info(rs.getInt("count") + " | ");
                    else
                        LOGGER.info(rs.getString("gas") + " | ");
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}