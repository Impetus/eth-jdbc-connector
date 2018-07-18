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
package com.impetus.blkchn.eth;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryPreparedStatement {
    private static final Logger LOGGER = LoggerFactory.getLogger(QueryPreparedStatement.class);

    public static void main(String[] args) throws ClassNotFoundException {

        String url = "jdbc:blkchn:ethereum://ropsten.infura.io/1234";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {
            Class.forName(driverClass);
            Connection conn = DriverManager.getConnection(url, null);
            PreparedStatement stmt = conn.prepareStatement(
                    "select count(*) as cnt, blocknumber from transaction where blocknumber = 1234 or blocknumber = ? or blocknumber =12323 or blocknumber = ? group by blocknumber");
            stmt.setObject(2, 2222486);
            stmt.setObject(1, 2222487);

            ResultSet rs = stmt.executeQuery();

            while (rs.next()) {
                LOGGER.info("" + rs.getInt(1));
                LOGGER.info("" + rs.getString("blocknumber"));
            }
            LOGGER.info("Result set MetaData");

            ResultSetMetaData rsMetaData = rs.getMetaData();
            LOGGER.info("Total Columns : " + rsMetaData.getColumnCount());
            LOGGER.info("column label name : " + rsMetaData.getColumnLabel(1));
            LOGGER.info("column Name : " + rsMetaData.getColumnName(1));
            LOGGER.info("tableName : " + rsMetaData.getTableName(1));

            stmt.setObject(2, 2222488);
            stmt.setObject(1, 2222489);

            rs = stmt.executeQuery();

            while (rs.next()) {
                LOGGER.info("" + rs.getInt(1));
                LOGGER.info("" + rs.getString("blocknumber"));
            }
            LOGGER.info("Result set MetaData");

            rsMetaData = rs.getMetaData();
            LOGGER.info("Total Columns : " + rsMetaData.getColumnCount());
            LOGGER.info("column label name : " + rsMetaData.getColumnLabel(1));
            LOGGER.info("column Name : " + rsMetaData.getColumnName(1));
            LOGGER.info("tableName : " + rsMetaData.getTableName(1));

            conn.close();
            rs.close();
            stmt.close();
            LOGGER.info("Connection closed " + conn.isClosed());
            LOGGER.info("ResultSet closed " + rs.isClosed());
            LOGGER.info("Statement closed "+stmt.isClosed());

        } catch (SQLException e1) {
            e1.printStackTrace();
        }

    }
}
