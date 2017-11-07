/******************************************************************************* 
 * * Copyright 2017 Impetus Infotech.
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
package com.impetus.eth.general;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * The Class EthDriverTest.
 * 
 * @author ashishk.shukla
 * 
 */
public class EthDriverTest
{

    /**
     * The main method.
     *
     * @param args
     *            the arguments
     * @throws ClassNotFoundException
     *             the class not found exception
     */
    public static void main(String[] args) throws ClassNotFoundException
    {

        String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try
        {
            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("SAMPLE_QUERY");
            while (rs.next())
            {
                // For Transactions

                System.out.println("" + rs.getString(3));
                System.out.println("" + rs.getString("from"));
                System.out.println();
            }
            System.out.println("Result set MetaData");

            ResultSetMetaData rsMetaData = rs.getMetaData();
            System.out.println("Total Columns : " + rsMetaData.getColumnCount());
            System.out.println("column label name : " + rsMetaData.getColumnLabel(3));
            System.out.println("column Name : " + rsMetaData.getColumnName(3));
            System.out.println("tableName : " + rsMetaData.getTableName(3));
        }
        catch (SQLException e1)
        {
            e1.printStackTrace();
        }

    }
}
