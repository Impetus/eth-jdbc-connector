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
package com.impetus.eth.test.mock;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;

import junit.framework.TestCase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.impetus.eth.jdbc.EthConnection;
import com.impetus.eth.jdbc.EthStatement;

@RunWith(PowerMockRunner.class)
@PrepareForTest(DriverManager.class)
public class EthResultSetTest extends TestCase {
    String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
   
    @Override
    protected void setUp() throws Exception {
        Connection conn = Mockito.mock(EthConnection.class);
        Statement stmt = Mockito.mock(EthStatement.class);
        PowerMockito.mockStatic(DriverManager.class);
        Mockito.when(DriverManager.getConnection(url, null)).thenReturn(conn);

        Mockito.when(conn.createStatement()).thenReturn(stmt);
        Mockito.when(stmt.executeQuery("select * from transactions where blocknumber=1652339")).thenReturn(
                ResultSetMockData.returnEthResultSetAll());

        Mockito.when(
                stmt.executeQuery("select blocknumber, blockhash,to,value,gasprice from transactions where blocknumber=1652339 or "
                        + "blocknumber=1652340")).thenReturn(ResultSetMockData.returnEthResultSetMultipleBlocks());

        Mockito.when(
                stmt.executeQuery("select blocknumber, blockhash,to,value,gasprice"
                        + " from transactions where blocknumber=1652339 or blocknumber=1652340 order by gasprice desc"))
                .thenReturn(ResultSetMockData.returnEthResultSetOrderBy());
        Mockito.when(
                stmt.executeQuery("select count(to), to from transactions where blocknumber=1652339 or blocknumber=1652340 group by to"))
                .thenReturn(ResultSetMockData.returnEthResultSetGroupBy());

    }

    @Test
    public void testResultSet() {

          String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try {
            
            Class.forName(driverClass);

            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();

            System.out.println("*****************SELECT * FROM A BLOCK ***************");
            System.out.println();
           
            ResultSet rs = stmt
                    .executeQuery("select * from transactions where blocknumber=1652339");
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                System.out.print(rs.getMetaData().getColumnLabel(i) + " | ");
            System.out.println();
            while (rs.next()) {
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                    if (i == 15)
                        System.out.print(rs.getInt(i) + " | ");
                    else
                        System.out.print(rs.getObject(i) + " | ");
                System.out.println();
            }
            
            
            
            
            System.out.println();
            System.out.println("************SELECT MULTIPLE COLUMNS WITH MULTIPLE BLOCKS *************   ");
            System.out.println();
            rs = stmt
                    .executeQuery("select blocknumber, blockhash,to,value,gasprice from transactions where blocknumber=1652339 or blocknumber=1652340");
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                System.out.print(rs.getMetaData().getColumnLabel(i) + " | ");
            System.out.println();

            while (rs.next()) {
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                    if (i == 15)
                        System.out.print(rs.getInt(i) + " | ");
                    else
                        System.out.print(rs.getObject(i) + " | ");
                System.out.println();
            }
            
            
            

            System.out.println();
            System.out.println("************SELECT ORDER BY COLUMNS*************   ");
            System.out.println();
            rs = stmt
                    .executeQuery("select blocknumber, blockhash,to,value,gasprice from transactions where blocknumber=1652339 or blocknumber=1652340 order by gasprice desc");
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                System.out.print(rs.getMetaData().getColumnLabel(i) + " | ");
            System.out.println();

            while (rs.next()) {
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                    if (i == 15)
                        System.out.print(rs.getInt(i) + " | ");
                    else
                        System.out.print(rs.getObject(i) + " | ");
                System.out.println();
            }

            
            
            
            System.out.println();
            System.out.println("********************SELECT GROUP BY COLUMNS ************");
            System.out.println();
            rs = stmt
                    .executeQuery("select count(to), to from transactions where blocknumber=1652339 or blocknumber=1652340 group by to");
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0; i < rsMetaData.getColumnCount(); i++)
                System.out.print(rsMetaData.getColumnLabel(i) + " | ");
            System.out.println();
            while (rs.next()) {
                System.out.print(rs.getInt(0));
                System.out.print(" | " + rs.getString(1));
                System.out.println();
            }

           
        } catch (Exception e) {

            e.printStackTrace();
        }
    }
}