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
import java.sql.Statement;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import com.impetus.eth.test.util.ConnectionUtil;
import com.impetus.test.catagory.IntegrationTest;

@Category(IntegrationTest.class)
public class TestAlias
{
    @Test
    public void testAlias()
    {
     
        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try
        {
            Class.forName(driverClass);
           
            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();

            System.out.println("*****************SELECT * TEST***************");
            System.out.println();
           
            ResultSet rs = stmt
                    .executeQuery("select gas as gp from transactions where blocknumber=1652339 or blocknumber=1652340");
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
            {
                System.out.print(rs.getMetaData().getColumnName(i) + " | ");
            }
            System.out.println();
            while (rs.next())
            {
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                 System.out.print(rs.getObject(i) + " | ");
               
                System.out.println();
            }
            System.out.println();
            System.out.println("*****************SELECT group By Order By Alias***************");
            System.out.println();
            
            Object[][] data= new Object[7][2];
            String[] columns= new String[2];
            rs = stmt
                    .executeQuery("select gas as gp,count(gas) as count from transactions where blocknumber=1652339 or blocknumber=1652340 group by gas order by count desc limit 4");
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++){
                columns[i]=rs.getMetaData().getColumnLabel(i);
                System.out.print(rs.getMetaData().getColumnLabel(i) + " | ");
            }
            System.out.println();
            int j=0;
            while (rs.next())
            {
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                {
                    data[j][i]=rs.getObject(i);
                    if (i == 1)
                        System.out.print(rs.getInt("count") + " | ");
                    else
                        System.out.print(rs.getLong("gas") + " | ");
                }
                j++;
                System.out.println();
            }
         
            System.out.println();
            System.out.println("*****************SELECT group By Having Alias***************");
            System.out.println();
            rs = stmt
                    .executeQuery("select gas as gp,count(gas) as count from transactions where blocknumber=1652339 or blocknumber=1652340 group by gas having count >1 order by count desc");
            for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                System.out.print(rs.getMetaData().getColumnLabel(i) + " | ");
            System.out.println();
            while (rs.next())
            {
                for (int i = 0; i < rs.getMetaData().getColumnCount(); i++)
                    if (i == 1)
                        System.out.print(rs.getInt("count") + " | ");
                    else
                        System.out.print(rs.getLong("gas") + " | ");
                System.out.println();
            }

        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
    }
}