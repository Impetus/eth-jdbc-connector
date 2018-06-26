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
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;
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
public class EthDriverTestBlocks
{
    @Test
    public  void testBlock()
    {

        String url = ConnectionUtil.getEthUrl();
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try
        {
            Class.forName(driverClass);
            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select count(blocknumber) from block where blocknumber=1652339 or blocknumber=1652340 group by blocknumber");
            while (rs.next())
            {
                System.out.println("count "+rs.getInt(1));
            }
            
            
            rs = stmt.executeQuery("select count(blocknumber), blocknumber from block where blocknumber=1652339 or blocknumber=1652340 or blocknumber=2120613 group by blocknumber ");
            while (rs.next())
            {
               System.out.print("count : "+rs.getInt(1));
               System.out.println(" block number "+rs.getObject(2));
                
            }
            

            System.out.println("*****************SELECT * TEST***************");
            System.out.println();
             rs=stmt.executeQuery("select * from block where blocknumber=1652339 or blocknumber=1652340");
            for (int i=1;i<=rs.getMetaData().getColumnCount();i++)
                System.out.print(rs.getMetaData().getColumnLabel(i)+" | ");
            System.out.println();
            while (rs.next())
            {
              for(int i=1;i<=rs.getMetaData().getColumnCount();i++)
                 
                  System.out.print(rs.getObject(i)+" | ");
                  System.out.println();
            }
        }
        catch (Exception e1)
        {
            e1.printStackTrace();
        }

    }
}