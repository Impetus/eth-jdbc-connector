package com.impetus.eth.integration.test;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;

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

        String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        try
        {
            Class.forName(driverClass);
            Connection conn = DriverManager.getConnection(url, null);
            Statement stmt = conn.createStatement();
            ResultSet rs = stmt.executeQuery("select transactions as ts, count(transactions) from blocks where blocknumber=1652339 or blocknumber=1652340 ");
            while (rs.next())
            {
               
                List<TransactionObject> lt = (List<TransactionObject>) rs.getObject("transactions");
                System.out.println("transation "+lt);
                System.out.print("blockNumber : " + lt.get(0).getBlockNumber().longValueExact());
                System.out.print("blockNumber : " + lt.get(0).getValue().toString());
                System.out.println("count "+rs.getInt(1));
            }
            
            
            rs = stmt.executeQuery("select count(blocknumber), blocknumber from blocks where blocknumber=1652339 or blocknumber=1652340 or blocknumber=2120613 group by blocknumber ");
            while (rs.next())
            {
               System.out.print("count : "+rs.getInt(0));
               System.out.println(" block number "+rs.getLong(1));
                
            }
            

            System.out.println("*****************SELECT * TEST***************");
            System.out.println();
             rs=stmt.executeQuery("select * from blocks where blocknumber=1652339 or blocknumber=1652340");
            for (int i=0;i<rs.getMetaData().getColumnCount();i++)
                System.out.print(rs.getMetaData().getColumnLabel(i)+" | ");
            System.out.println();
            while (rs.next())
            {
              for(int i=0;i<rs.getMetaData().getColumnCount();i++)
                 
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