package com.impetus.eth.general;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;
import org.web3j.utils.Numeric;

public class EthDriverTest {
public static void main(String[] args) throws ClassNotFoundException {

	 String url="jdbc:blkchn:ethereum://172.25.41.52:8545";
	 String driverClass="com.impetus.eth.jdbc.EthDriver";
	 try {
		Class.forName(driverClass);
		
		System.out.println(DriverManager.getDriver(url).getMajorVersion());
		
		Connection conn= DriverManager.getConnection(url, null);
		System.out.println("Connected to Ethereum\n*********\n");
		
		Statement stmt=conn.createStatement();
		System.out.println("Statement Created\n*********\n");
		
		ResultSet rs=stmt.executeQuery("SAMPLE_QUERY");
		while(rs.next()){
			//For Blocks
			/*
			System.out.println("\n************");
			 System.out.println("2nd column value : "+rs.getString(1));
			 System.out.println("block number in hex : "+rs.getString("number"));	
			 System.out.println("block number decoded : "+Numeric.decodeQuantity(rs.getString("number")));
			 
			 List<TransactionObject> lt= (List<TransactionObject>) rs.getObject("transactions");
			 System.out.println("trans index : "+lt.get(0).getTransactionIndex());
			*/
			
			//For Transactions
			  System.out.println(""+rs.getString(0));
			  System.out.println(""+rs.getString("blockHash"));
			  System.out.println();
	      		}
		
		
	//	System.out.println(Numeric.decodeQuantity(rs.getString("transactions")));
	
	} catch (SQLException e1) {
		e1.printStackTrace();
	}
	
	}
}
