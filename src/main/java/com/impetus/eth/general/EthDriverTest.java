package com.impetus.eth.general;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class EthDriverTest {
public static void main(String[] args) throws ClassNotFoundException {

	 String url="jdbc:blkchn:ethereum@172.25.41.52:8545";
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
			System.out.println(rs.getString("value"));
		}
		
	} catch (SQLException e1) {
		// TODO Auto-generated catch block
		e1.printStackTrace();
	}
	
	}
}
