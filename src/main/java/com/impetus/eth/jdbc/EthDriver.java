package com.impetus.eth.jdbc;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;



public class EthDriver implements Driver {
 
 static {
     try {
         DriverManager.registerDriver(new EthDriver());
     } catch (SQLException e) {
    	 
     }
 }
 
 
	@Override
	public boolean acceptsURL(String url) throws SQLException {
		  if (url == null) {
	            return false;
	        }
	        return url.toLowerCase().startsWith(DriverConstants.DRIVERPREFIX);
	}

	
	
	@Override
	public Connection connect(String url, Properties info) throws SQLException {
		
		System.out.println("In Driver Connect Block ");
		  if (url == null || !url.toLowerCase().startsWith(DriverConstants.DRIVERPREFIX)) 
		  {
	            return null;
	        }
		Properties props= getPropMap(url);    
		return new EthConnection(url,props);
	}

	@Override
	public int getMajorVersion() {
		
		return DriverConstants.MAJOR_VERSION;
	}

	@Override
	public int getMinorVersion() {
		
		return DriverConstants.MINOR_VERSION;
	}

	@Override
	public Logger getParentLogger() throws SQLFeatureNotSupportedException {
		throw new UnsupportedOperationException();
	}

	@Override
	public DriverPropertyInfo[] getPropertyInfo(String url, Properties info){
	throw new UnsupportedOperationException();
	}

	@Override
	public boolean jdbcCompliant() {
		return false;
	}

	
	public static Properties getPropMap(String url){

        Properties props = new Properties();

        StringBuilder token = new StringBuilder();
        int index = 0;

      
        index= nextColonIndex(url, index, token);
        if (!"jdbc".equalsIgnoreCase(token.toString())) {
            return null; 
        }

      
        index= nextColonIndex(url, index, token);
        if (!"blkchn".equalsIgnoreCase(token.toString())) {
            return null;
        }
       
       
        index=nextColonIndex(url, index, token);
        if (!"ethereum".equalsIgnoreCase(token.toString())) {
            return null;
        }
    
        index=nextColonIndex(url, index, token);
        index=nextColonIndex(url, index, token);
        String hostName=token.toString(); 
        props.setProperty(DriverConstants.HOSTNAME, hostName);

        index=nextColonIndex(url, index, token);

        try {
            int port = Integer.parseInt(token.toString());
            props.setProperty(DriverConstants.PORTNUMBER, Integer.toString(port));
           } catch(NumberFormatException e) {
            return null;
          }
             
          return props;
    
	}

	 private static int nextColonIndex(String url, int position, StringBuilder token) {
	        token.setLength(0);
	        while (position < url.length()) {
	            char chAtPos = url.charAt(position++);
            if (chAtPos == ':'||chAtPos=='@') {
	                    break;
	                }
        
            if (chAtPos == '/') {
                if (position < url.length() && url.charAt(position) == '/') {
                	position++;
                }

                break;
            }
            
	             token.append(chAtPos);
	        }

	        return position;
	    }
	 
	}
