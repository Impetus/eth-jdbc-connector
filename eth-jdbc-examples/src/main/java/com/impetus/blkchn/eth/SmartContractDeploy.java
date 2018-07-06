package com.impetus.blkchn.eth;

import com.impetus.eth.jdbc.DriverConstants;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class SmartContractDeploy {



    public static void main(String[] args) throws
            InterruptedException,ExecutionException {
        String url = "jdbc:blkchn:ethereum://127.0.0.1:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";
        String query = "DEPLOY smartcontract 'com.impetus.blkchn.eth.FirstSmartContract'()";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,
                    "/home/<path>");
            prop.put(DriverConstants.KEYSTORE_PASSWORD, "<password>");
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            boolean retBool = stmt.execute(query);
            if(retBool) {
                ResultSet ret = stmt.getResultSet();
                ret.next();
                String return_value = (String) ret.getObject(1);
                System.out.println("Smart contract has deployed at address :: "+return_value);
            }
            System.out.println("done");
        }catch (Exception e){
                System.out.println(e);
        }
    }
}
