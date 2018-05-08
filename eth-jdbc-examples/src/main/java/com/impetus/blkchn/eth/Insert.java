package com.impetus.blkchn.eth;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import org.web3j.crypto.CipherException;
import org.web3j.protocol.core.methods.response.TransactionReceipt;
import org.web3j.protocol.exceptions.TransactionTimeoutException;

import com.impetus.eth.jdbc.DriverConstants;
import com.impetus.eth.jdbc.EthStatement;

public class Insert {
    public static void main(String[] args) throws ClassNotFoundException, IOException, CipherException,
            InterruptedException, ExecutionException, TransactionTimeoutException {
        String url = "jdbc:blkchn:ethereum://172.25.41.52:8545";
        String driverClass = "com.impetus.eth.jdbc.EthDriver";

        String order;

        String query = "insert into transaction (toAddress, value, unit, async) values ('0xa76cd046cf6089fe2adcf1680fcede500e44bacd', 1.11, 'ether', true)";
        try {
            Class.forName(driverClass);
            Properties prop = new Properties();
            prop.put(DriverConstants.KEYSTORE_PATH,
                    "/home/ubuntu/rnd/ethereum/datadir/keystore/UTC--2017-09-11T04-53-29.614189140Z--8144c67b144a408abc989728e32965edf37adaa1");
            prop.put(DriverConstants.KEYSTORE_PASSWORD, "test123");
            Connection conn = DriverManager.getConnection(url, prop);
            Statement stmt = conn.createStatement();
            Object ret = ((EthStatement) stmt).executeAndReturn(query);
            System.out.println(((TransactionReceipt) ((CompletableFuture) ret).get()).getTransactionHash());

            // ResultSet rs = stmt.executeQuery(query);
            // while(rs.next()){
            // System.out.println(rs.getString("blocknumber"));
            // }
            // System.out.println("output status: " + ret);
        } catch (SQLException | ClassNotFoundException e1) {
            e1.printStackTrace();
        }

    }

}
