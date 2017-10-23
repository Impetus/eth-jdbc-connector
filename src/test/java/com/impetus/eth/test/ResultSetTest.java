package com.impetus.eth.test;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.web3j.protocol.Web3j;
import org.web3j.protocol.core.DefaultBlockParameter;
import org.web3j.protocol.core.methods.response.EthBlock;
import org.web3j.protocol.core.methods.response.EthBlock.TransactionResult;
import org.web3j.protocol.http.HttpService;

import com.impetus.eth.jdbc.EthResultSet;
import com.impetus.eth.jdbc.TransactionResultDataHandler;

public class ResultSetTest {
	
	private static Web3j web3j = null;
	
	@Before
	public void setUp(){
		web3j = Web3j.build(new HttpService("http://172.25.41.52:8545"));
		web3j.web3ClientVersion().observable().subscribe(x -> {
			String clientVersion = x.getWeb3ClientVersion();
			System.out.println("Client Version: " + clientVersion);
		});
	}

	@Test
	public void testTransactionResult() throws IOException {
		//GET ALL TRANSACTIONS FROM BLOCK TABLE WITH BLOCK_NUMBER = <NUMBER>
		List<TransactionResult> trans = getTransactions("1876545");
		
		TransactionResultDataHandler dataHandler = new TransactionResultDataHandler();
		
		ResultSet r = new EthResultSet(dataHandler.convertToObjArray(trans), dataHandler.getColumnNamesMap());
		
		try {
			while(r.next()){
				System.out.println(r.getString("hash"));
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private List<TransactionResult> getTransactions(String blockNumber) throws IOException {
		EthBlock block = web3j.ethGetBlockByNumber(
				DefaultBlockParameter.valueOf(new BigInteger(blockNumber)),
				true).send();
		
		return block.getBlock().getTransactions();
	}
		

}
