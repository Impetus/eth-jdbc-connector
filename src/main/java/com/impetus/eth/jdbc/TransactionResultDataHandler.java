package com.impetus.eth.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.web3j.protocol.core.methods.response.Transaction;

public class TransactionResultDataHandler implements DataHandler {

	private static HashMap<String, Integer> columnNamesMap = new HashMap<String, Integer>();

	static {
		columnNamesMap.put("blockHash", 0);
		columnNamesMap.put("blockNumber", 1);
		columnNamesMap.put("creates", 2);
		columnNamesMap.put("from", 3);
		columnNamesMap.put("gas", 4);
		columnNamesMap.put("gasprice", 5);
		columnNamesMap.put("hash", 6);
		columnNamesMap.put("input", 7);
		columnNamesMap.put("nonce", 8);
		columnNamesMap.put("publicKey", 9);
		columnNamesMap.put("r", 10);
		columnNamesMap.put("raw", 11);
		columnNamesMap.put("s", 12);
		columnNamesMap.put("to", 13);
		columnNamesMap.put("tranactionIndex", 14);
		columnNamesMap.put("v", 15);
		columnNamesMap.put("value", 16);
	}
	
	public static HashMap<String, Integer> getColumnNamesMap() {
		return columnNamesMap;
	}

	@Override
	public ArrayList<Object[]> convertToObjArray(List<?> rows) {

		ArrayList<Object[]> result = new ArrayList<Object[]>();
		for (Object t : rows) {
			Object[] arr = new Object[columnNamesMap.size()];
			Transaction tr = (Transaction) t;
			
			arr[0]=tr.getBlockHash();
			arr[1]=tr.getBlockNumberRaw();
			arr[2]=tr.getCreates();
			arr[3]=tr.getFrom();
			arr[4]=tr.getGasRaw();
			arr[5]=tr.getGasPriceRaw();
			arr[6]=tr.getHash();
			arr[7]=tr.getInput();
			arr[8]=tr.getNonceRaw();
			arr[9]=tr.getPublicKey();
			arr[10]=tr.getR();
			arr[11]=tr.getRaw();
			arr[12]=tr.getS();
			arr[13]=tr.getTo();
			arr[14]=tr.getTransactionIndexRaw();
			arr[15]=tr.getV();
			arr[16]=tr.getValueRaw();
			result.add(arr);
		}
		return result;
	}

	@Override
	public String getTableName() {
		
		return "transactions";
	}
}
