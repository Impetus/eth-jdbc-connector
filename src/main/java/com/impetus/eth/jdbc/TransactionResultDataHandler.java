package com.impetus.eth.jdbc;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.web3j.protocol.core.methods.response.EthBlock.TransactionObject;

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
	public ArrayList<ArrayList> convertToObjArray(List rows) {
		ArrayList<ArrayList> result = new ArrayList<ArrayList>();
		for (Object t : rows) {
			ArrayList arr = new ArrayList();
			TransactionObject tr = (TransactionObject) t;
			arr.add(tr.getBlockHash());
			arr.add(tr.getBlockNumberRaw());
			arr.add(tr.getCreates());
			arr.add(tr.getFrom());
			arr.add(tr.getGasRaw());
			arr.add(tr.getGasPriceRaw());
			arr.add(tr.getHash());
			arr.add(tr.getInput());
			arr.add(tr.getNonceRaw());
			arr.add(tr.getPublicKey());
			arr.add(tr.getR());
			arr.add(tr.getRaw());
			arr.add(tr.getS());
			arr.add(tr.getTo());
			arr.add(tr.getTransactionIndexRaw());
			arr.add(tr.getV());
			arr.add(tr.getValueRaw());
			result.add(arr);
		}
		return result;
	}

}
