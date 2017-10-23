package com.impetus.eth.jdbc;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class EthResultSet extends AbstractResultSet {
	
	protected int currentRowIndex;
	
	protected int totalRowCount;
	
	protected ArrayList<ArrayList> rowData;
	
	protected ArrayList currentRow;
	
	protected HashMap<String, Integer> columnNamesMap;
	
	public EthResultSet(ArrayList<ArrayList> rowData, HashMap<String, Integer> columnNamesMap){
		this.rowData = rowData;
		this.columnNamesMap = columnNamesMap;
		currentRowIndex = 0;
		totalRowCount = rowData.size();
		currentRow = rowData.get(currentRowIndex);
	}
	
	@Override
	public boolean next() throws SQLException {
		if(currentRowIndex < totalRowCount){
			currentRow = rowData.get(currentRowIndex);
			currentRowIndex++;
			return true;
		}
		return false;
	}
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		return (String) currentRow.get(columnNamesMap.get(columnLabel));
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return (String) currentRow.get(columnIndex);
	}

}
