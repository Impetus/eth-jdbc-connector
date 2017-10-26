package com.impetus.eth.jdbc;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

public class EthResultSet extends AbstractResultSet {
	
	protected static final int BEFORE_FIRST_ROW=0;
	
	protected static final int AFTER_LAST_ROW=-1;
	
	protected int currentRowCursor;
	
	protected int totalRowCount;
	
	protected ArrayList<Object[]> rowData;

	protected Object[] currentRow;	
	
	protected HashMap<String, Integer> columnNamesMap;
	
	protected int resultSetType;
	
	protected int rSetConcurrency;
	
	protected String tableName;
	
	
	public EthResultSet(ArrayList<Object[]> rowData, HashMap<String, Integer> columnNamesMap, int resultSetType, int rSetConcurrency,String tableName){
		this.rowData = rowData;
		this.columnNamesMap = columnNamesMap;
		this.resultSetType=resultSetType;
		this.rSetConcurrency=rSetConcurrency;
		this.tableName=tableName;
		currentRowCursor = BEFORE_FIRST_ROW;
		totalRowCount = rowData.size();
	}
	
	@Override
	public boolean first() throws SQLException {
		checkRSForward();
		if(totalRowCount>0){
			currentRowCursor = 1;
			currentRow = rowData.get(currentRowCursor-1);
			return true;
		}
		return false;
	}
	
	@Override
	public boolean last() throws SQLException {
		checkRSForward();
		if(totalRowCount>0){
			currentRowCursor = totalRowCount;
			currentRow = rowData.get(currentRowCursor-1);
			return true;
		}
		return false;
	}
	@Override
	public boolean next() throws SQLException {
		
		if(currentRowCursor!=AFTER_LAST_ROW && currentRowCursor < totalRowCount){
			currentRowCursor++;
			currentRow = rowData.get(currentRowCursor-1);
			return true;
		}else{
		currentRowCursor=AFTER_LAST_ROW;
		return false;
		}
	}
	
	@Override
	public boolean previous() throws SQLException {
		checkRSForward();
		if(currentRowCursor > 1){
			currentRowCursor--;
			currentRow = rowData.get(currentRowCursor-1);
			return true;
		}else{
		currentRowCursor=BEFORE_FIRST_ROW;
		return false;
		}
	}
	@Override
	public void afterLast() throws SQLException {
		checkRSForward();
		currentRowCursor=AFTER_LAST_ROW;
		}

	@Override
	public void beforeFirst() throws SQLException {
		checkRSForward();
		currentRowCursor=BEFORE_FIRST_ROW;
	}
    
	@Override
	public boolean isFirst() throws SQLException {
		if(currentRowCursor==1)
			return true;
		else
		   return false;
	}

	@Override
	public boolean isLast() throws SQLException {
		if(currentRowCursor==totalRowCount)
			return true;
		else
		   return false;
	}
	
	@Override
	public boolean relative(int rows) throws SQLException {
		checkRSForward();
		
		if(rows==0)
		return true;
		
		if(rows==-1)
			return previous();
		
		if(rows==1)
			return next();
		
		if((rows<0 &&(currentRowCursor+rows)>0 )||
				(rows>0 &&(currentRowCursor+rows)<=totalRowCount))
		{
			currentRowCursor=currentRowCursor+rows;
			currentRow= rowData.get(currentRowCursor-1);
			return true;
	    }else 
			return false;
			
	}
	
	@Override
	public int getFetchSize() throws SQLException {
		return totalRowCount;
	}
	
	
	@Override
	public String getString(String columnLabel) throws SQLException {
		return (String) currentRow[columnNamesMap.get(columnLabel)];
	}

	@Override
	public String getString(int columnIndex) throws SQLException {
		return (String) currentRow[columnIndex];
	}
   
	@Override
	public Object getObject(String columnLabel) throws SQLException {
		return currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public Object getObject(int columnIndex) throws SQLException {
		return currentRow[columnIndex];
	}
	
	@Override
	public int getInt(int columnIndex) throws SQLException {
		return (int) currentRow[columnIndex];
	}

	@Override
	public int getInt(String columnLabel) throws SQLException {
		return  (int) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public long getLong(int columnIndex) throws SQLException {
		return (long) currentRow[columnIndex];
	}

	@Override
	public long getLong(String columnLabel) throws SQLException {
		return  (long) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
		return (BigDecimal) currentRow[columnIndex];
	}

	@Override
	public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
		return  (BigDecimal) currentRow[columnNamesMap.get(columnLabel)];
	}
	@Override
	public boolean getBoolean(int columnIndex) throws SQLException {
		return (boolean) currentRow[columnIndex];
	}

	@Override
	public boolean getBoolean(String columnLabel) throws SQLException {
		return  (boolean) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public byte getByte(int columnIndex) throws SQLException {
		return (byte) currentRow[columnIndex];
	}

	@Override
	public byte getByte(String columnLabel) throws SQLException {
		return  (byte) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public byte[] getBytes(int columnIndex) throws SQLException {
		return (byte[]) currentRow[columnIndex];
	}

	@Override
	public byte[] getBytes(String columnLabel) throws SQLException {
		return (byte[]) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public double getDouble(int columnIndex) throws SQLException {
		return (double) currentRow[columnIndex];
	}

	@Override
	public double getDouble(String columnLabel) throws SQLException {
		return (double) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public short getShort(int columnIndex) throws SQLException {
		return (short) currentRow[columnIndex];
	}

	@Override
	public short getShort(String columnLabel) throws SQLException {
		return (short) currentRow[columnNamesMap.get(columnLabel)];
	}
	
	@Override
	public int getRow() throws SQLException {
		return currentRowCursor;
	}
	
	@Override
	 public int getType() throws SQLException {
	           return resultSetType;
	    }
	
	@Override
	public ResultSetMetaData getMetaData() throws SQLException {
		return new EthResultSetMetaData(tableName, columnNamesMap);
	}
	
	@Override
	public int findColumn(String columnLabel) throws SQLException {
		return columnNamesMap.get(columnLabel);
	}
    
	
	 protected void checkRSForward() throws SQLException {
	        if (resultSetType == ResultSet.TYPE_FORWARD_ONLY) {
	            throw new SQLException("Result Set is Type Forward only", "1000");
	        }
	    }
	 
}
