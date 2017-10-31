package com.impetus.eth.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.impetus.blkch.jdbc.BlkchnResultSetMetaData;

public class EthResultSetMetaData implements BlkchnResultSetMetaData {
	private String tableName;
	private HashMap<String, Integer> columnNamesMap = new HashMap<String, Integer>();
	
	private Map<Integer,String> indexToColumnMap=null;

	public EthResultSetMetaData(String tableName,
			HashMap<String, Integer> columnNamesMap) {
		super();
		this.tableName = tableName;
		this.columnNamesMap = columnNamesMap;
		indexToColumnMap = columnNamesMap.entrySet()
			       .stream()
			       .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
	}

	@Override
	public boolean isWrapperFor(Class<?> arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public <T> T unwrap(Class<T> arg0) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getCatalogName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnClassName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getColumnCount() throws SQLException {
	    return columnNamesMap.size();
	}

	@Override
	public int getColumnDisplaySize(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnLabel(int column) throws SQLException {
		return indexToColumnMap.get(column);
	}

	@Override
	public String getColumnName(int column) throws SQLException {
		return indexToColumnMap.get(column);
	}

	@Override
	public int getColumnType(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getColumnTypeName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getPrecision(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int getScale(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getSchemaName(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public String getTableName(int column) throws SQLException {
		return tableName;
	}

	@Override
	public boolean isAutoIncrement(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isCaseSensitive(int column) throws SQLException {
		return true;
	}

	@Override
	public boolean isCurrency(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isDefinitelyWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public int isNullable(int column) throws SQLException {
		return ResultSetMetaData.columnNullableUnknown;
	}

	@Override
	public boolean isReadOnly(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isSearchable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isSigned(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

	@Override
	public boolean isWritable(int column) throws SQLException {
		throw new SQLFeatureNotSupportedException();
	}

}
