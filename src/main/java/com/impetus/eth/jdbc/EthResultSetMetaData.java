/******************************************************************************* 
 * * Copyright 2017 Impetus Infotech.
 * *
 * * Licensed under the Apache License, Version 2.0 (the "License");
 * * you may not use this file except in compliance with the License.
 * * You may obtain a copy of the License at
 * *
 * * http://www.apache.org/licenses/LICENSE-2.0
 * *
 * * Unless required by applicable law or agreed to in writing, software
 * * distributed under the License is distributed on an "AS IS" BASIS,
 * * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * * See the License for the specific language governing permissions and
 * * limitations under the License.
 ******************************************************************************/
package com.impetus.eth.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

import com.impetus.blkch.jdbc.BlkchnResultSetMetaData;

/**
 * The Class EthResultSetMetaData.
 *
 * @author ashishk.shukla
 * 
 */
public class EthResultSetMetaData implements BlkchnResultSetMetaData
{

    /** The table name. */
    private String tableName;

    /** The column names map. */
    private HashMap<String, Integer> columnNamesMap = new HashMap<String, Integer>();

    /** The index to column map. */
    private Map<Integer, String> indexToColumnMap = null;

    /**
     * Instantiates a new eth result set meta data.
     *
     * @param tableName
     *            the table name
     * @param columnNamesMap
     *            the column names map
     */
    public EthResultSetMetaData(String tableName, HashMap<String, Integer> columnNamesMap)
    {
        super();
        this.tableName = tableName;
        this.columnNamesMap = columnNamesMap;
        indexToColumnMap = columnNamesMap.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#isWrapperFor(java.lang.Class)
     */
    @Override
    public boolean isWrapperFor(Class<?> arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.Wrapper#unwrap(java.lang.Class)
     */
    @Override
    public <T> T unwrap(Class<T> arg0) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getCatalogName(int)
     */
    @Override
    public String getCatalogName(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnClassName(int)
     */
    @Override
    public String getColumnClassName(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnCount()
     */
    @Override
    public int getColumnCount() throws SQLException
    {
        return columnNamesMap.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnDisplaySize(int)
     */
    @Override
    public int getColumnDisplaySize(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnLabel(int)
     */
    @Override
    public String getColumnLabel(int column) throws SQLException
    {
        return indexToColumnMap.get(column);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnName(int)
     */
    @Override
    public String getColumnName(int column) throws SQLException
    {
        return indexToColumnMap.get(column);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnType(int)
     */
    @Override
    public int getColumnType(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getColumnTypeName(int)
     */
    @Override
    public String getColumnTypeName(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getPrecision(int)
     */
    @Override
    public int getPrecision(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getScale(int)
     */
    @Override
    public int getScale(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getSchemaName(int)
     */
    @Override
    public String getSchemaName(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#getTableName(int)
     */
    @Override
    public String getTableName(int column) throws SQLException
    {
        return tableName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isAutoIncrement(int)
     */
    @Override
    public boolean isAutoIncrement(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isCaseSensitive(int)
     */
    @Override
    public boolean isCaseSensitive(int column) throws SQLException
    {
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isCurrency(int)
     */
    @Override
    public boolean isCurrency(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isDefinitelyWritable(int)
     */
    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isNullable(int)
     */
    @Override
    public int isNullable(int column) throws SQLException
    {
        return ResultSetMetaData.columnNullableUnknown;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isReadOnly(int)
     */
    @Override
    public boolean isReadOnly(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isSearchable(int)
     */
    @Override
    public boolean isSearchable(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isSigned(int)
     */
    @Override
    public boolean isSigned(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.sql.ResultSetMetaData#isWritable(int)
     */
    @Override
    public boolean isWritable(int column) throws SQLException
    {
        throw new SQLFeatureNotSupportedException();
    }

}
