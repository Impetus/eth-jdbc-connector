/******************************************************************************* 
* * Copyright 2018 Impetus Infotech.
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
import java.sql.Types;

import com.impetus.blkch.BlkchnException;
import com.impetus.eth.query.EthColumns;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.impetus.blkch.jdbc.BlkchnResultSetMetaData;

/**
 * The Class EthResultSetMetaData.
 * 
 * @author ashishk.shukla
 * 
 */
public class EthResultSetMetaData implements BlkchnResultSetMetaData {

    private static final Logger LOGGER = LoggerFactory.getLogger(EthResultSetMetaData.class);

    private String tableName;

    private Map<String, Integer> columnNamesMap = new HashMap<String, Integer>();

    private Map<Integer, String> indexToColumnMap = null;

    private Map<Integer, String> indexToAliasMap = new HashMap<Integer, String>();

    private Map<String, String> aliasMapping;

    protected Map<String, Integer> colTypeMap;

    public EthResultSetMetaData(String tableName, Map<String, Integer> columnNamesMap, Map<String, String> aliasMapping,
        Map<String, Integer> colTypeMap) {
        super();
        LOGGER.info("Instantiating new EthResultSetMetaData Object ");
        this.tableName = tableName;
        this.columnNamesMap = columnNamesMap;
        indexToColumnMap =
            columnNamesMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getValue, Map.Entry::getKey));
        this.aliasMapping = aliasMapping;
        this.colTypeMap = colTypeMap;
        if (!aliasMapping.isEmpty())
            setIndexToAlias();
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
        if (!aliasMapping.isEmpty()) {
            return indexToAliasMap.get(column - 1);
        } else
            return getColumnName(column);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return indexToColumnMap.get(column - 1);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        String colName = indexToColumnMap.get(column - 1);
        if (!colTypeMap.isEmpty()) {
            if (colTypeMap.containsKey(colName))
                return (int) colTypeMap.get(colName);
            else
                throw new BlkchnException("Column is not present in result set");
        }
        throw new BlkchnException("Column is not present in result set");
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int sqlType = getColumnType(column);
        switch (sqlType) {
            case Types.INTEGER:
                return EthColumns.INTEGERTYPE;
            case Types.BIGINT:
                return EthColumns.BIGINTEGERTYPE;
            case Types.DOUBLE:
                return EthColumns.DOUBLETYPE;
            case Types.FLOAT:
                return EthColumns.FLOATTYPE;
            case Types.VARCHAR:
                return EthColumns.STRINGTYPE;
            case Types.JAVA_OBJECT:
                return EthColumns.OBJECTTYPE;
            default:
                return EthColumns.OBJECTTYPE;
        }
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

    private void setIndexToAlias() {
        indexToAliasMap.putAll(indexToColumnMap);
        for (Map.Entry<String, String> aliasMapEntrySet : aliasMapping.entrySet()) {
            if (indexToAliasMap.containsValue((aliasMapEntrySet.getValue()))) {
                indexToAliasMap.put(columnNamesMap.get(aliasMapEntrySet.getValue()), aliasMapEntrySet.getKey());
            }
        }
    }

}
